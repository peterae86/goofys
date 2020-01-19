package internal

import (
	"errors"
	"fmt"
	"github.com/jacobsa/fuse"
	"io"
	"sync"
)

type RandomWriteFileHandle struct {
	inode *Inode
	cloud StorageBackend
	key   string

	mpuName   *string
	dirty     bool
	writeInit sync.Once
	mpuWG     sync.WaitGroup

	mu         sync.Mutex
	mpuId      *MultipartBlobCommitInput
	lastPartId uint32

	buf *LocalFileBuf

	lastWriteError error

	existingReadahead int
	seqReadAmount     uint64
	numOOORead        uint64 // number of out of order read
	// User space PID. All threads created by a process will have the same TGID,
	// but different PIDs[1].
	// This value can be nil if we fail to get TGID from PID[2].
	// [1] : https://godoc.org/github.com/shirou/gopsutil/process#Process.Tgid
	// [2] : https://github.com/shirou/gopsutil#process-class
	Tgid *int32
}

func (fh *RandomWriteFileHandle) GetInode() *Inode {
	return fh.inode
}

func (fh *RandomWriteFileHandle) GetTgid() *int32 {
	return fh.Tgid
}

func (fh *RandomWriteFileHandle) initWrite() {
	fh.writeInit.Do(func() {
		fh.mpuWG.Add(1)
		go fh.initMPU()
	})
}

func (fh *RandomWriteFileHandle) initMPU() {
	defer func() {
		fh.mpuWG.Done()
	}()

	fs := fh.inode.fs
	fh.mpuName = &fh.key

	resp, err := fh.cloud.MultipartBlobBegin(&MultipartBlobBeginInput{
		Key:         *fh.mpuName,
		ContentType: fs.flags.GetMimeType(*fh.mpuName),
	})

	fh.mu.Lock()
	defer fh.mu.Unlock()

	if err != nil {
		fh.lastWriteError = mapAwsError(err)
	} else {
		fh.mpuId = resp
	}

	return
}

func (fh *RandomWriteFileHandle) mpuPartNoSpawn(part int) (err error) {
	fs := fh.inode.fs

	fs.replicators.Take(1, true)
	defer fs.replicators.Return(1)

	if part < 0 || part >= 10000 {
		return errors.New(fmt.Sprintf("invalid part number: %v", part))
	}

	reader, err := fh.buf.getPartReader(part)
	if err != nil {
		return err
	}
	var mpu = MultipartBlobAddInput{
		Commit:     fh.mpuId,
		PartNumber: uint32(part + 1),
		Body:       reader,
		//Size:       uint64(buf.Len()),
		//Last:       last,
		//Offset:     uint64(total - int64(buf.Len())),
	}
	fh.mpuWG.Add(1)
	defer fh.mpuWG.Done()
	_, err = fh.cloud.MultipartBlobAdd(&mpu)
	return
}

func (fh *RandomWriteFileHandle) mpuPart(part int) error {
	// maybe wait for CreateMultipartUpload
	if fh.mpuId == nil {
		fh.mpuWG.Wait()
		// initMPU might have errored
		if fh.mpuId == nil {
			return nil
		}
	}

	err := fh.mpuPartNoSpawn(part)
	if err != nil {
		if fh.lastWriteError == nil {
			fh.lastWriteError = err
		}
	}
	return err
}

func (fh *RandomWriteFileHandle) waitForCreateMPU() (err error) {
	if fh.mpuId == nil {
		fh.mu.Unlock()
		fh.initWrite()
		fh.mpuWG.Wait() // wait for initMPU
		fh.mu.Lock()

		if fh.lastWriteError != nil {
			return fh.lastWriteError
		}
	}

	return
}

func (fh *RandomWriteFileHandle) partSize() uint64 {
	var size uint64

	if fh.lastPartId < 1000 {
		size = 5 * 1024 * 1024
	} else if fh.lastPartId < 2000 {
		size = 25 * 1024 * 1024
	} else {
		size = 125 * 1024 * 1024
	}

	maxPartSize := fh.cloud.Capabilities().MaxMultipartSize
	if maxPartSize != 0 {
		size = MinUInt64(maxPartSize, size)
	}
	return size
}

func (fh *RandomWriteFileHandle) WriteFile(offset int64, data []byte) (err error) {
	fh.inode.logFuse("WriteFileMultipart", offset, len(data))

	fh.mu.Lock()
	defer fh.mu.Unlock()

	err = fh.waitForCreateMPU()
	if err != nil {
		return
	}

	if fh.lastWriteError != nil {
		return fh.lastWriteError
	}

	_, err = fh.buf.Write(int(offset), data, func(part int) {
		err = fh.mpuPart(part)
		if err != nil {
			fh.inode.logFuse("err upload part:%v err:%v", part, err)
		}
	})
	return
}

func (fh *RandomWriteFileHandle) ReadFile(offset int64, buf []byte) (bytesRead int, err error) {
	fh.inode.logFuse("ReadFile", offset, len(buf))
	defer func() {
		fh.inode.logFuse("< ReadFile", bytesRead, err)

		if err != nil {
			if err == io.EOF {
				err = nil
			}
		}
	}()

	fh.mu.Lock()
	defer fh.mu.Unlock()

	nwant := len(buf)
	var nread int

	for bytesRead < nwant && err == nil {
		nread, err = fh.readFile(offset+int64(bytesRead), buf[bytesRead:])
		if nread > 0 {
			bytesRead += nread
		}
		if err != nil {
			return
		}
		fh.inode.logFuse(fmt.Sprintf("< readFile bytesRead:%v nwant:%v", bytesRead, nwant))
	}

	return
}

func (fh *RandomWriteFileHandle) readFile(offset int64, buf []byte) (bytesRead int, err error) {
	defer func() {
		fh.inode.logFuse("< readFile", bytesRead, err)
	}()

	if uint64(offset) >= fh.inode.Attributes.Size {
		// nothing to read
		if fh.inode.Invalid {
			err = fuse.ENOENT
		} else if fh.inode.KnownSize == nil {
			err = io.EOF
		} else {
			err = io.EOF
		}
		return
	}

	return fh.buf.Read(int(offset), buf)
}

func (fh *RandomWriteFileHandle) Release() {
	// read buffers

	// write buffers
	fh.buf.Free()

	fh.inode.mu.Lock()
	defer fh.inode.mu.Unlock()

	if fh.inode.fileHandles == 0 {
		panic(fh.inode.fileHandles)
	}

	fh.inode.fileHandles -= 1
}

func (fh *RandomWriteFileHandle) resetToKnownSize() {
	if fh.inode.KnownSize != nil {
		fh.inode.Attributes.Size = *fh.inode.KnownSize
	} else {
		fh.inode.Attributes.Size = 0
		fh.inode.Invalid = true
	}
}

func (fh *RandomWriteFileHandle) FlushFile() (err error) {
	fh.mu.Lock()
	defer fh.mu.Unlock()

	fh.inode.logFuse("FlushFile")

	if !fh.dirty || fh.lastWriteError != nil {
		if fh.lastWriteError != nil {
			err = fh.lastWriteError
			fh.resetToKnownSize()
		}
		return
	}

	fs := fh.inode.fs

	// abort mpu on error
	defer func() {
		if err != nil {
			if fh.mpuId != nil {
				go func() {
					_, _ = fh.cloud.MultipartBlobAbort(fh.mpuId)
					fh.mpuId = nil
				}()
			}

			fh.resetToKnownSize()
		} else {
			if fh.dirty {
				// don't unset this if we never actually flushed
				size := fh.inode.Attributes.Size
				fh.inode.KnownSize = &size
				fh.inode.Invalid = false
			}
			fh.dirty = false
		}

		fh.writeInit = sync.Once{}
		fh.lastPartId = 0
	}()

	fh.mpuWG.Wait()

	if fh.lastWriteError != nil {
		return fh.lastWriteError
	}

	if fh.mpuId == nil {
		return
	}

	_, err = fh.cloud.MultipartBlobCommit(fh.mpuId)
	if err != nil {
		return
	}

	fh.mpuId = nil

	// we want to get key from inode because the file could have been renamed
	_, key := fh.inode.cloud()
	if *fh.mpuName != key {
		// the file was renamed
		err = fh.inode.renameObject(fs, PUInt64(uint64(fh.buf.fileSize)), *fh.mpuName, *fh.inode.FullName())
	}

	return
}
