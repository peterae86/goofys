package internal

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/jacobsa/fuse"
	"io"
	"sync"
	"syscall"
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

	poolHandle *BufferPool
	buf        *MBuf

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

func (fh *RandomWriteFileHandle) mpuPartNoSpawn(buf *MBuf, part uint32, total int64, last bool) (err error) {
	fs := fh.inode.fs

	fs.replicators.Take(1, true)
	defer fs.replicators.Return(1)

	if part == 0 || part > 10000 {
		return errors.New(fmt.Sprintf("invalid part number: %v", part))
	}

	var mpu = MultipartBlobAddInput{
		Commit:     fh.mpuId,
		PartNumber: part,
		Body:       bytes.NewReader(buf.buffers[part-1].buffer),
		//Size:       uint64(buf.Len()),
		//Last:       last,
		//Offset:     uint64(total - int64(buf.Len())),
	}
	defer func() {
		if mpu.Body != nil {
			bufferLog.Debugf("Free %T", buf)
			buf.FreePart(int64(part - 1))
		}
	}()

	_, err = fh.cloud.MultipartBlobAdd(&mpu)

	return
}

func (fh *RandomWriteFileHandle) mpuPart(buf *MBuf, part uint32, total int64) {
	defer func() {
		fh.mpuWG.Done()
	}()

	// maybe wait for CreateMultipartUpload
	if fh.mpuId == nil {
		fh.mpuWG.Wait()
		// initMPU might have errored
		if fh.mpuId == nil {
			return
		}
	}

	err := fh.mpuPartNoSpawn(buf, part, total, false)
	if err != nil {
		if fh.lastWriteError == nil {
			fh.lastWriteError = err
		}
	}
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

func (fh *RandomWriteFileHandle) WriteFileMultipart(offset int64, data []byte) (err error) {
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

	if fh.buf == nil {
		fh.buf = MBuf{}.Init(fh.poolHandle, fh.partSize(), true)
	}

	_, _ = fh.buf.Write(offset, data, func(part int64) {
		if !fh.buf.wbufs[part] {
			err = fh.mpuPartNoSpawn(fh.buf, uint32(part+1), BUF_SIZE, false)
			if err == nil {
				fh.buf.wbufs[part] = true
			}
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
		if bytesRead > 0 {
			fh.readBufOffset += int64(bytesRead)
			fh.seqReadAmount += uint64(bytesRead)
		}

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

	fs := fh.inode.fs

	if fh.poolHandle == nil {
		fh.poolHandle = fs.bufferPool
	}

	if fh.IsFallocateWrite {
		part := offset / fh.buf.size
		if int(part) >= len(fh.buf.buffers) {
			bytesRead = -1
		}
		if int(offset-fh.buf.size*part) < len(fh.buf.buffers[part].buffer) {
			bytesRead = copy(buf, fh.buf.buffers[part].buffer[offset-fh.buf.size*part:])
		} else {
			bytesRead = len(buf)
			if bytesRead > cap(fh.buf.buffers[part].buffer)-int(offset-fh.buf.size*part) {
				bytesRead = cap(fh.buf.buffers[part].buffer) - int(offset-fh.buf.size*part)
			}
		}
		return
	}

	return
}

func (fh *RandomWriteFileHandle) Release() {
	// read buffers

	// write buffers
	if fh.poolHandle != nil {
		if fh.buf != nil && fh.buf.buffers != nil {
			if fh.lastWriteError == nil {
				//panic("buf not freed but error is nil")
			}

			fh.buf.Free()
			// the other in-flight multipart PUT buffers will be
			// freed when they finish/error out
		}
	}

	fh.inode.mu.Lock()
	defer fh.inode.mu.Unlock()

	if fh.inode.fileHandles == 0 {
		panic(fh.inode.fileHandles)
	}

	fh.inode.fileHandles -= 1
}

func (fh *RandomWriteFileHandle) flushSmallFile() (err error) {
	buf := fh.buf
	fh.buf = nil

	if buf == nil {
		buf = MBuf{}.Init(fh.poolHandle, 0, true)
	}

	defer buf.Free()

	fs := fh.inode.fs

	fs.replicators.Take(1, true)
	defer fs.replicators.Return(1)

	// we want to get key from inode because the file could have been renamed
	_, key := fh.inode.cloud()

	var bf []byte
	if len(buf.buffers) > 0 {
		bf = buf.buffers[0].buffer
	}

	resp, err := fh.cloud.PutBlob(&PutBlobInput{
		Key:         key,
		Body:        bytes.NewReader(bf),
		Size:        PUInt64(uint64(len(bf))),
		ContentType: fs.flags.GetMimeType(*fh.inode.FullName()),
	})
	if err != nil {
		fh.lastWriteError = err
	} else {
		inode := fh.inode
		inode.mu.Lock()
		defer inode.mu.Unlock()
		if resp.ETag != nil {
			inode.s3Metadata["etag"] = []byte(*resp.ETag)
		}
		if resp.StorageClass != nil {
			inode.s3Metadata["storage-class"] = []byte(*resp.StorageClass)
		}
	}
	return
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
		fh.nextWriteOffset = 0
		fh.lastPartId = 0
	}()

	if fh.lastPartId == 0 {
		return fh.flushSmallFile()
	}

	fh.mpuWG.Wait()

	if fh.lastWriteError != nil {
		return fh.lastWriteError
	}

	if fh.mpuId == nil {
		return
	}

	if !fh.IsFallocateWrite {
		nParts := fh.lastPartId
		if fh.buf != nil {
			// upload last part
			nParts++
			err = fh.mpuPartNoSpawn(fh.buf, nParts, fh.nextWriteOffset, true)
			if err != nil {
				return
			}
			fh.buf = nil
		}
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
		err = fh.inode.renameObject(fs, PUInt64(uint64(fh.nextWriteOffset)), *fh.mpuName, *fh.inode.FullName())
	}

	return
}
