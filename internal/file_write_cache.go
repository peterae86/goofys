package internal // Copyright 2015 - 2017 Ka-Hing Cheung
import (
	"bytes"
	"github.com/google/uuid"
	_ "github.com/shirou/gopsutil/mem"
	"io"
	"os"
	"sort"
	"sync"
)

//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

type IntervalSet struct {
	imap map[int]int
	keys []int
}

func (is *IntervalSet) insert(a, b int) {
	if v, ok := is.imap[a]; !ok || b > v {
		is.imap[a] = b
		if !ok {
			is.keys = append(is.keys, a)
			sort.Ints(is.keys)
		}
	}
}

func (is *IntervalSet) len() int {
	var preB int
	var total int
	for _, a := range is.keys {
		b := is.imap[a]
		if a >= preB {
			total += b - a
		} else {
			if b > preB {
				total += b - preB
			}
		}
		if b > preB {
			preB = b
		}
	}
	return total
}

type InnerBuf struct {
	wmap       *IntervalSet
	length     int
	cap        int
	rp         int
	wg         *sync.RWMutex
	upload     bool
	uploadLock *sync.Mutex
}

func (ib *InnerBuf) write(start int, end int) (n int, err error) {
	ib.wmap.insert(start, end)
	return
}

func (ib *InnerBuf) getReaderBytes() []byte {
	return make([]byte, ib.length)
}

func (ib *InnerBuf) init(length int) error {
	var err error
	ib.wmap = &IntervalSet{
		imap: map[int]int{},
		keys: nil,
	}
	ib.wg = &sync.RWMutex{}
	ib.uploadLock = &sync.Mutex{}
	ib.length = length
	return err
}

type LocalFileBuf struct {
	file      *os.File
	buffers   []*InnerBuf
	wbufs     []bool
	bufNum    int
	rp        int
	blockSize int
	fileSize  int
	fileKey   string
	path      string
	wg        *sync.RWMutex
}

func (mb *LocalFileBuf) Init(size uint64, block bool, path string) *LocalFileBuf {
	mb.blockSize = BUF_SIZE * 4 //20M, max file to 200G
	mb.fileSize = int(size)
	mb.path = path
	mb.wg = &sync.RWMutex{}
	var err error
	mb.file, err = os.Create(path + "/" + uuid.New().String())
	if err != nil {
		panic(err)
	}
	err = mb.file.Truncate(int64(size))
	if err != nil {
		panic(err)
	}
	if size != 0 {
		mb.bufNum = (mb.fileSize + mb.blockSize - 1) / mb.blockSize
		mb.wbufs = make([]bool, 0, mb.bufNum)
		mb.buffers = make([]*InnerBuf, 0, mb.bufNum)
		for i := 0; i < mb.bufNum; i++ {
			mb.buffers = append(mb.buffers, &InnerBuf{})
			var err error
			if i == mb.bufNum-1 {
				err = mb.buffers[i].init(int(size) - i*mb.blockSize)
			} else {
				err = mb.buffers[i].init(mb.blockSize)
			}
			if err != nil {
				panic(err)
			}
		}
	}

	return mb
}
func (mb *LocalFileBuf) getPartReader(part int) (io.ReadSeeker, error) {
	b := mb.buffers[part].getReaderBytes()
	_, err := mb.file.ReadAt(b, int64(part*mb.blockSize))
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(b), err
}

//
func (mb *LocalFileBuf) Read(offset int, p []byte) (n int, err error) {
	n, err = mb.file.ReadAt(p, int64(offset))
	return
}

func (mb *LocalFileBuf) Write(offset int, p []byte, writePartFullCallback func(part int)) (n int, err error) {
	start := 0
	for {
		part, partOffset := mb.offsetToPart(offset)
		b := mb.buffers[part]
		writeLen := b.length - partOffset
		if start+writeLen >= len(p) {
			writeLen = len(p) - start
		}
		if writeLen == 0 {
			break
		}
		_, _ = b.write(partOffset, partOffset+writeLen)
		mb.wg.Lock()
		_, err = mb.file.WriteAt(p[start:start+writeLen], int64(offset))
		mb.wg.Unlock()
		if err != nil {
			return 0, err
		}
		start += writeLen
		offset += writeLen
		if b.wmap.len() == b.length {
			go func(part int) {
				b.uploadLock.Lock()
				defer b.uploadLock.Unlock()
				if !b.upload {
					writePartFullCallback(part)
					b.upload = true
				}
			}(part)
		}
	}

	return len(p), nil
}

func (mb *LocalFileBuf) WriteFrom(offset int64, r io.Reader) (n int, err error) {
	return 0, nil
}

func (mb *LocalFileBuf) Reset() {
	mb.Free()
	mb.Init(uint64(mb.fileSize), true, mb.path)
}

func (mb *LocalFileBuf) Close() error {
	mb.Free()
	return nil
}

func (mb *LocalFileBuf) Free() {
	mb.buffers = nil
	_ = os.Remove(mb.file.Name())
}

func (mb *LocalFileBuf) offsetToPart(offset int) (int, int) {
	return offset / mb.blockSize, offset % mb.blockSize
}
