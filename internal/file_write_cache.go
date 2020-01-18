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
	file   *os.File
	wmap   *IntervalSet
	length int
	cap    int
	rp     int
	wg     *sync.RWMutex
}

func (ib *InnerBuf) write(offset int, bytes []byte) (n int, err error) {
	ib.wg.Lock()
	defer ib.wg.Unlock()
	n, err = ib.file.WriteAt(bytes, int64(offset))
	ib.wmap.insert(offset, offset+n)
	return
}

func (ib *InnerBuf) read(offset int, bytes []byte) (n int, err error) {
	ib.wg.RLock()
	defer ib.wg.RUnlock()
	return ib.file.ReadAt(bytes, int64(offset))
}

func (ib *InnerBuf) getReader() (io.ReadSeeker, error) {
	ib.wg.RLock()
	defer ib.wg.RUnlock()
	data := make([]byte, 0, ib.length)
	_, err := ib.file.Read(data)
	return bytes.NewReader(data), err
}

func (ib *InnerBuf) init() error {
	var err error
	ib.file, err = os.Create("/Users/" + uuid.New().String())
	ib.wmap = &IntervalSet{
		imap: map[int]int{},
		keys: nil,
	}
	ib.wg = &sync.RWMutex{}
	return err
}

func (ib *InnerBuf) free() error {
	var err error
	err = os.Remove(ib.file.Name())
	return err
}

type LocalFileBuf struct {
	buffers   []*InnerBuf
	wbufs     []bool
	bufNum    int
	rp        int
	blockSize int
	fileSize  int
	fileKey   string
	wg        *sync.RWMutex
}

func (mb LocalFileBuf) Init(size uint64, block bool) *LocalFileBuf {
	mb.blockSize = BUF_SIZE * 4 //20M, max file to 200G
	mb.fileSize = int(size)
	mb.wg = &sync.RWMutex{}

	if size != 0 {
		mb.bufNum = (mb.fileSize + mb.blockSize - 1) / mb.blockSize
		mb.wbufs = make([]bool, 0, mb.bufNum)
		mb.buffers = make([]*InnerBuf, 0, mb.bufNum)
	}

	return &mb
}

//
func (mb *LocalFileBuf) Read(offset int, p []byte) (n int, err error) {
	part :=
	if int(part) >= len(mb.buffers) {
		err = io.EOF
		return
	}
	b := mb.buffers[part]
	if b.rp == len(b.buffer) {
		err = io.EOF
		return
	}

	if len(mb.rbufs) == len(mb.buffers) {
		err = io.EOF
		return
	} else if len(mb.rbufs) > len(mb.buffers) {
		panic("mb.cur > len(mb.buffers)")
	}

	n = copy(p, b.buffer[b.rp:])
	b.rp += n

	return
}

func (mb *LocalFileBuf) Write(offset int, p []byte, writePartFullCallback func(part int)) (n int, err error) {
	part, partOffset := mb.offsetToPart(offset)
	b := mb.buffers[part]
	b.write(partOffset, p)
	n = copy(b.buffer[start:cap(b.buffer)], p)
	// resize the buffer to account for what we just read
	if start+n > b.length {
		b.length = start + n
		b.buffer = b.buffer[:b.length]
	}
	b.wmap.insert(start, start+n)
	if b.wmap.len() == b.cap {
		if writePartFullCallback != nil {
			writePartFullCallback(part)
		}
		mb.wbufs[part] = true
	}
	if n < len(p) {
		return mb.Write(offset+n, p[n:], writePartFullCallback)
	}
	return
}

func (mb *LocalFileBuf) WriteFrom(offset int64, r io.Reader) (n int, err error) {
	part := offset / mb.blockSize
	b := mb.buffers[part]

	n, err = r.Read(b.buffer[offset-part*mb.blockSize : cap(b.buffer)])
	b.wmap.insert(int(offset-mb.blockSize*part), int(offset-mb.blockSize*part)+n)
	// resize the buffer to account for what we just read
	if int(offset-mb.blockSize*part)+n > b.length {
		b.length = int(offset-mb.blockSize*part) + n
		b.buffer = b.buffer[:int(offset-mb.blockSize*part)+n]
	}
	if b.wmap.len() == b.cap {
		mb.wbufs[part] = true
	}
	if n > 0 {
		return mb.WriteFrom(offset+int64(n), r)
	} else {
		return 0, nil
	}
}

func (mb *LocalFileBuf) Reset() {

}

func (mb *LocalFileBuf) Close() error {
	mb.Free()
	return nil
}

func (mb *LocalFileBuf) Free() {
	for _, b := range mb.buffers {
		_ = b.free()
	}

	mb.buffers = nil
}

func (mb *LocalFileBuf) FreePart(part int64) {
	err := mb.buffers[part].free()
	if err != nil {
		println(err)
	}
	mb.buffers[part] = nil
}

func (mb *LocalFileBuf) offsetToPart(offset int) (int, int) {
	return offset / mb.blockSize, offset % mb.blockSize
}
