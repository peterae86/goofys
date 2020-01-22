package internal // Copyright 2015 - 2017 Ka-Hing Cheung
import (
	"bytes"
	"github.com/google/uuid"
	_ "github.com/shirou/gopsutil/mem"
	"io"
	"os"
	"sort"
	"sync"
	"sync/atomic"
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

type MarkTreeNode struct {
	start  int
	end    int
	lc     atomic.Value
	rc     atomic.Value
	isFull bool
}

func (mt *MarkTreeNode) mark(st, ed int) bool {
	if mt.isFull {
		return true
	}

	if mt.end < st || mt.start > ed {
		return mt.isFull
	}
	if mt.start >= st || mt.end <= ed {
		mt.isFull = true
		return true
	}
	mid := (mt.end + mt.start) >> 1
	if mt.lc.Load() == nil {
		mt.lc.Store(&MarkTreeNode{
			start:  mt.start,
			end:    mid,
			lc:     atomic.Value{},
			rc:     atomic.Value{},
			isFull: false,
		})
	}
	if mt.rc.Load() == nil {
		mt.rc.Store(&MarkTreeNode{
			start:  mid + 1,
			end:    mt.end,
			lc:     atomic.Value{},
			rc:     atomic.Value{},
			isFull: false,
		})
	}
	if mt.lc.Load().(*MarkTreeNode).mark(st, ed) && mt.rc.Load().(*MarkTreeNode).mark(st, ed) {
		mt.isFull = true
		return true
	} else {
		return false
	}
}

func (mt *MarkTreeNode) checkFull(st, ed int) bool {
	if mt.isFull {
		return true
	}
	if mt.end < st || mt.start > ed {
		return true
	}
	if mt.start >= st || mt.end <= ed {
		return mt.isFull
	}
	if mt.lc.Load() == nil || mt.rc.Load() == nil {
		return false
	}
	if mt.lc.Load().(*MarkTreeNode).checkFull(st, ed) && mt.rc.Load().(*MarkTreeNode).checkFull(st, ed) {
		mt.isFull = true
		return true
	}
	return false
}

type InnerPart struct {
	start      int
	end        int
	length     int
	upload     bool
	uploadLock *sync.Mutex
}

func (ib *InnerPart) write(start int, end int) (n int, err error) {
	//ib.wg.Lock()
	//defer ib.wg.Unlock()
	//ib.wmap.insert(start, end)
	return
}

func (ib *InnerPart) getReaderBytes() []byte {
	return make([]byte, ib.length)
}

type LocalFileBuf struct {
	file          *os.File
	writeMarkTree *MarkTreeNode
	buffers       []*InnerPart
	wbufs         []bool
	bufNum        int
	rp            int
	blockSize     int
	fileSize      int
	fileKey       string
	path          string
	wg            *sync.RWMutex
}

func (mb *LocalFileBuf) Init(size uint64, block bool, path string) *LocalFileBuf {
	sizeRange := 1
	for {
		if sizeRange > int(size) {
			break
		}
		sizeRange = sizeRange << 1
	}
	mb.writeMarkTree = &MarkTreeNode{
		start:  0,
		end:    sizeRange - 1,
		lc:     atomic.Value{},
		rc:     atomic.Value{},
		isFull: false,
	}
	mb.blockSize = 4 * 4 * 1024 * 1024 //16M, max file to 200G
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
		mb.buffers = make([]*InnerPart, 0, mb.bufNum)
		for i := 0; i < mb.bufNum; i++ {
			if i == mb.bufNum-1 {
				mb.buffers = append(mb.buffers, &InnerPart{
					start:      i * mb.blockSize,
					end:        int(size) - 1,
					length:     int(size) - i*mb.blockSize,
					upload:     false,
					uploadLock: &sync.Mutex{},
				})
			} else {
				mb.buffers = append(mb.buffers, &InnerPart{
					start:      i * mb.blockSize,
					end:        (i+1)*mb.blockSize - 1,
					length:     mb.blockSize,
					upload:     false,
					uploadLock: &sync.Mutex{},
				})
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
	_, err = mb.file.WriteAt(p, int64(offset))
	if err != nil {
		return 0, err
	}
	mb.writeMarkTree.mark(offset, offset+len(p)-1)
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
		start += writeLen
		offset += writeLen
		if mb.writeMarkTree.checkFull(b.start, b.end) {
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
	mb.writeMarkTree = nil
	_ = os.Remove(mb.file.Name())
}

func (mb *LocalFileBuf) offsetToPart(offset int) (int, int) {
	return offset / mb.blockSize, offset % mb.blockSize
}
