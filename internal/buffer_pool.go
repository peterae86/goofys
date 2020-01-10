// Copyright 2015 - 2017 Ka-Hing Cheung
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

package internal

import (
	. "github.com/peterae86/goofys/api/common"
	"sort"

	"io"
	"runtime"
	"runtime/debug"
	"sync"

	"github.com/shirou/gopsutil/mem"
)

type BufferPool struct {
	mu   sync.Mutex
	cond *sync.Cond

	numBuffers uint64
	maxBuffers uint64

	totalBuffers       uint64
	computedMaxbuffers uint64

	pool *sync.Pool
}

const BUF_SIZE = 5 * 1024 * 1024

func maxMemToUse(buffersNow uint64) uint64 {
	m, err := mem.VirtualMemory()
	if err != nil {
		panic(err)
	}

	availableMem, err := getCgroupAvailableMem()
	if err != nil {
		log.Debugf("amount of available memory from cgroup is: %v", availableMem/1024/1024)
	}

	if err != nil || availableMem < 0 || availableMem > m.Available {
		availableMem = m.Available
	}

	log.Debugf("amount of available memory: %v", availableMem/1024/1024)

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	log.Debugf("amount of allocated memory: %v %v", ms.Sys/1024/1024, ms.Alloc/1024/1024)

	max := uint64(availableMem+ms.Sys) / 2
	maxbuffers := MaxUInt64(max/BUF_SIZE, 1)
	log.Debugf("using up to %v %vMB buffers, now is %v", maxbuffers, BUF_SIZE/1024/1024, buffersNow)
	return maxbuffers
}

func rounduUp(size uint64, pageSize int) int {
	return pages(size, pageSize) * pageSize
}

func pages(size uint64, pageSize int) int {
	return int((size + uint64(pageSize) - 1) / uint64(pageSize))
}

func (pool BufferPool) Init() *BufferPool {
	pool.cond = sync.NewCond(&pool.mu)

	pool.computedMaxbuffers = pool.maxBuffers
	pool.pool = &sync.Pool{New: func() interface{} {
		return make([]byte, 0, BUF_SIZE)
	}}

	return &pool
}

// for testing
func NewBufferPool(maxSizeGlobal uint64) *BufferPool {
	pool := BufferPool{maxBuffers: maxSizeGlobal / BUF_SIZE}.Init()
	return pool
}

//func (pool *BufferPool) RequestBuffer() (buf []byte) {
//	return pool.RequestMultiple(BUF_SIZE, true)[0]
//}

func (pool *BufferPool) recomputeBufferLimit() {
	if pool.maxBuffers == 0 {
		pool.computedMaxbuffers = maxMemToUse(pool.numBuffers)
		if pool.computedMaxbuffers == 0 {
			panic("OOM")
		}
	}
}

func (pool *BufferPool) RequestMultiple(size uint64, block bool) (buffers []*InnerBuf) {
	nPages := pages(size, BUF_SIZE)

	pool.mu.Lock()
	defer pool.mu.Unlock()

	if pool.totalBuffers%10 == 0 {
		pool.recomputeBufferLimit()
	}

	bufferLog.Debugf("requesting %v", size)

	for pool.numBuffers+uint64(nPages) > pool.computedMaxbuffers {
		if block {
			if pool.numBuffers == 0 {
				pool.MaybeGC()
				pool.recomputeBufferLimit()
				if pool.numBuffers+uint64(nPages) > pool.computedMaxbuffers {
					// we don't have any in use buffers, and we've made attempts to
					// free memory AND correct our limits, yet we still can't allocate.
					// it's likely that we are simply asking for too much
					log.Errorf("Unable to allocate %d bytes, limit is %d bytes",
						nPages*BUF_SIZE, pool.computedMaxbuffers*BUF_SIZE)
					panic("OOM")
				}
			}
			pool.cond.Wait()
		} else {
			return
		}
	}

	for i := 0; i < nPages; i++ {
		pool.numBuffers++
		pool.totalBuffers++
		buf := pool.pool.Get()
		if i != nPages-1 {
			buffers = append(buffers, &InnerBuf{
				buffer: buf.([]byte),
				cap:    BUF_SIZE,
				wmap: &IntervalSet{
					imap: map[int]int{},
				},
			})
		} else {
			buffers = append(buffers, &InnerBuf{
				buffer: buf.([]byte),
				cap:    int(size) - (nPages-1)*BUF_SIZE,
				wmap: &IntervalSet{
					imap: map[int]int{},
				},
			})
		}
	}
	return
}

func (pool *BufferPool) MaybeGC() {
	if pool.numBuffers == 0 {
		debug.FreeOSMemory()
	}
}

func (pool *BufferPool) Free(buf []byte) {
	bufferLog.Debugf("returning %v", len(buf))
	pool.mu.Lock()
	defer pool.mu.Unlock()

	buf = buf[:0]
	pool.pool.Put(buf)
	pool.numBuffers--
	pool.cond.Signal()
}

var mbufLog = GetLogger("mbuf")

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
	buffer []byte
	wmap   *IntervalSet
	length int
	cap    int
	rp     int
}

type MBuf struct {
	pool    *BufferPool
	buffers []*InnerBuf
	rbufs   map[int64]bool
	wbufs   map[int64]bool
	rp      int64
	size    int64
}

func (mb MBuf) Init(h *BufferPool, size uint64, block bool) *MBuf {
	mb.pool = h
	mb.size = BUF_SIZE

	if size != 0 {
		mb.buffers = h.RequestMultiple(size, block)
		if mb.buffers == nil {
			return nil
		}
		mb.wbufs = map[int64]bool{}
		mb.rbufs = map[int64]bool{}
	}

	return &mb
}

//func (mb *MBuf) Len() (length int) {
//	for _, v := range mb.buffers {
//		if v != nil {
//			length += v.wp - v.rp
//		}
//	}
//	return
//}

//// seek only seeks the reader
//func (mb *MBuf) Seek(offset int64, whence int) (int64, error) {
//	switch whence {
//	case 0: // relative to beginning
//		if offset == 0 {
//			mb.rbuf = 0
//			mb.rp = 0
//			return 0, nil
//		}
//	case 1: // relative to current position
//		if offset == 0 {
//			for i := 0; i < mb.rbuf; i++ {
//				offset += int64(len(mb.buffers[i]))
//			}
//			offset += int64(mb.rp)
//			return offset, nil
//		}
//
//	case 2: // relative to the end
//		if offset == 0 {
//			for i := 0; i < len(mb.buffers); i++ {
//				offset += int64(len(mb.buffers[i]))
//			}
//			mb.rbuf = len(mb.buffers)
//			mb.rp = 0
//			return offset, nil
//		}
//	}
//
//	log.Errorf("Seek %d %d", offset, whence)
//	panic(fuse.EINVAL)
//
//	return 0, fuse.EINVAL
//}

func (mb *MBuf) Read(p []byte) (n int, err error) {
	part := mb.rp / mb.size
	n, err = mb.ReadPart(part, p)
	if err != nil {
		return
	}
	mb.rp += int64(n)
	return
}

//
func (mb *MBuf) ReadPart(part int64, p []byte) (n int, err error) {
	if int(part) >= len(mb.buffers) {
		err = io.EOF
		return
	}
	b := mb.buffers[part]
	if b.rp == len(b.buffer) {
		err = io.EOF
		return
	}

	if b.rp == cap(b.buffer) {
		mb.rbufs[part] = true
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

func (mb *MBuf) Full() bool {
	return mb.buffers == nil || len(mb.wbufs) == len(mb.buffers)
}

func (mb *MBuf) Write(offset int64, p []byte, writePartFullCallback func(part int64)) (n int, err error) {
	part := offset / mb.size
	b := mb.buffers[part]

	//if b.wp == cap(b.buffer) {
	//	return
	//} else if b.wp > cap(b.buffer) {
	//	panic("mb.wp > cap(b)")
	//}
	start := int(offset - mb.size*part)
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
		return mb.Write(offset+int64(n), p[n:], writePartFullCallback)
	}
	return
}

func (mb *MBuf) WriteFrom(offset int64, r io.Reader) (n int, err error) {
	part := offset / mb.size
	b := mb.buffers[part]

	n, err = r.Read(b.buffer[offset-part*mb.size : cap(b.buffer)])
	b.wmap.insert(int(offset-mb.size*part), int(offset-mb.size*part)+n)
	// resize the buffer to account for what we just read
	if int(offset-mb.size*part)+n > b.length {
		b.length = int(offset-mb.size*part) + n
		b.buffer = b.buffer[:int(offset-mb.size*part)+n]
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

func (mb *MBuf) Reset() {
	mb.rbufs = map[int64]bool{}
	mb.wbufs = map[int64]bool{}
	for _, v := range mb.buffers {
		if v != nil {
			v.wmap = &IntervalSet{
				imap: map[int]int{},
			}
		}
	}
}

func (mb *MBuf) Close() error {
	mb.Free()
	return nil
}

func (mb *MBuf) Free() {
	for _, b := range mb.buffers {
		mb.pool.Free(b.buffer)
	}

	mb.buffers = nil
}

func (mb *MBuf) FreePart(part int64) {
	mb.pool.Free(mb.buffers[part].buffer)
}

var bufferLog = GetLogger("buffer")

type Buffer struct {
	mu   sync.Mutex
	cond *sync.Cond

	buf    *MBuf
	reader io.ReadCloser
	err    error
}

type ReaderProvider func() (io.ReadCloser, error)

func (b Buffer) Init(buf *MBuf, r ReaderProvider) *Buffer {
	b.buf = buf
	b.cond = sync.NewCond(&b.mu)

	go func() {
		b.readLoop(r)
	}()

	return &b
}

func (b *Buffer) readLoop(r ReaderProvider) {
	offset := int64(0)
	for {
		b.mu.Lock()
		if b.reader == nil {
			b.reader, b.err = r()
			b.cond.Broadcast()
			if b.err != nil {
				b.mu.Unlock()
				break
			}
		}

		if b.buf == nil {
			// buffer was drained
			b.mu.Unlock()
			break
		}

		nread, err := b.buf.WriteFrom(offset, b.reader)
		offset += int64(nread)
		if err != nil {
			b.err = err
			b.mu.Unlock()
			break
		}
		bufferLog.Debugf("wrote %v into buffer", nread)

		if nread == 0 {
			b.reader.Close()
			b.mu.Unlock()
			break
		}

		b.mu.Unlock()
		// if we get here we've read _something_, bounce this goroutine
		// to allow another one to read
		runtime.Gosched()
	}
	bufferLog.Debugf("<-- readLoop()")
}

func (b *Buffer) readFromStream(p []byte) (n int, err error) {
	bufferLog.Debugf("reading %v from stream", len(p))

	n, err = b.reader.Read(p)
	if n != 0 && err == io.ErrUnexpectedEOF {
		err = nil
	} else {
		bufferLog.Debugf("read %v from stream", n)
	}
	return
}

func (b *Buffer) Read(p []byte) (n int, err error) {
	bufferLog.Debugf("Buffer.Read(%v)", len(p))

	b.mu.Lock()
	defer b.mu.Unlock()

	for b.reader == nil && b.err == nil {
		bufferLog.Debugf("waiting for stream")
		b.cond.Wait()
	}

	// we could have received the err before Read was called
	if b.reader == nil {
		if b.err == nil {
			panic("reader and err are both nil")
		}
		err = b.err
		return
	}

	if b.buf != nil {
		bufferLog.Debugf("reading %v from buffer", len(p))

		n, err = b.buf.Read(p)
		if n == 0 {
			b.buf.Free()
			b.buf = nil
			bufferLog.Debugf("drained buffer")
			n, err = b.readFromStream(p)
		} else {
			bufferLog.Debugf("read %v from buffer", n)
		}
	} else if b.err != nil {
		err = b.err
	} else {
		n, err = b.readFromStream(p)
	}

	return
}

func (b *Buffer) ReInit(r ReaderProvider) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.reader != nil {
		b.reader.Close()
		b.reader = nil
	}

	if b.buf != nil {
		b.buf.Reset()
	}

	b.err = nil
	go func() {
		b.readLoop(r)
	}()
}

func (b *Buffer) Close() (err error) {
	bufferLog.Debugf("Buffer.Close()")

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.reader != nil {
		err = b.reader.Close()
	}

	if b.buf != nil {
		b.buf.Free()
		b.buf = nil
	}

	return
}
