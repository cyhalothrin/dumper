package internal

import (
	"bytes"
	"os"
	"sync"
	"sync/atomic"
)

const maxMemSize = 50 * 1024 * 1024

type rowsBuffer struct {
	mx          sync.Mutex
	memBuffers  map[string]*bytes.Buffer
	memSize     *atomic.Int64
	fileBuffers map[string]*os.File
}

func newRowsBuffer() *rowsBuffer {
	return &rowsBuffer{
		memBuffers: make(map[string]*bytes.Buffer),
		memSize:    &atomic.Int64{},
	}
}

func (r *rowsBuffer) getMemBuffer(key string) *tableRowsBuffer {
	r.mx.Lock()
	defer r.mx.Unlock()

	buf, ok := r.memBuffers[key]
	if !ok {
		buf = new(bytes.Buffer)
		r.memBuffers[key] = buf
	}

	return &tableRowsBuffer{buf: buf, root: r}
}

func (r *rowsBuffer) getFileBuffer(key string) (*os.File, error) {
	r.mx.Lock()
	defer r.mx.Unlock()

	if r.fileBuffers == nil {
		r.fileBuffers = make(map[string]*os.File)
	}

	file, err := os.CreateTemp("", "dumper_"+key)
	if err != nil {
		return nil, err
	}

	r.fileBuffers[key] = file

	return file, nil
}

func (r *rowsBuffer) flush() error {
	if r.memSize.Load() < maxMemSize {
		return nil
	}

	r.mx.Lock()
	defer r.mx.Unlock()

	for key, buf := range r.memBuffers {
		file, err := r.getFileBuffer(key)
		if err != nil {
			return err
		}

		if _, err = buf.WriteTo(file); err != nil {
			return err
		}

		buf.Reset()
	}

	r.memBuffers = nil
	r.memSize.Store(0)

	return nil
}

func (r *rowsBuffer) clear() error {
	for _, file := range r.fileBuffers {
		if err := file.Close(); err != nil {
			return err
		}

		if err := os.Remove(file.Name()); err != nil {
			return err
		}
	}

	return nil
}

func (r *rowsBuffer) addSize(size int64) int64 {
	return r.memSize.Add(size)
}

type tableRowsBuffer struct {
	buf  *bytes.Buffer
	root *rowsBuffer
}

func (t *tableRowsBuffer) Write(p []byte) (int, error) {
	n, err := t.buf.Write(p)
	if err != nil {
		return 0, err
	}

	if err = t.root.flush(); err != nil {
		return 0, err
	}

	return n, nil
}
