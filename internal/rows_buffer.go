package internal

import (
	"bytes"
	"fmt"
	"io"
	"os"
)

const maxMemSize = 50 * 1024 * 1024 // 50 MB

type rowsBuffer struct {
	memBuffers  map[string]*bytes.Buffer
	memSize     int64
	fileBuffers map[string]*os.File
	err         error
}

func newRowsBuffer() *rowsBuffer {
	return &rowsBuffer{
		memBuffers: make(map[string]*bytes.Buffer),
	}
}

func (r *rowsBuffer) writef(table, format string, args ...any) {
	if r.err != nil {
		return
	}

	_, r.err = fmt.Fprintf(r.getMemBuffer(table), format, args...)
}

func (r *rowsBuffer) getMemBuffer(key string) *tableRowsBuffer {
	buf, ok := r.memBuffers[key]
	if !ok {
		buf = new(bytes.Buffer)
		r.memBuffers[key] = buf
	}

	return &tableRowsBuffer{buf: buf, root: r}
}

func (r *rowsBuffer) getFileBuffer(key string) (*os.File, error) {
	if r.fileBuffers == nil {
		r.fileBuffers = make(map[string]*os.File)
	}

	if file, ok := r.fileBuffers[key]; ok {
		return file, nil
	}

	file, err := os.CreateTemp("", "dumper_"+key)
	if err != nil {
		return nil, err
	}

	r.fileBuffers[key] = file

	return file, nil
}

func (r *rowsBuffer) flush() error {
	if r.memSize < maxMemSize {
		return nil
	}

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

	r.memSize = 0

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

func (r *rowsBuffer) addSize(size int64) {
	r.memSize += size
}

func (r *rowsBuffer) getReader(table string) io.Reader {
	if r.err != nil {
		return nil
	}

	file, ok := r.fileBuffers[table]
	if !ok {
		return r.memBuffers[table]
	}

	_, r.err = file.Seek(0, io.SeekStart)

	return io.MultiReader(file, r.memBuffers[table])
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

	t.root.addSize(int64(n))

	if err = t.root.flush(); err != nil {
		return 0, err
	}

	return n, nil
}
