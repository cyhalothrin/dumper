package internal

import (
	"os"

	"github.com/akrylysov/pogreb"
)

const maxSelectedIDsInMem = 1

type selectedIDsStorage struct {
	m          map[string]map[string]bool
	db         *pogreb.DB
	dbFilePath string
	size       int
	err        error
}

func newSelectedIDsStorage() *selectedIDsStorage {
	return &selectedIDsStorage{
		m: make(map[string]map[string]bool),
	}
}

func (s *selectedIDsStorage) add(table, id string) {
	if s.err != nil {
		return
	}

	if _, ok := s.m[table]; !ok {
		s.m[table] = make(map[string]bool)
	}

	s.m[table][id] = true
	s.size++

	if s.size >= maxSelectedIDsInMem {
		if err := s.flush(); err != nil {
			s.err = err
		}
	}
}

func (s *selectedIDsStorage) flush() error {
	if err := s.openDb(); err != nil {
		return err
	}

	for table, ids := range s.m {
		for id := range ids {
			err := s.db.Put([]byte(table+"_"+id), nil)
			if err != nil {
				return err
			}
		}

		s.m[table] = make(map[string]bool)
	}

	return nil
}

func (s *selectedIDsStorage) openDb() error {
	if s.db != nil {
		return nil
	}

	dir, err := os.MkdirTemp("", "dumper")
	if err != nil {
		return err
	}

	s.dbFilePath = dir + "/dumper.pogreb"
	s.db, err = pogreb.Open(s.dbFilePath, nil)

	return err
}

func (s *selectedIDsStorage) clear() error {
	if s.db != nil {
		dbCloseErr := s.db.Close()

		if err := os.RemoveAll(s.dbFilePath); err != nil {
			return err
		}

		return dbCloseErr
	}

	return s.err
}

func (s *selectedIDsStorage) has(table, id string) bool {
	if s.m[table][id] {
		return true
	}

	if s.db == nil {
		return false
	}

	has, err := s.db.Has([]byte(table + "_" + id))
	if err != nil {
		s.err = err

		return false
	}

	return has
}

func (s *selectedIDsStorage) hasTable(table string) bool {
	_, ok := s.m[table]

	return ok
}
