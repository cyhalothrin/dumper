package query

import (
	"context"
	"strconv"
	"strings"

	"github.com/cyhalothrin/dumper/internal/db"
	"github.com/cyhalothrin/dumper/internal/schema"
)

type SelectBuilder struct {
	tableName     string
	selectColumns []string
	inSubQuery    string
	selectLimit   int
	in            map[string]*whereIn
}

func Select(tableName string) *SelectBuilder {
	return &SelectBuilder{
		tableName: tableName,
	}
}

func (s *SelectBuilder) Columns(columns []string) *SelectBuilder {
	s.selectColumns = columns

	return s
}

func (s *SelectBuilder) InSubQuery(query string) *SelectBuilder {
	s.inSubQuery = query

	return s
}

func (s *SelectBuilder) Exec(ctx context.Context) ([]string, []map[string]any, error) {
	query := "SELECT "

	if len(s.selectColumns) > 0 {
		query += strings.Join(s.selectColumns, ", ")
	} else {
		query += "*"
	}

	query += " FROM " + s.tableName

	var args []any

	if s.inSubQuery != "" {
		query += " WHERE id IN (" + s.inSubQuery + ")"
	} else if len(s.in) > 0 {
		query += " WHERE "

		var concatAnd bool

		for _, in := range s.in {
			if len(in.values) == 0 {
				continue
			}

			if concatAnd {
				query += " AND "
			}

			concatAnd = true
			query += strings.Join(in.columns, ",") + " IN ("

			for j, vals := range in.values {
				if j > 0 {
					query += ","
				}

				for i, val := range vals {
					if i > 0 {
						query += ",?"
					} else {
						query += "(?"
					}

					args = append(args, val)
				}

				query += ")"
			}

			query += ")"
		}
	}

	if s.selectLimit > 0 {
		query += " LIMIT " + strconv.Itoa(s.selectLimit)
	}

	rows, err := db.SourceDB.QueryxContext(ctx, query, args...)
	if err != nil {
		return nil, nil, err
	}

	defer rows.Close()

	var records []map[string]any

	for rows.Next() {
		record := make(map[string]any)

		if err = rows.MapScan(record); err != nil {
			return nil, nil, err
		}

		records = append(records, record)
	}

	if len(records) == 0 {
		return nil, nil, nil
	}

	var columns []string

	if len(s.selectColumns) > 0 {
		columns = s.selectColumns
	} else {
		table, err := schema.GetTable(ctx, s.tableName)
		if err != nil {
			return nil, nil, err
		}

		columns = table.Columns()
	}

	return columns, records, nil
}

func (s *SelectBuilder) Limit(limit int) *SelectBuilder {
	s.selectLimit = limit

	return s
}

func (s *SelectBuilder) WhereIn(columns []string, keys [][]any) *SelectBuilder {
	if s.in == nil {
		s.in = make(map[string]*whereIn)
	}

	s.in[strings.Join(columns, ",")] = &whereIn{
		columns: columns,
		values:  keys,
	}

	return s
}

type whereIn struct {
	columns []string
	values  [][]any
}
