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

	if s.inSubQuery != "" {
		query += " WHERE id IN (" + s.inSubQuery + ")"
	}

	if s.selectLimit > 0 {
		query += " LIMIT " + strconv.Itoa(s.selectLimit)
	}

	rows, err := db.SourceDB.QueryxContext(ctx, query)
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
		columns, err = schema.GetTable(s.tableName).ColumnNames(ctx)
		if err != nil {
			return nil, nil, err
		}
	}

	return columns, records, nil
}

func (s *SelectBuilder) Limit(limit int) *SelectBuilder {
	s.selectLimit = limit

	return s
}
