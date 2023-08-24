package schema

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/cyhalothrin/dumper/internal/db"
)

type Table struct {
	Name        string
	columnNames []string
	columns     map[string]Column
}

func GetTable(tableName string) *Table {
	if t, ok := tables[tableName]; ok {
		return t
	}

	t := &Table{
		Name: tableName,
	}

	tables[tableName] = t

	return t
}

func (t *Table) describe(ctx context.Context) error {
	if t.columns != nil {
		return nil
	}

	t.columns = make(map[string]Column)

	var tableDescription []columnDescription

	err := db.SourceDB.SelectContext(ctx, &tableDescription, "DESCRIBE "+t.Name)
	if err != nil {
		return fmt.Errorf("get table %s description: %w", t.Name, err)
	}

	for _, columnDesc := range tableDescription {
		t.columnNames = append(t.columnNames, columnDesc.Field)

		t.columns[columnDesc.Field] = Column{
			Name:     columnDesc.Field,
			Nullable: columnDesc.Null == "YES",
			Type:     columnDesc.Type,
		}
	}

	return nil
}

func (t *Table) GetNotNullColumns(ctx context.Context) ([]string, error) {
	if err := t.describe(ctx); err != nil {
		return nil, err
	}

	var notNullColumns []string

	for _, column := range t.columns {
		if !column.Nullable {
			notNullColumns = append(notNullColumns, column.Name)
		}
	}

	return notNullColumns, nil
}

func (t *Table) ColumnNames(ctx context.Context) ([]string, error) {
	if err := t.describe(ctx); err != nil {
		return nil, err
	}

	return t.columnNames, nil
}

func (t *Table) Column(name string) Column {
	return t.columns[name]
}

type columnDescription struct {
	Field   string         `db:"Field"`
	Type    string         `db:"Type"`
	Null    string         `db:"Null"`
	Key     string         `db:"Key"`
	Extra   string         `db:"Extra"`
	Default sql.NullString `db:"Default"`
}
