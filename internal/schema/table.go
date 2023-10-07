package schema

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/cyhalothrin/dumper/internal/config"
	"github.com/cyhalothrin/dumper/internal/db"
)

type Table struct {
	Name         string
	foreignKeys  []*ForeignKey
	PrimaryKey   *PrimaryKey
	columnsOrder []string
	columns      map[string]Column
}

func GetTable(ctx context.Context, tableName string) (*Table, error) {
	if t, ok := tables[tableName]; ok {
		return t, nil
	}

	table, err := describeTable(ctx, tableName)
	if err != nil {
		return nil, err
	}

	tables[tableName] = table

	return table, nil
}

func (t *Table) GetNotNullColumns() []string {
	var notNullColumns []string

	for _, column := range t.columns {
		if !column.Nullable {
			notNullColumns = append(notNullColumns, column.Name)
		}
	}

	return notNullColumns
}

func (t *Table) Column(name string) Column {
	return t.columns[name]
}

func (t *Table) Columns() []string {
	columnsCopy := make([]string, len(t.columnsOrder))

	copy(columnsCopy, t.columnsOrder)

	return columnsCopy
}

func (t *Table) ForeignKey(columnName string) *ForeignKey {
	for _, fk := range t.foreignKeys {
		for _, col := range fk.Columns {
			if col == columnName {
				return fk
			}
		}
	}

	return nil
}

func (t *Table) SortColumns(columns []string) {
	colPositions := make(map[string]int, len(columns))

	for i, col := range columns {
		colPositions[col] = i
	}

	sort.Slice(columns, func(i, j int) bool {
		return colPositions[columns[i]] < colPositions[columns[j]]
	})
}

func (t *Table) CreateTableStatement(ctx context.Context) (string, error) {
	var (
		statement string
		tableName string
	)

	err := db.SourceDB.QueryRowxContext(ctx, "SHOW CREATE TABLE "+t.Name).Scan(&tableName, &statement)
	if err != nil {
		return "", fmt.Errorf("get table %s create statement: %w", t.Name, err)
	}

	if config.Config.Dump.CreateTablesIfNotExist {
		statement = strings.Replace(statement, "CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1)
	}

	return statement, nil
}

func (t *Table) ForeignKeys() []*ForeignKey {
	fks := make([]*ForeignKey, len(t.foreignKeys))
	copy(fks, t.foreignKeys)

	return fks
}

func describeTable(ctx context.Context, name string) (*Table, error) {
	t := &Table{
		Name: name,
	}

	if err := describeColumns(ctx, t); err != nil {
		return nil, err
	}

	if err := describePrimaryKey(ctx, t); err != nil {
		return nil, err
	}

	if err := describeForeignKeys(ctx, t); err != nil {
		return nil, err
	}

	return t, nil
}

func describeColumns(ctx context.Context, table *Table) error {
	table.columns = make(map[string]Column)

	type columnDescription struct {
		Field   string         `db:"Field"`
		Type    string         `db:"Type"`
		Null    string         `db:"Null"`
		Key     string         `db:"Key"`
		Extra   string         `db:"Extra"`
		Default sql.NullString `db:"Default"`
	}

	var tableDescription []columnDescription

	err := db.SourceDB.SelectContext(ctx, &tableDescription, "DESCRIBE "+table.Name)
	if err != nil {
		return fmt.Errorf("get table %s description: %w", table.Name, err)
	}

	for _, columnDesc := range tableDescription {
		table.columnsOrder = append(table.columnsOrder, columnDesc.Field)

		table.columns[columnDesc.Field] = Column{
			Name:     columnDesc.Field,
			Nullable: columnDesc.Null == "YES",
			dbType:   columnDesc.Type,
			Default:  columnDesc.Default,
		}
	}

	return nil
}

func describeForeignKeys(ctx context.Context, table *Table) error {
	type fkDescription struct {
		ColumnName           string `db:"column_name"`
		ReferencedTable      string `db:"REFERENCED_TABLE_NAME"`
		ReferencedColumnName string `db:"referenced_column_name"`
	}

	var records []fkDescription

	query := `SELECT GROUP_CONCAT(COLUMN_NAME) AS column_name, 
		REFERENCED_TABLE_NAME, 
		GROUP_CONCAT(REFERENCED_COLUMN_NAME) AS referenced_column_name
	FROM information_schema.KEY_COLUMN_USAGE
	WHERE REFERENCED_TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND REFERENCED_TABLE_NAME IS NOT NULL
	GROUP BY CONSTRAINT_NAME`

	err := db.SourceDB.SelectContext(ctx, &records, query, table.Name)
	if err != nil {
		return fmt.Errorf("get table %s foreign keys: %w", table.Name, err)
	}

	table.foreignKeys = make([]*ForeignKey, 0, len(records)) // To show that we have already tried to get foreign keys

	for _, fkDesc := range records {
		table.foreignKeys = append(table.foreignKeys, &ForeignKey{
			Columns:             strings.Split(fkDesc.ColumnName, ","),
			ReferencedTableName: fkDesc.ReferencedTable,
			ReferencedColumns:   strings.Split(fkDesc.ReferencedColumnName, ","),
		})
	}

	return nil
}

func describePrimaryKey(ctx context.Context, table *Table) error {
	rows, err := db.SourceDB.QueryxContext(ctx, "SHOW KEYS FROM "+table.Name+" WHERE Key_name = 'PRIMARY'")
	if err != nil {
		return fmt.Errorf("get table %s primary key: %w", table.Name, err)
	}

	defer rows.Close()

	var columns []string

	for rows.Next() {
		record := make(map[string]any)
		if err = rows.MapScan(record); err != nil {
			return fmt.Errorf("get table %s primary key: %w", table.Name, err)
		}

		columns = append(columns, string(record["Column_name"].([]uint8)))
	}

	if err = rows.Err(); err != nil {
		return fmt.Errorf("get table %s primary key: %w", table.Name, err)
	}

	if len(columns) == 0 {
		return nil
	}

	table.PrimaryKey = &PrimaryKey{
		Columns: columns,
		table:   table,
	}

	return nil
}
