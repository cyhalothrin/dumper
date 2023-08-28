package internal

import (
	"context"
	"fmt"
	"strings"

	"slices"

	"github.com/cyhalothrin/dumper/internal/config"
	"github.com/cyhalothrin/dumper/internal/db"
	"github.com/cyhalothrin/dumper/internal/query"
	"github.com/cyhalothrin/dumper/internal/schema"
)

func Init(ctx context.Context) error {
	config.Normalize()

	return db.Connect(ctx)
}

func Dump(ctx context.Context) error {
	d := &dumper{
		selectedRecords: make(map[string]map[string]bool),
	}

	return d.do(ctx)
}

type dumper struct {
	// selectedRecords выбранные записи по таблицам чтобы не уйти в бесконечный цикл из-за внешних ключей
	selectedRecords map[string]map[string]bool
}

func (d *dumper) do(ctx context.Context) (err error) {
	if err = d.dumpTables(ctx); err != nil {
		return err
	}

	return nil
}

func (d *dumper) dumpTables(ctx context.Context) error {
	for _, tableConf := range config.Config.Tables {
		if tableConf.SelectQuery != "" {
			if err := d.dumpTable(ctx, tableConf); err != nil {
				return err
			}
		}
	}

	return nil
}

func (d *dumper) dumpTable(ctx context.Context, tableConf config.TableConfig) error {
	selectQuery := query.Select(tableConf.Name).InSubQuery(tableConf.SelectQuery).Limit(tableConf.Limit)
	table, err := schema.GetTable(ctx, tableConf.Name)
	if err != nil {
		return err
	}

	return d.selectRecords(ctx, table, selectQuery)
}

func (d *dumper) selectRecords(ctx context.Context, table *schema.Table, selectQuery *query.SelectBuilder) error {
	tableConfig, ok := d.getTableConfig(table.Name)
	if !ok {
		return nil
	}

	selectQuery.Columns(d.getSelectColumnsFor(table, tableConfig))

	columns, records, err := selectQuery.Exec(ctx)
	if err != nil {
		return err
	}

	records, err = d.filterRecords(table, records)
	if err != nil {
		return err
	}

	if len(records) == 0 {
		return nil
	}

	for _, columnName := range columns {
		fk := table.ForeignKey(columnName)
		if fk == nil {
			continue
		}

		var keys [][]any

		for _, record := range records {
			var tuple []any

			for _, fkColumn := range fk.Columns {
				tuple = append(tuple, record[fkColumn])
			}

			keys = append(keys, tuple)
		}

		referencedTable, err := schema.GetTable(ctx, fk.ReferencedTableName)
		if err != nil {
			return err
		}

		fkSelectQuery := query.Select(fk.ReferencedTableName).WhereIn(fk.ReferencedColumns, keys)

		// TODO: remove already selected keys
		err = d.selectRecords(ctx, referencedTable, fkSelectQuery)
		if err != nil {
			return err
		}
	}

	d.printInsertStatement(table, columns, records)

	return nil
}

func (*dumper) getSelectColumnsFor(table *schema.Table, tableConfig config.TableConfig) []string {
	if len(tableConfig.AllowColumns) == 0 && len(tableConfig.IgnoreColumns) == 0 {
		return nil
	}

	tableColumns := table.Columns()

	var columns []string

	if len(tableConfig.AllowColumns) > 0 {
		columns = make([]string, len(tableConfig.AllowColumns))
		copy(columns, tableConfig.AllowColumns)

		for _, colName := range tableColumns {
			if !slices.Contains(tableConfig.AllowColumns, colName) &&
				(table.ForeignKey(colName) != nil || table.PrimaryKey.Contains(colName)) {
				columns = append(columns, colName)
			}
		}
	} else {
		columns = tableColumns
	}

	if len(tableConfig.IgnoreColumns) > 0 {
		for _, colName := range tableConfig.IgnoreColumns {
			if table.ForeignKey(colName) != nil || table.PrimaryKey.Contains(colName) {
				continue
			}

			if i := slices.Index(columns, colName); i != -1 {
				columns = slices.Delete(columns, i, i+1)
			}
		}
	}

	table.SortColumns(columns)

	return columns
}

func (d *dumper) filterRecords(table *schema.Table, records []map[string]any) ([]map[string]any, error) {
	if d.selectedRecords[table.Name] == nil {
		d.selectedRecords[table.Name] = make(map[string]bool)
	}

	var index int

	for _, record := range records {
		pkVal, err := table.PrimaryKey.Format(record)
		if err != nil {
			return nil, err
		}

		if !d.selectedRecords[table.Name][pkVal] {
			d.selectedRecords[table.Name][pkVal] = true
			index++
		}
	}

	return records[:index], nil
}

func (*dumper) printInsertStatement(table *schema.Table, columns []string, records []map[string]any) {
	fmt.Printf("INSERT INTO %s (%s) VALUES ", table.Name, strings.Join(columns, ", "))

	for i, record := range records {
		if i > 0 {
			fmt.Print(",\n\t")
		}

		fmt.Print("(")

		for i, column := range columns {
			if i > 0 {
				fmt.Print(", ")
			}

			fmt.Print(table.Column(column).Format(record[column]))
		}

		fmt.Print(")")
	}

	fmt.Println(";")
}

func (d *dumper) getTableConfig(name string) (config.TableConfig, bool) {
	tableConf, ok := config.Config.Tables[name]

	return tableConf, ok
}
