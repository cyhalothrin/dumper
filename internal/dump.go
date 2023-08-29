package internal

import (
	"context"
	"fmt"
	"strings"

	"slices"

	"github.com/cyhalothrin/dumper/internal/config"
	"github.com/cyhalothrin/dumper/internal/db"
	"github.com/cyhalothrin/dumper/internal/faker"
	"github.com/cyhalothrin/dumper/internal/query"
	"github.com/cyhalothrin/dumper/internal/schema"
)

// TODO: FK and not included tables
// TODO: FK adn ignored columns
// TODO: faker

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

	_, records, err := selectQuery.Exec(ctx)
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

	err = d.selectRelatedRecords(ctx, table, records)
	if err != nil {
		return err
	}

	d.printInsertStatement(table, tableConfig, records)

	return nil
}

func (d *dumper) selectRelatedRecords(ctx context.Context, table *schema.Table, records []map[string]any) error {
	for _, fk := range table.ForeignKeys() {
		keys := d.extractFkValuesFromRecord(records, fk)

		referencedTable, err := schema.GetTable(ctx, fk.ReferencedTableName)
		if err != nil {
			return err
		}

		keys = d.filterForeignKeys(referencedTable, keys)
		if len(keys) == 0 {
			continue
		}

		fkSelectQuery := query.Select(fk.ReferencedTableName).WhereIn(fk.ReferencedColumns, keys)

		if err = d.selectRecords(ctx, referencedTable, fkSelectQuery); err != nil {
			return err
		}
	}

	return nil
}

func (d *dumper) extractFkValuesFromRecord(records []map[string]any, fk *schema.ForeignKey) [][]any {
	keys := make([][]any, 0, len(records))

	for _, record := range records {
		var tuple []any

		for _, fkColumn := range fk.Columns {
			fkVal := record[fkColumn]
			if fkVal == nil {
				tuple = nil

				break
			}

			tuple = append(tuple, fkVal)
		}

		if len(tuple) > 0 {
			keys = append(keys, tuple)
		}
	}

	return keys
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
		pkVal := table.PrimaryKey.FormatFromRecord(record)

		if !d.selectedRecords[table.Name][pkVal] {
			d.selectedRecords[table.Name][pkVal] = true
			index++
		}
	}

	return records[:index], nil
}

func (d *dumper) filterForeignKeys(table *schema.Table, keys [][]any) [][]any {
	if len(keys) == 0 {
		return nil
	}

	if d.selectedRecords[table.Name] == nil {
		d.selectedRecords[table.Name] = make(map[string]bool)
	}

	var index int

	for _, key := range keys {
		pkVal := table.PrimaryKey.Format(key)

		if !d.selectedRecords[table.Name][pkVal] {
			index++
		}
	}

	return keys[:index]
}

func (*dumper) printInsertStatement(table *schema.Table, tableConfig config.TableConfig, records []map[string]any) {
	columns := table.Columns()

	fmt.Printf("INSERT INTO %s (%s) VALUES ", table.Name, strings.Join(columns, ", "))

	for i, record := range records {
		if i > 0 {
			fmt.Print(",\n\t")
		}

		fmt.Print("(")

		for j, column := range columns {
			if j > 0 {
				fmt.Print(", ")
			}

			if tableConfig.IsIgnoredColumn(column) {
				fmt.Print(table.Column(column).DefaultValue())
			} else if fakerConfig := tableConfig.UseFaker(column); fakerConfig != nil {
				fmt.Print(faker.Format(fakerConfig))
			} else {
				fmt.Print(table.Column(column).Format(record[column]))
			}
		}

		fmt.Print(")")
	}

	fmt.Println(";")
}

func (d *dumper) getTableConfig(name string) (config.TableConfig, bool) {
	tableConf, ok := config.Config.Tables[name]

	return tableConf, ok
}
