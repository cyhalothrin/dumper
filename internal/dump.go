package internal

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"regexp"
	"slices"
	"strings"

	"github.com/cyhalothrin/dumper/internal/config"
	"github.com/cyhalothrin/dumper/internal/db"
	"github.com/cyhalothrin/dumper/internal/faker"
	"github.com/cyhalothrin/dumper/internal/query"
	"github.com/cyhalothrin/dumper/internal/schema"
)

func Init(ctx context.Context) error {
	config.Normalize()

	return db.Connect(ctx)
}

func Dump(ctx context.Context) error {
	var out io.Writer = os.Stdout

	if config.Config.Dump.Out != "" {
		file, err := os.Create(config.Config.Dump.Out)
		if err != nil {
			return fmt.Errorf("failed to create dump file: %w", err)
		}

		defer func() {
			_ = file.Close()
		}()

		out = file
	}

	d := &dumper{
		selectedRecords:     make(map[string]map[string]bool),
		tableInsertsBuffers: make(map[string]io.ReadWriter),
		dumpTarget:          out,
	}

	return d.do(ctx)
}

type dumper struct {
	// selectedRecords выбранные записи по таблицам чтобы не уйти в бесконечный цикл из-за внешних ключей
	selectedRecords     map[string]map[string]bool
	tableInsertsBuffers map[string]io.ReadWriter
	tablesInsertOrder   []string
	writeErr            error
	disableFKChecks     bool
	dumpTarget          io.Writer
}

func (d *dumper) do(ctx context.Context) (err error) {
	if err = d.dumpTables(ctx); err != nil {
		return err
	}

	return d.writeErr
}

func (d *dumper) dumpTables(ctx context.Context) error {
	for _, tableConf := range config.Config.Tables {
		if len(tableConf.SelectQuery) > 0 {
			if err := d.dumpTable(ctx, tableConf); err != nil {
				return err
			}
		}
	}

	return d.printDump(ctx)
}

func (d *dumper) dumpTable(ctx context.Context, tableConf config.TableConfig) error {
	for _, selectPKs := range tableConf.SelectQuery {
		selectQuery := query.Select(tableConf.Name).InSubQuery(selectPKs).Limit(tableConf.Limit)

		table, err := schema.GetTable(ctx, tableConf.Name)
		if err != nil {
			return err
		}

		err = d.selectRecords(ctx, table, selectQuery)
		if err != nil {
			return err
		}
	}

	return nil
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
	if d.selectedRecords[table.Name] != nil {
		d.disableFKChecks = true
	}

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

func (d *dumper) printInsertStatement(table *schema.Table, tableConfig config.TableConfig, records []map[string]any) {
	columns := table.Columns()
	w := d.tableInsertsBuffers[table.Name]
	d.tablesInsertOrder = append(d.tablesInsertOrder, table.Name)

	if w == nil {
		w = bytes.NewBuffer(nil)
		d.tableInsertsBuffers[table.Name] = w
	} else {
		d.writef(w, "\n")
	}

	d.writef(w, "INSERT INTO %s (%s) VALUES \n\t", table.Name, strings.Join(columns, ", "))

	for i, record := range records {
		if i > 0 {
			d.writef(w, ",\n\t")
		}

		d.writef(w, "(")

		for j, column := range columns {
			if j > 0 {
				d.writef(w, ", ")
			}

			if config.Config.Dump.AddColumnName {
				d.writef(w, "\n\t\t# %s\n\t\t", column)
			}

			if tableConfig.IsIgnoredColumn(column) {
				// Print default value if column is ignored and it is required
				d.writef(w, table.Column(column).DefaultValue())
			} else if fakerConfig := tableConfig.UseFaker(column); fakerConfig != nil {
				// Use fakers
				d.writef(w, table.Column(column).Format(faker.Format(fakerConfig)))
			} else {
				// Print column value
				d.writef(w, table.Column(column).Format(record[column]))
			}
		}

		d.writef(w, ")")
	}

	d.printOnDuplicateKeyUpdateStatement(w, table, tableConfig)

	d.writef(w, ";")
}

func (d *dumper) printOnDuplicateKeyUpdateStatement(w io.Writer, table *schema.Table, tableConfig config.TableConfig) {
	d.writef(w, " ON DUPLICATE KEY UPDATE ")

	var colNum int

	for _, column := range table.Columns() {
		if !tableConfig.IsIgnoredColumn(column) {
			if colNum > 0 {
				d.writef(w, ",")
			}

			d.writef(w, "%s = VALUES(%s)", column, column)

			colNum++
		}
	}
}

func (d *dumper) writef(w io.Writer, format string, args ...any) {
	if d.writeErr != nil {
		return
	}

	_, err := fmt.Fprintf(w, format, args...)
	if err != nil {
		d.writeErr = err
	}
}

func (d *dumper) printDump(ctx context.Context) error {
	if d.disableFKChecks {
		d.writef(d.dumpTarget, "# Disable FK checks because references cycle detected\nSET FOREIGN_KEY_CHECKS = 0;\n\n")
	}

	if !config.Config.Dump.OnlyData {
		for tableName := range config.Config.Tables {
			err := d.printTableCreate(ctx, tableName, d.dumpTarget)
			if err != nil {
				return err
			}
		}
	}

	for _, tableName := range d.tablesInsertOrder {
		if reader := d.tableInsertsBuffers[tableName]; reader != nil {
			_, _ = io.Copy(d.dumpTarget, reader)
		}

		d.writef(d.dumpTarget, "\n\n")

		delete(d.tableInsertsBuffers, tableName)
	}

	return nil
}

func (d *dumper) getTableConfig(name string) (config.TableConfig, bool) {
	tableConf, ok := config.Config.Tables[name]

	return tableConf, ok
}

func (d *dumper) printTableCreate(ctx context.Context, tableName string, w io.Writer) error {
	table, err := schema.GetTable(ctx, tableName)
	if err != nil {
		return err
	}

	createStatement, err := table.CreateTableStatement(ctx)
	if err != nil {
		return err
	}

	buf := bytes.NewBuffer(nil)

	var deleted bool

	for i, st := range strings.Split(createStatement, "\n") {
		if strings.HasPrefix(st, ")") { // End of create statement
			if deleted {
				// Remove trailing comma
				buf.Truncate(buf.Len() - 1)
			}

			d.writef(buf, "\n")
			d.writef(buf, st)
			d.writef(buf, ";\n\n")

			break
		}

		if !d.isFKDefinitionForNotIncludedTable(st) {
			if i > 0 {
				d.writef(buf, "\n")
			}

			d.writef(buf, st)

			deleted = false
		} else {
			deleted = true
		}
	}

	_, err = io.Copy(w, buf)

	return err
}

func (d *dumper) isFKDefinitionForNotIncludedTable(st string) bool {
	re := regexp.MustCompile("REFERENCES `([^`]+)`")
	match := re.FindStringSubmatch(st)

	if len(match) < 2 {
		return false
	}

	_, ok := config.Config.Tables[match[1]]

	return !ok
}
