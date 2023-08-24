package internal

import (
	"context"
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"

	"github.com/cyhalothrin/dumper/internal/config"
	"github.com/cyhalothrin/dumper/internal/db"
	"github.com/cyhalothrin/dumper/internal/query"
	"github.com/cyhalothrin/dumper/internal/schema"
)

func Init(ctx context.Context) error {
	return db.Connect(ctx)
}

func Dump(ctx context.Context) error {
	d := &dumper{}

	return d.do(ctx)
}

type dumper struct {
	sourceDB *sqlx.DB
}

func (d *dumper) do(ctx context.Context) (err error) {
	if err = d.dumpTables(ctx); err != nil {
		return err
	}

	return nil
}

func (d *dumper) dumpTables(ctx context.Context) error {
	for _, tableConf := range config.Config.Tables {
		if err := d.dumpTable(ctx, tableConf); err != nil {
			return err
		}
	}

	return nil
}

func (d *dumper) dumpTable(ctx context.Context, tableConf config.TableConfig) error {
	selectQuery := query.Select(tableConf.Name).InSubQuery(tableConf.SelectQuery).Limit(tableConf.Limit)
	table := schema.GetTable(tableConf.Name)

	if tableConf.SelectColumnsMode == config.SelectColumnsModeNotNull {
		columns, err := table.GetNotNullColumns(ctx)
		if err != nil {
			return err
		}

		selectQuery.Columns(columns)
	}

	columns, records, err := selectQuery.Exec(ctx)
	if err != nil {
		return err
	}

	if len(records) == 0 {
		return nil
	}

	d.printInsertStatement(table, columns, records)

	return nil
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
}
