package schema

import "slices"

type PrimaryKey struct {
	table   *Table
	Columns []string
}

func (k PrimaryKey) FormatFromRecord(record map[string]any) string {
	values := make([]any, len(k.Columns))

	for i, colName := range k.Columns {
		values[i] = record[colName]
	}

	return k.Format(values)
}

func (k PrimaryKey) Format(values []any) string {
	var pkVal string

	for i, val := range values {
		if i > 0 {
			pkVal += ","
		}

		pkVal += k.table.Column(k.Columns[i]).Format(val)
	}

	return pkVal
}

func (k PrimaryKey) Contains(name string) bool {
	return slices.Contains(k.Columns, name)
}
