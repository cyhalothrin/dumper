package schema

type PrimaryKey struct {
	table   *Table
	Columns []string
}

func (k PrimaryKey) Format(record map[string]any) (string, error) {
	var pkVal string

	for i, colName := range k.Columns {
		if i > 0 {
			pkVal += ","
		}

		pkVal += k.table.Column(colName).Format(record[colName])
	}

	return pkVal, nil
}
