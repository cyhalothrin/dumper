package schema

type ForeignKey struct {
	Columns             []string
	ReferencedTableName string
	ReferencedColumns   []string
}

//func (fk ForeignKey) Table() *Table {
//	if fk.table == nil {
//		fk.table = GetTable(fk.tableName)
//	}
//
//	return fk.table
//}
//
//func (fk ForeignKey) Column(ctx context.Context) (Column, error) {
//	var err error
//
//	if fk.column.Name == "" {
//		fk.column, err = fk.Table().Column(ctx, fk.columnName)
//	}
//
//	return fk.column, err
//}
