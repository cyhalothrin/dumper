package schema

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
)

type Column struct {
	Name     string
	Nullable bool
	dbType   string
	Default  sql.NullString
}

func (c Column) Format(val any) string {
	if val == nil {
		if c.Nullable {
			return "NULL"
		}

		panic(fmt.Sprintf("column %s is not nullable", c.Name))
	}

	switch t := val.(type) {
	case []uint8:
		return c.formatBytes(t)
	case string:
		return c.formatBytes([]byte(t))
	case int64:
		return strconv.FormatInt(t, 10)
	case float32:
		return strconv.FormatFloat(float64(t), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	}

	panic(fmt.Sprintf("unsupported type %T", val))
}

func (c Column) formatBytes(val []uint8) string {
	switch {
	case c.isNumeric():
		return string(val)
	default:
		return fmt.Sprintf("'%s'", string(val))
	}
}

func (c Column) DefaultValue() string {
	if c.Default.Valid {
		return c.formatBytes([]uint8(c.Default.String))
	}

	if c.Nullable {
		return "NULL"
	}

	if c.isNumeric() {
		return "0"
	}

	return "''"
}

// int(11)
// varchar(255)
// tinyint(1)
// datetime
// timestamp

func (c Column) isNumeric() bool {
	return strings.HasPrefix(c.dbType, "int") || strings.HasPrefix(c.dbType, "tinyint")
}
