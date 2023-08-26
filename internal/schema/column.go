package schema

import (
	"fmt"
	"strconv"
	"strings"
)

type Column struct {
	Name     string
	Nullable bool
	dbType   string
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
	case c.isInt():
		return string(val)
	default:
		return fmt.Sprintf("'%s'", string(val))
	}
}

// int(11)
// varchar(255)
// tinyint(1)
// datetime
// timestamp

func (c Column) isInt() bool {
	return strings.HasPrefix(c.dbType, "int") || strings.HasPrefix(c.dbType, "tinyint")
}
