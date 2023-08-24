package schema

import (
	"fmt"
	"strings"
)

type Column struct {
	Name     string
	Nullable bool
	Type     string
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
	return strings.HasPrefix(c.Type, "int") || strings.HasPrefix(c.Type, "tinyint")
}
