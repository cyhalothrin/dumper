package config

import "slices"

var Config *DumperConfig

func init() {
	Config = &DumperConfig{}
}

type DumperConfig struct {
	SourceDB   DBConfig               `mapstructure:"source_db"`
	TablesList []TableConfig          `mapstructure:"tables"`
	Tables     map[string]TableConfig `mapstructure:"-"`
	DumpFile   string                 `mapstructure:"dump_file"`
}

func Normalize() {
	Config.Tables = make(map[string]TableConfig)

	for _, tableConf := range Config.TablesList {
		tableConf.ColumnsFaker = make(map[string]*FakerConfig)

		for _, fakerConf := range tableConf.Faker {
			for _, colName := range fakerConf.Columns {
				tableConf.ColumnsFaker[colName] = fakerConf
			}
		}

		Config.Tables[tableConf.Name] = tableConf
	}

	Config.TablesList = nil
}

type DBConfig struct {
	Driver string `mapstructure:"driver"`
	DSN    string `mapstructure:"dsn"`
}

type FakerConfig struct {
	Columns  []string `mapstructure:"columns"`
	Type     string   `mapstructure:"type"`
	Pattern  string   `mapstructure:"pattern"`
	NamePart string   `mapstructure:"part"`
	Length   int      `mapstructure:"length"`
}

type TableConfig struct {
	Name string `mapstructure:"name"`
	// SelectQuery в выборке нужны только id записей, которые нужно перенести
	SelectQuery       string                  `mapstructure:"select_query"`
	Limit             int                     `mapstructure:"limit"`
	SelectColumnsMode SelectColumnsMode       `mapstructure:"select_mode"`
	AllowColumns      []string                `mapstructure:"allow_columns"`
	IgnoreColumns     []string                `mapstructure:"ignore_columns"`
	Faker             []*FakerConfig          `mapstructure:"faker"`
	ColumnsFaker      map[string]*FakerConfig `mapstructure:"-"`
}

func (c TableConfig) IsIgnoredColumn(column string) bool {
	return slices.Contains(c.IgnoreColumns, column)
}

func (c TableConfig) UseFaker(column string) *FakerConfig {
	return c.ColumnsFaker[column]
}

type SelectColumnsMode string

const (
	SelectColumnsModeAll     SelectColumnsMode = "all"
	SelectColumnsModeNotNull SelectColumnsMode = "not_null"
)
