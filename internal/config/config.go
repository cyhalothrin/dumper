package config

var Config *DumperConfig

func init() {
	Config = &DumperConfig{}
}

type DumperConfig struct {
	SourceDB DBConfig      `mapstructure:"source_db"`
	Tables   []TableConfig `mapstructure:"tables"`
	DumpFile string        `mapstructure:"dump_file"`
}

type DBConfig struct {
	Driver string `mapstructure:"driver"`
	DSN    string `mapstructure:"dsn"`
}

type TableConfig struct {
	Name string `mapstructure:"name"`
	// SelectQuery в выборке нужны только id записей, которые нужно перенести
	SelectQuery       string            `mapstructure:"select_query"`
	Limit             int               `mapstructure:"limit"`
	SelectColumnsMode SelectColumnsMode `mapstructure:"select_mode"`
	// AllowColumns      []string          `mapstructure:"allow_columns"`
	// IgnoreColumns     []string          `mapstructure:"ignore_columns"`
}

type SelectColumnsMode string

const (
	SelectColumnsModeAll     SelectColumnsMode = "all"
	SelectColumnsModeNotNull SelectColumnsMode = "not_null"
)
