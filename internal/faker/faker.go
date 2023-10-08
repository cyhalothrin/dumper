package faker

import (
	"github.com/brianvoe/gofakeit/v6"

	"github.com/cyhalothrin/dumper/internal/config"
)

func Format(conf *config.FakerConfig) string {
	return gofakeit.Generate(conf.Value)
}
