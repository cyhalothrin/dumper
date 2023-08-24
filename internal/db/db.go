package db

import (
	"context"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"

	"github.com/cyhalothrin/dumper/internal/config"
)

var SourceDB *sqlx.DB

func Connect(ctx context.Context) (err error) {
	SourceDB, err = sqlx.Connect(config.Config.SourceDB.Driver, config.Config.SourceDB.DSN)
	if err != nil {
		return err
	}

	if err = SourceDB.PingContext(ctx); err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}

	return nil
}
