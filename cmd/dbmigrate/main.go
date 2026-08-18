package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"github.com/tandemn-labs/chunk-manager/db/migrations"
)

func main() {
	if len(os.Args) != 2 {
		fail("usage: dbmigrate up|down|status|version")
	}
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		fail("DATABASE_URL is required")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	database, err := sql.Open("pgx", databaseURL)
	if err != nil {
		fail("open PostgreSQL: %v", err)
	}
	defer database.Close()
	if err := database.PingContext(ctx); err != nil {
		fail("connect to PostgreSQL: %v", err)
	}

	goose.SetBaseFS(migrations.Files)
	if err := goose.SetDialect("postgres"); err != nil {
		fail("set migration dialect: %v", err)
	}

	switch os.Args[1] {
	case "up":
		err = goose.UpContext(ctx, database, ".")
	case "down":
		err = goose.DownContext(ctx, database, ".")
	case "status":
		err = goose.StatusContext(ctx, database, ".")
	case "version":
		var version int64
		version, err = goose.GetDBVersionContext(ctx, database)
		if err == nil {
			fmt.Println(version)
		}
	default:
		fail("unknown migration command %q", os.Args[1])
	}
	if err != nil {
		fail("migration %s: %v", os.Args[1], err)
	}
}

func fail(format string, values ...any) {
	_, _ = fmt.Fprintf(os.Stderr, format+"\n", values...)
	os.Exit(1)
}
