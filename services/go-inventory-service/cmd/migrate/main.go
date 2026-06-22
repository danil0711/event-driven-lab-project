package main

import (
	"inventory/internal/config"
	"log"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/joho/godotenv"
)

func main() {

	err := godotenv.Load()
	if err != nil {
		log.Println(".env not found, using system env")
	}

	cfg, err := config.Load()
	if err != nil {
		log.Fatal(err)
	}

	m, err := migrate.New(
		"file://migrations",
		cfg.PostgresDSN(),
	)
	if err != nil {
		log.Fatal(err)
	}

	if err := m.Up(); err != nil && err.Error() != "no change" {
		log.Fatal(err)
	}
}
