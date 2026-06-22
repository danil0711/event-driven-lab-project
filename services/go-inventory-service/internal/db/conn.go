package db

import (
	"context"
	"inventory/internal/config"

	"github.com/jackc/pgx/v5/pgxpool"
)

func New(ctx context.Context, cfg config.Config) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, cfg.PostgresDSN())
	if err != nil {
		return nil, err
	}

	return pool, nil
}
