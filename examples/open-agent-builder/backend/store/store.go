// Package store provides MySQL-based persistence for LLM configuration.
package store

import (
	"database/sql"
	"encoding/json"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/infiniflow/ragflow/harness/examples/open-agent-builder/engine"
)

// Store persists configuration to MySQL.
type Store struct {
	db *sql.DB
}

// New opens a MySQL connection and ensures the config table exists.
func New(dsn string) (*Store, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("mysql open: %w", err)
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("mysql ping: %w", err)
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS app_config (
		id VARCHAR(64) PRIMARY KEY,
		config_json JSON NOT NULL,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
	)`); err != nil {
		return nil, fmt.Errorf("create table: %w", err)
	}
	return &Store{db: db}, nil
}

// SaveModels stores the full model list (with API keys) to MySQL.
func (s *Store) SaveModels(models []engine.ModelConfig) error {
	b, err := json.Marshal(models)
	if err != nil {
		return fmt.Errorf("marshal models: %w", err)
	}
	_, err = s.db.Exec(
		`INSERT INTO app_config (id, config_json) VALUES ('models', ?)
		 ON DUPLICATE KEY UPDATE config_json = VALUES(config_json)`,
		string(b),
	)
	return err
}

// LoadModels retrieves the model list from MySQL (may be nil).
func (s *Store) LoadModels() ([]engine.ModelConfig, error) {
	var configJSON string
	err := s.db.QueryRow(`SELECT config_json FROM app_config WHERE id = 'models'`).Scan(&configJSON)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query models: %w", err)
	}
	var models []engine.ModelConfig
	if err := json.Unmarshal([]byte(configJSON), &models); err != nil {
		return nil, fmt.Errorf("unmarshal models: %w", err)
	}
	return models, nil
}

// Close closes the database connection.
func (s *Store) Close() error {
	return s.db.Close()
}
