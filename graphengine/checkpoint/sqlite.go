package checkpoint

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/infiniflow/ragflow/harness/graphengine/constants"
	_ "github.com/mattn/go-sqlite3"
)

// SqliteSaver is a SQLite-based checkpoint saver.
type SqliteSaver struct {
	db     *sql.DB
	conn   *sql.Conn
	isAsync bool
}

// NewSqliteSaver creates a new SQLite checkpoint saver.
func NewSqliteSaver(dbPath string) (*SqliteSaver, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite database: %w", err)
	}
	
	saver := &SqliteSaver{
		db:      db,
		isAsync: false,
	}
	
	if err := saver.setup(); err != nil {
		return nil, err
	}
	
	return saver, nil
}

// NewSqliteSaverWithConnection creates a new SQLite checkpoint saver with an existing connection.
func NewSqliteSaverWithConnection(conn *sql.Conn) (*SqliteSaver, error) {
	saver := &SqliteSaver{
		conn:    conn,
		isAsync: true,
	}
	
	if err := saver.setup(); err != nil {
		return nil, err
	}
	
	return saver, nil
}

// setup creates the necessary tables.
func (s *SqliteSaver) setup() error {
	query := `
	CREATE TABLE IF NOT EXISTS checkpoints (
		id TEXT PRIMARY KEY,
		thread_id TEXT NOT NULL,
		parent_id TEXT,
		checkpoint BLOB NOT NULL,
		metadata BLOB,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);
	
	CREATE INDEX IF NOT EXISTS idx_thread_id ON checkpoints(thread_id);
	CREATE INDEX IF NOT EXISTS idx_parent_id ON checkpoints(parent_id);
	`
	
	if s.db != nil {
		_, err := s.db.Exec(query)
		if err != nil {
			return fmt.Errorf("failed to create tables: %w", err)
		}
	} else if s.conn != nil {
		_, err := s.conn.ExecContext(context.Background(), query)
		if err != nil {
			return fmt.Errorf("failed to create tables: %w", err)
		}
	}
	
	return nil
}

// Get retrieves the latest checkpoint for a thread.
func (s *SqliteSaver) Get(ctx context.Context, config map[string]interface{}) (map[string]interface{}, error) {
	threadID, ok := config[constants.ConfigKeyThreadID].(string)
	if !ok {
		return nil, fmt.Errorf("thread_id is required")
	}
	
	var query string
	var args []interface{}
	
	if checkpointID, ok := config[constants.ConfigKeyCheckpointID].(string); ok {
		query = `SELECT checkpoint FROM checkpoints WHERE id = ? AND thread_id = ?`
		args = []interface{}{checkpointID, threadID}
	} else {
		query = `SELECT checkpoint FROM checkpoints WHERE thread_id = ? ORDER BY created_at DESC LIMIT 1`
		args = []interface{}{threadID}
	}
	
	var checkpointData []byte
	var err error
	
	if s.db != nil {
		err = s.db.QueryRowContext(ctx, query, args...).Scan(&checkpointData)
	} else {
		err = s.conn.QueryRowContext(ctx, query, args...).Scan(&checkpointData)
	}
	
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get checkpoint: %w", err)
	}
	
	var checkpoint map[string]interface{}
	if err := json.Unmarshal(checkpointData, &checkpoint); err != nil {
		return nil, fmt.Errorf("failed to unmarshal checkpoint: %w", err)
	}
	
	return checkpoint, nil
}

// Put saves a new checkpoint.
func (s *SqliteSaver) Put(ctx context.Context, config map[string]interface{}, checkpoint map[string]interface{}) error {
	threadID, ok := config[constants.ConfigKeyThreadID].(string)
	if !ok {
		return fmt.Errorf("thread_id is required")
	}
	
	checkpointID := uuid.New().String()
	if id, ok := config[constants.ConfigKeyCheckpointID].(string); ok {
		checkpointID = id
	}
	
	checkpointData, err := json.Marshal(checkpoint)
	if err != nil {
		return fmt.Errorf("failed to marshal checkpoint: %w", err)
	}
	
	metadataData, err := json.Marshal(config)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}
	
	var parentID interface{}
	if pid, ok := config["parent_checkpoint_id"].(string); ok {
		parentID = pid
	}
	
	query := `INSERT INTO checkpoints (id, thread_id, parent_id, checkpoint, metadata) VALUES (?, ?, ?, ?, ?)`
	
	if s.db != nil {
		_, err = s.db.ExecContext(ctx, query, checkpointID, threadID, parentID, checkpointData, metadataData)
	} else {
		_, err = s.conn.ExecContext(ctx, query, checkpointID, threadID, parentID, checkpointData, metadataData)
	}
	
	if err != nil {
		return fmt.Errorf("failed to save checkpoint: %w", err)
	}
	
	return nil
}

// List lists checkpoints for a thread.
func (s *SqliteSaver) List(ctx context.Context, config map[string]interface{}, limit int) ([]map[string]interface{}, error) {
	threadID, ok := config[constants.ConfigKeyThreadID].(string)
	if !ok {
		return nil, fmt.Errorf("thread_id is required")
	}
	
	if limit <= 0 {
		limit = 10
	}
	
	query := `SELECT id, metadata, created_at, parent_id FROM checkpoints WHERE thread_id = ? ORDER BY created_at DESC LIMIT ?`
	
	var rows *sql.Rows
	var err error
	
	if s.db != nil {
		rows, err = s.db.QueryContext(ctx, query, threadID, limit)
	} else {
		rows, err = s.conn.QueryContext(ctx, query, threadID, limit)
	}
	
	if err != nil {
		return nil, fmt.Errorf("failed to list checkpoints: %w", err)
	}
	defer rows.Close()
	
	result := make([]map[string]interface{}, 0)
	
	for rows.Next() {
		var id string
		var metadataData []byte
		var createdAt time.Time
		var parentID sql.NullString
		
		if err := rows.Scan(&id, &metadataData, &createdAt, &parentID); err != nil {
			return nil, fmt.Errorf("failed to scan checkpoint: %w", err)
		}
		
		var metadata map[string]interface{}
		if err := json.Unmarshal(metadataData, &metadata); err != nil {
			metadata = make(map[string]interface{})
		}
		
		entry := map[string]interface{}{
			"checkpoint_id": id,
			"thread_id":     threadID,
			"metadata":      metadata,
			"created_at":    createdAt,
		}
		
		if parentID.Valid {
			entry["parent_id"] = parentID.String
		}
		
		result = append(result, entry)
	}
	
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating checkpoints: %w", err)
	}
	
	return result, nil
}

// Close closes the database connection.
func (s *SqliteSaver) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}
