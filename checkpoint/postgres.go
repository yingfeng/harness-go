// Package checkpoint provides PostgreSQL-based checkpoint management.
package checkpoint

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/infiniflow/ragflow/harness/types"
)

// PostgresSaver is a PostgreSQL-based checkpoint saver.
type PostgresSaver struct {
	pool   *pgxpool.Pool
	config *pgxpool.Config
	// Connection string for reconnection
	connString string
	// Serializer for complex types
	serializer Serializer
}

// Serializer defines the interface for serializing/deserializing complex types.
type Serializer interface {
	Serialize(value interface{}) ([]byte, error)
	Deserialize(data []byte, target interface{}) error
}

// JSONSerializer is the default JSON serializer.
type JSONSerializer struct{}

// Serialize implements Serializer.
func (JSONSerializer) Serialize(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}

// Deserialize implements Serializer.
func (JSONSerializer) Deserialize(data []byte, target interface{}) error {
	return json.Unmarshal(data, target)
}

// PostgresConfig holds configuration for PostgreSQL connection.
type PostgresConfig struct {
	Host            string
	Port            int
	User            string
	Password        string
	Database        string
	SSLMode         string
	MaxConnections  int
	MinConnections  int
	MaxConnIdleTime time.Duration
	MaxConnLifetime time.Duration
}

// NewPostgresSaver creates a new PostgreSQL checkpoint saver.
func NewPostgresSaver(connString string) (*PostgresSaver, error) {
	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	saver := &PostgresSaver{
		pool:       pool,
		config:     config,
		connString: connString,
		serializer: JSONSerializer{},
	}

	if err := saver.setup(); err != nil {
		pool.Close()
		return nil, err
	}

	return saver, nil
}

// NewPostgresSaverWithConfig creates a new PostgreSQL checkpoint saver with explicit config.
func NewPostgresSaverWithConfig(config *PostgresConfig) (*PostgresSaver, error) {
	if config == nil {
		return nil, fmt.Errorf("config is required")
	}

	sslMode := "disable"
	if config.SSLMode != "" {
		sslMode = config.SSLMode
	}

	connString := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		config.Host, config.Port, config.User, config.Password, config.Database, sslMode,
	)

	return NewPostgresSaver(connString)
}

// setup creates the necessary tables and runs migrations.
func (s *PostgresSaver) setup() error {
	ctx := context.Background()
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	// Create tables if they don't exist
	_, err = conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS checkpoints (
			id UUID PRIMARY KEY,
			thread_id TEXT NOT NULL,
			checkpoint_ns TEXT NOT NULL DEFAULT '',
			parent_checkpoint_id UUID,
			checkpoint JSONB NOT NULL,
			metadata JSONB,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			version INTEGER NOT NULL DEFAULT 0,
			channel_versions JSONB,
			versions_seen JSONB,
			updated_channels JSONB,
			channel_values JSONB,
			pending_writes JSONB,
			PRIMARY KEY (thread_id, checkpoint_ns, id)
		);

		CREATE INDEX IF NOT EXISTS idx_checkpoints_thread_id 
			ON checkpoints(thread_id);
		CREATE INDEX IF NOT EXISTS idx_checkpoints_checkpoint_ns 
			ON checkpoints(checkpoint_ns);
		CREATE INDEX IF NOT EXISTS idx_checkpoints_parent_id 
			ON checkpoints(parent_checkpoint_id);
		CREATE INDEX IF NOT EXISTS idx_checkpoints_created_at 
			ON checkpoints(created_at DESC);

		CREATE TABLE IF NOT EXISTS checkpoint_blobs (
			id UUID PRIMARY KEY,
			thread_id TEXT NOT NULL,
			checkpoint_ns TEXT NOT NULL DEFAULT '',
			channel_name TEXT NOT NULL,
			value BYTEA NOT NULL,
			version INTEGER NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (thread_id, checkpoint_ns, channel_name, version)
		);

		CREATE TABLE IF NOT EXISTS checkpoint_writes (
			id UUID PRIMARY KEY,
			thread_id TEXT NOT NULL,
			checkpoint_ns TEXT NOT NULL DEFAULT '',
			checkpoint_id UUID NOT NULL,
			task_id TEXT NOT NULL,
			task_path TEXT NOT NULL DEFAULT '',
			channel_name TEXT NOT NULL,
			value BYTEA NOT NULL,
			overwrite BOOLEAN NOT NULL DEFAULT FALSE,
			node_name TEXT,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id, task_id, channel_name)
		);

		CREATE TABLE IF NOT EXISTS checkpoint_migrations (
			v INTEGER PRIMARY KEY
		);
	`)
	if err != nil {
		return fmt.Errorf("failed to create tables: %w", err)
	}

	// Check if migration table exists and run migrations
	_, err = conn.Exec(ctx, `
		INSERT INTO checkpoint_migrations (v)
		SELECT 0
		WHERE NOT EXISTS (SELECT 1 FROM checkpoint_migrations WHERE v = 0);
	`)
	if err != nil {
		// Table might not exist yet, ignore
	}

	return nil
}

// Get retrieves the latest checkpoint for a thread.
func (s *PostgresSaver) Get(ctx context.Context, config map[string]interface{}) (map[string]interface{}, error) {
	threadID, ok := config["thread_id"].(string)
	if !ok {
		return nil, fmt.Errorf("thread_id is required")
	}

	checkpointNS := ""
	if ns, ok := config["checkpoint_ns"].(string); ok {
		checkpointNS = ns
	}

	query := `
		SELECT checkpoint, metadata, parent_checkpoint_id
		FROM checkpoints
		WHERE thread_id = $1 AND checkpoint_ns = $2
		ORDER BY created_at DESC
		LIMIT 1
	`

	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	var checkpointData []byte
	var metadataData []byte
	var parentID sql.NullString

	err = conn.QueryRow(ctx, query, threadID, checkpointNS).Scan(
		&checkpointData, &metadataData, &parentID,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to query checkpoint: %w", err)
	}

	var checkpoint map[string]interface{}
	if err := json.Unmarshal(checkpointData, &checkpoint); err != nil {
		return nil, fmt.Errorf("failed to unmarshal checkpoint: %w", err)
	}

	// Load blob values
	blobs, err := s.loadBlobs(ctx, threadID, checkpointNS)
	if err != nil {
		return nil, fmt.Errorf("failed to load blob values: %w", err)
	}

	if checkpoint["channel_values"] == nil {
		checkpoint["channel_values"] = make(map[string]interface{})
	}
	channelValues := checkpoint["channel_values"].(map[string]interface{})
	for k, v := range blobs {
		channelValues[k] = v
	}

	// Add metadata if needed
	if len(metadataData) > 0 {
		var metadata map[string]interface{}
		if err := json.Unmarshal(metadataData, &metadata); err == nil {
			checkpoint["metadata"] = metadata
		}
	}

	return checkpoint, nil
}

// Put saves a new checkpoint.
func (s *PostgresSaver) Put(ctx context.Context, config map[string]interface{}, checkpoint map[string]interface{}) error {
	threadID, ok := config["thread_id"].(string)
	if !ok {
		return fmt.Errorf("thread_id is required")
	}

	checkpointNS := ""
	if ns, ok := config["checkpoint_ns"].(string); ok {
		checkpointNS = ns
	}

	checkpointID := uuid.New().String()
	if id, ok := config["checkpoint_id"].(string); ok {
		checkpointID = id
	}

	parentID := ""
	if pid, ok := config["parent_checkpoint_id"].(string); ok {
		parentID = pid
	}

	// Separate primitive values from blobs
	primitiveValues := make(map[string]interface{})
	blobValues := make(map[string]interface{})

	if channelValues, ok := checkpoint["channel_values"].(map[string]interface{}); ok {
		for k, v := range channelValues {
			if v == nil || isPrimitive(v) {
				primitiveValues[k] = v
			} else {
				blobValues[k] = v
			}
		}
	}

	// Create checkpoint copy with only primitive values
	checkpointCopy := make(map[string]interface{})
	for k, v := range checkpoint {
		checkpointCopy[k] = v
	}
	checkpointCopy["channel_values"] = primitiveValues

	checkpointData, err := json.Marshal(checkpointCopy)
	if err != nil {
		return fmt.Errorf("failed to marshal checkpoint: %w", err)
	}

	metadataData, err := json.Marshal(config)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	// Start transaction
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Insert checkpoint
	_, err = tx.Exec(ctx, `
		INSERT INTO checkpoints (
			id, thread_id, checkpoint_ns, parent_checkpoint_id,
			checkpoint, metadata, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, NOW())
		ON CONFLICT (thread_id, checkpoint_ns, id) DO UPDATE SET
			checkpoint = EXCLUDED.checkpoint,
			metadata = EXCLUDED.metadata,
			parent_checkpoint_id = EXCLUDED.parent_checkpoint_id
	`,
		checkpointID, threadID, checkpointNS, parentID,
		checkpointData, metadataData,
	)
	if err != nil {
		return fmt.Errorf("failed to insert checkpoint: %w", err)
	}

	// Save blob values
	if len(blobValues) > 0 {
		if err := s.saveBlobs(ctx, tx, threadID, checkpointNS, blobValues); err != nil {
			return fmt.Errorf("failed to save blob values: %w", err)
		}
	}

	return tx.Commit(ctx)
}

// List lists checkpoints for a thread.
func (s *PostgresSaver) List(ctx context.Context, config map[string]interface{}, limit int) ([]map[string]interface{}, error) {
	threadID, ok := config["thread_id"].(string)
	if !ok {
		return nil, fmt.Errorf("thread_id is required")
	}

	checkpointNS := ""
	if ns, ok := config["checkpoint_ns"].(string); ok {
		checkpointNS = ns
	}

	if limit <= 0 {
		limit = 10
	}

	query := `
		SELECT 
			id, checkpoint, metadata, parent_checkpoint_id, created_at
		FROM checkpoints
		WHERE thread_id = $1 AND checkpoint_ns = $2
		ORDER BY created_at DESC
		LIMIT $3
	`

	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	rows, err := conn.Query(ctx, query, threadID, checkpointNS, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query checkpoints: %w", err)
	}
	defer rows.Close()

	result := make([]map[string]interface{}, 0)
	for rows.Next() {
		var id string
		var checkpointData []byte
		var metadataData []byte
		var parentID sql.NullString
		var createdAt time.Time

		if err := rows.Scan(&id, &checkpointData, &metadataData, &parentID, &createdAt); err != nil {
			return nil, fmt.Errorf("failed to scan checkpoint: %w", err)
		}

		var checkpoint map[string]interface{}
		if err := json.Unmarshal(checkpointData, &checkpoint); err != nil {
			return nil, fmt.Errorf("failed to unmarshal checkpoint: %w", err)
		}

		// Load blob values
		blobs, err := s.loadBlobs(ctx, threadID, checkpointNS)
		if err != nil {
			return nil, fmt.Errorf("failed to load blob values: %w", err)
		}

		if checkpoint["channel_values"] == nil {
			checkpoint["channel_values"] = make(map[string]interface{})
		}
		channelValues := checkpoint["channel_values"].(map[string]interface{})
		for k, v := range blobs {
			channelValues[k] = v
		}

		entry := map[string]interface{}{
			"checkpoint_id": id,
			"thread_id":     threadID,
			"checkpoint":    checkpoint,
			"created_at":    createdAt,
		}

		if parentID.Valid {
			entry["parent_id"] = parentID.String
		}

		if len(metadataData) > 0 {
			var metadata map[string]interface{}
			if err := json.Unmarshal(metadataData, &metadata); err == nil {
				entry["metadata"] = metadata
			}
		}

		result = append(result, entry)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating checkpoints: %w", err)
	}

	return result, nil
}

// PutWrites stores pending writes for a checkpoint.
func (s *PostgresSaver) PutWrites(ctx context.Context, config map[string]interface{}, writes []PendingWrite, taskID string) error {
	threadID, ok := config["thread_id"].(string)
	if !ok {
		return fmt.Errorf("thread_id is required")
	}

	checkpointNS := ""
	if ns, ok := config["checkpoint_ns"].(string); ok {
		checkpointNS = ns
	}

	checkpointID := ""
	if id, ok := config["checkpoint_id"].(string); ok {
		checkpointID = id
	}
	if checkpointID == "" {
		return fmt.Errorf("checkpoint_id is required for PutWrites")
	}

	taskPath := ""
	if path, ok := config["task_path"].(string); ok {
		taskPath = path
	}

	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	// Start transaction
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	for _, write := range writes {
		valueData, err := s.serializer.Serialize(write.Value)
		if err != nil {
			return fmt.Errorf("failed to serialize write value: %w", err)
		}

		_, err = tx.Exec(ctx, `
			INSERT INTO checkpoint_writes (
				id, thread_id, checkpoint_ns, checkpoint_id,
				task_id, task_path, channel_name, value,
				overwrite, node_name, created_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW())
			ON CONFLICT (thread_id, checkpoint_ns, checkpoint_id, task_id, channel_name) 
			DO UPDATE SET
				value = EXCLUDED.value,
				overwrite = EXCLUDED.overwrite,
				node_name = EXCLUDED.node_name
		`,
			uuid.New().String(), threadID, checkpointNS, checkpointID,
			taskID, taskPath, write.Channel, valueData,
			write.Overwrite, write.Node,
		)
		if err != nil {
			return fmt.Errorf("failed to insert write: %w", err)
		}
	}

	return tx.Commit(ctx)
}

// GetTuple loads a checkpoint and its parent as a tuple.
func (s *PostgresSaver) GetTuple(ctx context.Context, config *types.RunnableConfig) (*CheckpointTuple, error) {
	if config == nil {
		config = types.NewRunnableConfig()
	}

	threadID := config.ThreadID
	if threadID == "" {
		return nil, fmt.Errorf("thread_id is required")
	}

	checkpointIDInterface, _ := config.Get("checkpoint_id")
	checkpointID, _ := checkpointIDInterface.(string)
	checkpointNS := ""
	if nsInterface, ok := config.Get("checkpoint_ns"); ok {
		if ns, ok := nsInterface.(string); ok {
			checkpointNS = ns
		}
	}

	var query string
	var args []interface{}

	if checkpointID != "" {
		query = `
			SELECT 
				checkpoint, metadata, parent_checkpoint_id,
				created_at, version, channel_versions, versions_seen,
				updated_channels, channel_values, pending_writes
			FROM checkpoints
			WHERE thread_id = $1 AND checkpoint_ns = $2 AND id = $3
		`
		args = []interface{}{threadID, checkpointNS, checkpointID}
	} else {
		query = `
			SELECT 
				checkpoint, metadata, parent_checkpoint_id,
				created_at, version, channel_versions, versions_seen,
				updated_channels, channel_values, pending_writes
			FROM checkpoints
			WHERE thread_id = $1 AND checkpoint_ns = $2
			ORDER BY created_at DESC
			LIMIT 1
		`
		args = []interface{}{threadID, checkpointNS}
	}

	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	var checkpointData []byte
	var metadataData []byte
	var parentID sql.NullString
	var createdAt time.Time
	var version int
	var channelVersionsData []byte
	var versionsSeenData []byte
	var updatedChannelsData []byte
	var channelValuesData []byte
	var pendingWritesData []byte

	err = conn.QueryRow(ctx, query, args...).Scan(
		&checkpointData, &metadataData, &parentID,
		&createdAt, &version, &channelVersionsData,
		&versionsSeenData, &updatedChannelsData,
		&channelValuesData, &pendingWritesData,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return NewCheckpointTuple(config, nil, nil), nil
		}
		return nil, fmt.Errorf("failed to query checkpoint: %w", err)
	}

	// Parse checkpoint
	var checkpointDataMap map[string]interface{}
	if err := json.Unmarshal(checkpointData, &checkpointDataMap); err != nil {
		return nil, fmt.Errorf("failed to unmarshal checkpoint: %w", err)
	}

	// Load blob values
	blobs, err := s.loadBlobs(ctx, threadID, checkpointNS)
	if err != nil {
		return nil, fmt.Errorf("failed to load blob values: %w", err)
	}

	if checkpointDataMap["channel_values"] == nil {
		checkpointDataMap["channel_values"] = make(map[string]interface{})
	}
	channelValues := checkpointDataMap["channel_values"].(map[string]interface{})
	for k, v := range blobs {
		channelValues[k] = v
	}

	// Convert map to Checkpoint struct
	checkpoint, err := FromMap(checkpointDataMap)
	if err != nil {
		return nil, fmt.Errorf("failed to convert checkpoint data: %w", err)
	}

	// Create parent config if parent exists
	var parentConfig *types.RunnableConfig
	if parentID.Valid {
		parentConfig = types.NewRunnableConfig()
		parentConfig.ThreadID = threadID
		parentConfig.Set("checkpoint_id", parentID.String)
		parentConfig.Set("checkpoint_ns", checkpointNS)
	}

	return NewCheckpointTuple(config, checkpoint, parentConfig), nil
}

// DeleteThread deletes all checkpoints for a thread.
func (s *PostgresSaver) DeleteThread(ctx context.Context, threadID string) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, "DELETE FROM checkpoints WHERE thread_id = $1", threadID)
	if err != nil {
		return fmt.Errorf("failed to delete checkpoints: %w", err)
	}

	_, err = tx.Exec(ctx, "DELETE FROM checkpoint_blobs WHERE thread_id = $1", threadID)
	if err != nil {
		return fmt.Errorf("failed to delete checkpoint blobs: %w", err)
	}

	_, err = tx.Exec(ctx, "DELETE FROM checkpoint_writes WHERE thread_id = $1", threadID)
	if err != nil {
		return fmt.Errorf("failed to delete checkpoint writes: %w", err)
	}

	return tx.Commit(ctx)
}

// Close closes the database connection pool.
func (s *PostgresSaver) Close() error {
	s.pool.Close()
	return nil
}

// Helper functions

// isPrimitive checks if a value is a primitive type (string, number, boolean, null).
func isPrimitive(v interface{}) bool {
	switch v.(type) {
	case string, int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64, bool, nil:
		return true
	default:
		return false
	}
}

// saveBlobs saves blob values to the checkpoint_blobs table.
func (s *PostgresSaver) saveBlobs(ctx context.Context, tx pgx.Tx, threadID, checkpointNS string, blobs map[string]interface{}) error {
	for channelName, value := range blobs {
		valueData, err := s.serializer.Serialize(value)
		if err != nil {
			return fmt.Errorf("failed to serialize blob value for channel %s: %w", channelName, err)
		}

		_, err = tx.Exec(ctx, `
			INSERT INTO checkpoint_blobs (
				id, thread_id, checkpoint_ns, channel_name,
				value, version, created_at
			) VALUES ($1, $2, $3, $4, $5, 1, NOW())
			ON CONFLICT (thread_id, checkpoint_ns, channel_name, version) 
			DO UPDATE SET
				value = EXCLUDED.value
		`,
			uuid.New().String(), threadID, checkpointNS, channelName, valueData,
		)
		if err != nil {
			return fmt.Errorf("failed to insert blob for channel %s: %w", channelName, err)
		}
	}

	return nil
}

// loadBlobs loads blob values from the checkpoint_blobs table.
func (s *PostgresSaver) loadBlobs(ctx context.Context, threadID, checkpointNS string) (map[string]interface{}, error) {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	rows, err := conn.Query(ctx, `
		SELECT channel_name, value
		FROM checkpoint_blobs
		WHERE thread_id = $1 AND checkpoint_ns = $2
	`, threadID, checkpointNS)
	if err != nil {
		if err == pgx.ErrNoRows {
			return make(map[string]interface{}), nil
		}
		return nil, fmt.Errorf("failed to query blobs: %w", err)
	}
	defer rows.Close()

	blobs := make(map[string]interface{})
	for rows.Next() {
		var channelName string
		var valueData []byte

		if err := rows.Scan(&channelName, &valueData); err != nil {
			return nil, fmt.Errorf("failed to scan blob row: %w", err)
		}

		var value interface{}
		if err := s.serializer.Deserialize(valueData, &value); err != nil {
			return nil, fmt.Errorf("failed to deserialize blob for channel %s: %w", channelName, err)
		}

		blobs[channelName] = value
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating blobs: %w", err)
	}

	return blobs, nil
}