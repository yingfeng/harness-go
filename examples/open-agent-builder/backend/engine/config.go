package engine

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// ModelRegistry holds all configured models and provides lookup.
type ModelRegistry struct {
	Models []ModelConfig
}

// Get returns the model config by name.
func (r *ModelRegistry) Get(name string) *ModelConfig {
	for i := range r.Models {
		if r.Models[i].Name == name {
			return &r.Models[i]
		}
	}
	for i := range r.Models {
		if r.Models[i].APIKey != "" {
			return &r.Models[i]
		}
	}
	if len(r.Models) > 0 {
		return &r.Models[0]
	}
	return nil
}

// Default returns the first "default" model or the first available model.
func (r *ModelRegistry) Default() *ModelConfig {
	if m := r.Get("default"); m != nil {
		return m
	}
	if len(r.Models) > 0 {
		return &r.Models[0]
	}
	return nil
}

// LoadConfigFile reads and parses config.yaml from the given path.
func LoadConfigFile(path string) (*AppConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}
	var cfg AppConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config file: %w", err)
	}
	return &cfg, nil
}

// NewRegistryFromConfig creates a ModelRegistry from an AppConfig.
func NewRegistryFromConfig(cfg *AppConfig) *ModelRegistry {
	return &ModelRegistry{Models: cfg.Models}
}
