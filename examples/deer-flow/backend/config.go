package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// Config is the root deer-flow configuration structure.
type Config struct {
	Server  ServerConfig  `yaml:"server"`
	Models  []ModelConfig `yaml:"models"`
	Skills  SkillsConfig  `yaml:"skills"`
	Memory  MemoryConfig  `yaml:"memory"`
}

type ServerConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	LogLevel string `yaml:"log_level"`
}

type ModelConfig struct {
	Name         string  `yaml:"name"`
	DisplayName  string  `yaml:"display_name"`
	Model        string  `yaml:"model"`
	APIBase      string  `yaml:"api_base"`
	APIKey       string  `yaml:"api_key"`
	MaxTokens    int     `yaml:"max_tokens"`
	Temperature  float64 `yaml:"temperature"`
	TimeoutSec   int     `yaml:"timeout_seconds"`
}

type SkillsConfig struct {
	Dir string `yaml:"dir"`
}

type MemoryConfig struct {
	Enabled                bool    `yaml:"enabled"`
	MaxFacts               int     `yaml:"max_facts"`
	FactConfidenceThreshold float64 `yaml:"fact_confidence_threshold"`
}

// DefaultConfig returns sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		Server: ServerConfig{
			Host:     "0.0.0.0",
			Port:     8001,
			LogLevel: "info",
		},
		Models: []ModelConfig{
			{
				Name:        "default",
				DisplayName: "Default Model",
				Model:       "gpt-4o",
				APIBase:     "https://api.openai.com/v1",
				MaxTokens:   4096,
				Temperature: 0.7,
				TimeoutSec:  120,
			},
		},
		Skills: SkillsConfig{
			Dir: "skills",
		},
		Memory: MemoryConfig{
			Enabled:                true,
			MaxFacts:               1000,
			FactConfidenceThreshold: 0.5,
		},
	}
}

// resolveConfigPath finds config.yaml, searching relative paths.
func resolveConfigPath() string {
	candidates := []string{
		"../config.yaml",                     // run from backend/
		"config.yaml",                        // run from examples/deer-flow/
		"examples/deer-flow/config.yaml",     // run from harness-go root
	}
	for _, c := range candidates {
		if _, err := os.Stat(c); err == nil {
			return c
		}
	}
	return ""
}

// LoadConfig reads and returns the configuration.
func LoadConfig() *Config {
	cfg := DefaultConfig()

	path := resolveConfigPath()
	if path == "" {
		fmt.Println("config.yaml not found, using defaults + environment variables")
		applyEnvOverrides(cfg)
		return cfg
	}

	data, err := os.ReadFile(path)
	if err != nil {
		fmt.Printf("Warning: cannot read %s: %v, using defaults\n", path, err)
		applyEnvOverrides(cfg)
		return cfg
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		fmt.Printf("Warning: cannot parse %s: %v, using defaults\n", path, err)
		applyEnvOverrides(cfg)
		return cfg
	}

	// Resolve relative skills dir to absolute
	if cfg.Skills.Dir != "" && !filepath.IsAbs(cfg.Skills.Dir) {
		cfg.Skills.Dir = filepath.Join(filepath.Dir(path), cfg.Skills.Dir)
	}

	applyEnvOverrides(cfg)
	return cfg
}

// applyEnvOverrides overrides config values from environment variables.
// Environment variables take precedence over config file.
func applyEnvOverrides(cfg *Config) {
	if v := os.Getenv("OPENAI_API_KEY"); v != "" {
		if len(cfg.Models) > 0 {
			cfg.Models[0].APIKey = v
		}
	}
	if v := os.Getenv("OPENAI_BASE_URL"); v != "" {
		if len(cfg.Models) > 0 {
			cfg.Models[0].APIBase = strings.TrimRight(v, "/")
		}
	}
	if v := os.Getenv("OPENAI_MODEL"); v != "" {
		if len(cfg.Models) > 0 {
			cfg.Models[0].Model = v
		}
	}
	if v := os.Getenv("PORT"); v != "" {
		fmt.Sscanf(v, "%d", &cfg.Server.Port)
	}
}
