package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type DBConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
}

func (d DBConfig) DSN() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true", d.User, d.Password, d.Host, d.Port, d.Database)
}

// SoftRelationship describes an undeclared FK: FromTable.FromColumn references ToTable.ToColumn.
type SoftRelationship struct {
	FromTable  string `yaml:"fromTable"`
	FromColumn string `yaml:"fromColumn"`
	ToTable    string `yaml:"toTable"`
	ToColumn   string `yaml:"toColumn"`
}

// ExcludeField identifies a single column in a table whose value should be replaced with NULL in the dump.
type ExcludeField struct {
	Table string `yaml:"table"`
	Field string `yaml:"field"`
}

// ReplaceField specifies a static value to substitute for a column in the dump (for anonymization).
type ReplaceField struct {
	Table string `yaml:"table"`
	Field string `yaml:"field"`
	Value string `yaml:"value"`
}

type Config struct {
	Origin            DBConfig           `yaml:"origin"`
	ExcludeTables     []string           `yaml:"excludeTables"`
	EmptyTables       []string           `yaml:"emptyTables"`
	ExcludeFields     []ExcludeField     `yaml:"excludeFields"`
	ReplaceFields     []ReplaceField     `yaml:"replaceFields"`
	KeepUsernames     []string           `yaml:"keepUsernames"`
	SoftRelationships []SoftRelationship `yaml:"softRelationships"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing config file: %w", err)
	}

	return &cfg, nil
}
