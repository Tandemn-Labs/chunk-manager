package main

import (
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestLoadConfigDefaults(t *testing.T) {
	cfg, err := loadConfig(mapEnvironment(map[string]string{
		"DATABASE_URL": "postgres://database/chunks",
	}))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.databaseURL != "postgres://database/chunks" {
		t.Errorf("database URL = %q", cfg.databaseURL)
	}
	if cfg.grpcListenAddr != ":9090" {
		t.Errorf("gRPC listen address = %q", cfg.grpcListenAddr)
	}
	if cfg.adminListenAddr != ":9091" {
		t.Errorf("admin listen address = %q", cfg.adminListenAddr)
	}
	if cfg.reconcileInterval != 30*time.Second {
		t.Errorf("reconcile interval = %s", cfg.reconcileInterval)
	}
	if cfg.reconcileOperationLimit != 10*time.Second {
		t.Errorf("reconcile operation timeout = %s", cfg.reconcileOperationLimit)
	}
	if cfg.postgresMaxConnections != 0 {
		t.Errorf("PostgreSQL max connections = %d", cfg.postgresMaxConnections)
	}
	if cfg.storeLockTimeout != 0 {
		t.Errorf("store lock timeout = %s", cfg.storeLockTimeout)
	}
	if cfg.healthCheckInterval != 5*time.Second {
		t.Errorf("health check interval = %s", cfg.healthCheckInterval)
	}
	if cfg.shutdownTimeout != 15*time.Second {
		t.Errorf("shutdown timeout = %s", cfg.shutdownTimeout)
	}
	if cfg.logLevel != slog.LevelInfo {
		t.Errorf("log level = %s", cfg.logLevel)
	}
}

func TestLoadConfigOverrides(t *testing.T) {
	cfg, err := loadConfig(mapEnvironment(map[string]string{
		"DATABASE_URL":                "postgres://database/other",
		"GRPC_LISTEN_ADDR":            "127.0.0.1:19090",
		"ADMIN_LISTEN_ADDR":           "127.0.0.1:19091",
		"RECONCILE_INTERVAL":          "2m",
		"RECONCILE_OPERATION_TIMEOUT": "3s",
		"POSTGRES_MAX_CONNECTIONS":    "23",
		"STORE_LOCK_TIMEOUT":          "250ms",
		"HEALTH_CHECK_INTERVAL":       "7s",
		"SHUTDOWN_TIMEOUT":            "20s",
		"LOG_LEVEL":                   "debug",
	}))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.grpcListenAddr != "127.0.0.1:19090" {
		t.Errorf("gRPC listen address = %q", cfg.grpcListenAddr)
	}
	if cfg.adminListenAddr != "127.0.0.1:19091" {
		t.Errorf("admin listen address = %q", cfg.adminListenAddr)
	}
	if cfg.reconcileInterval != 2*time.Minute {
		t.Errorf("reconcile interval = %s", cfg.reconcileInterval)
	}
	if cfg.reconcileOperationLimit != 3*time.Second {
		t.Errorf("reconcile operation timeout = %s", cfg.reconcileOperationLimit)
	}
	if cfg.postgresMaxConnections != 23 {
		t.Errorf("PostgreSQL max connections = %d", cfg.postgresMaxConnections)
	}
	if cfg.storeLockTimeout != 250*time.Millisecond {
		t.Errorf("store lock timeout = %s", cfg.storeLockTimeout)
	}
	if cfg.healthCheckInterval != 7*time.Second {
		t.Errorf("health check interval = %s", cfg.healthCheckInterval)
	}
	if cfg.shutdownTimeout != 20*time.Second {
		t.Errorf("shutdown timeout = %s", cfg.shutdownTimeout)
	}
	if cfg.logLevel != slog.LevelDebug {
		t.Errorf("log level = %s", cfg.logLevel)
	}
}

func TestLoadConfigRejectsMissingDatabaseURL(t *testing.T) {
	_, err := loadConfig(mapEnvironment(nil))
	if err == nil || !strings.Contains(err.Error(), "DATABASE_URL is required") {
		t.Fatalf("error = %v", err)
	}
}

func TestLoadConfigRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value string
		want  string
	}{
		{name: "empty database URL", key: "DATABASE_URL", value: " ", want: "DATABASE_URL is required"},
		{name: "empty gRPC address", key: "GRPC_LISTEN_ADDR", value: "", want: "cannot be empty"},
		{name: "zero reconcile interval", key: "RECONCILE_INTERVAL", value: "0s", want: "greater than zero"},
		{name: "invalid operation timeout", key: "RECONCILE_OPERATION_TIMEOUT", value: "soon", want: "valid duration"},
		{name: "negative max connections", key: "POSTGRES_MAX_CONNECTIONS", value: "-1", want: "cannot be negative"},
		{name: "overflow max connections", key: "POSTGRES_MAX_CONNECTIONS", value: "2147483648", want: "32-bit integer"},
		{name: "negative lock timeout", key: "STORE_LOCK_TIMEOUT", value: "-1ms", want: "cannot be negative"},
		{name: "zero health interval", key: "HEALTH_CHECK_INTERVAL", value: "0", want: "greater than zero"},
		{name: "zero shutdown timeout", key: "SHUTDOWN_TIMEOUT", value: "0s", want: "greater than zero"},
		{name: "invalid log level", key: "LOG_LEVEL", value: "TRACE", want: "valid slog level"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			environment := map[string]string{"DATABASE_URL": "postgres://database/chunks"}
			environment[test.key] = test.value

			_, err := loadConfig(mapEnvironment(environment))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func mapEnvironment(values map[string]string) func(string) (string, bool) {
	return func(name string) (string, bool) {
		value, exists := values[name]
		return value, exists
	}
}
