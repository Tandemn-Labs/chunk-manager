package main

import (
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"
)

const (
	defaultGRPCListenAddr          = ":9090"
	defaultReconcileInterval       = 30 * time.Second
	defaultReconcileOperationLimit = 10 * time.Second
	defaultHealthCheckInterval     = 5 * time.Second
	defaultShutdownTimeout         = 15 * time.Second
)

type config struct {
	databaseURL             string
	grpcListenAddr          string
	reconcileInterval       time.Duration
	reconcileOperationLimit time.Duration
	postgresMaxConnections  int32
	storeLockTimeout        time.Duration
	healthCheckInterval     time.Duration
	shutdownTimeout         time.Duration
	logLevel                slog.Level
}

func loadConfig(lookupEnv func(string) (string, bool)) (config, error) {
	databaseURL, exists := lookupEnv("DATABASE_URL")
	if !exists || strings.TrimSpace(databaseURL) == "" {
		return config{}, fmt.Errorf("DATABASE_URL is required")
	}

	grpcListenAddr, err := readString(lookupEnv, "GRPC_LISTEN_ADDR", defaultGRPCListenAddr)
	if err != nil {
		return config{}, err
	}
	reconcileInterval, err := readDuration(
		lookupEnv,
		"RECONCILE_INTERVAL",
		defaultReconcileInterval,
		false,
	)
	if err != nil {
		return config{}, err
	}
	reconcileOperationLimit, err := readDuration(
		lookupEnv,
		"RECONCILE_OPERATION_TIMEOUT",
		defaultReconcileOperationLimit,
		false,
	)
	if err != nil {
		return config{}, err
	}
	storeLockTimeout, err := readDuration(lookupEnv, "STORE_LOCK_TIMEOUT", 0, true)
	if err != nil {
		return config{}, err
	}
	healthCheckInterval, err := readDuration(
		lookupEnv,
		"HEALTH_CHECK_INTERVAL",
		defaultHealthCheckInterval,
		false,
	)
	if err != nil {
		return config{}, err
	}
	shutdownTimeout, err := readDuration(
		lookupEnv,
		"SHUTDOWN_TIMEOUT",
		defaultShutdownTimeout,
		false,
	)
	if err != nil {
		return config{}, err
	}
	postgresMaxConnections, err := readMaxConnections(lookupEnv)
	if err != nil {
		return config{}, err
	}
	logLevel, err := readLogLevel(lookupEnv)
	if err != nil {
		return config{}, err
	}

	return config{
		databaseURL:             databaseURL,
		grpcListenAddr:          grpcListenAddr,
		reconcileInterval:       reconcileInterval,
		reconcileOperationLimit: reconcileOperationLimit,
		postgresMaxConnections:  postgresMaxConnections,
		storeLockTimeout:        storeLockTimeout,
		healthCheckInterval:     healthCheckInterval,
		shutdownTimeout:         shutdownTimeout,
		logLevel:                logLevel,
	}, nil
}

func readString(
	lookupEnv func(string) (string, bool),
	name string,
	defaultValue string,
) (string, error) {
	value, exists := lookupEnv(name)
	if !exists {
		return defaultValue, nil
	}
	if strings.TrimSpace(value) == "" {
		return "", fmt.Errorf("%s cannot be empty", name)
	}
	return value, nil
}

func readDuration(
	lookupEnv func(string) (string, bool),
	name string,
	defaultValue time.Duration,
	allowZero bool,
) (time.Duration, error) {
	value, exists := lookupEnv(name)
	if !exists {
		return defaultValue, nil
	}
	duration, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be a valid duration: %w", name, err)
	}
	if duration < 0 || (!allowZero && duration == 0) {
		if allowZero {
			return 0, fmt.Errorf("%s cannot be negative", name)
		}
		return 0, fmt.Errorf("%s must be greater than zero", name)
	}
	return duration, nil
}

func readMaxConnections(lookupEnv func(string) (string, bool)) (int32, error) {
	value, exists := lookupEnv("POSTGRES_MAX_CONNECTIONS")
	if !exists {
		return 0, nil
	}
	connections, err := strconv.ParseInt(value, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("POSTGRES_MAX_CONNECTIONS must be a valid 32-bit integer: %w", err)
	}
	if connections < 0 {
		return 0, fmt.Errorf("POSTGRES_MAX_CONNECTIONS cannot be negative")
	}
	return int32(connections), nil
}

func readLogLevel(lookupEnv func(string) (string, bool)) (slog.Level, error) {
	value := "INFO"
	if configured, exists := lookupEnv("LOG_LEVEL"); exists {
		value = configured
	}
	var level slog.Level
	if err := level.UnmarshalText([]byte(value)); err != nil {
		return 0, fmt.Errorf("LOG_LEVEL must be a valid slog level: %w", err)
	}
	return level, nil
}
