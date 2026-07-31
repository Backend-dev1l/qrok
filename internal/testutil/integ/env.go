// Package integ — общие настройки для интеграционных и e2e-тестов.
// Требует запущенный стек: make compose-up.
package integ

import "os"

const (
	defaultPostgresDSN = "postgres://qrok:qrok@127.0.0.1:5432/qrok?sslmode=disable"
	defaultKafkaBroker = "127.0.0.1:9092"
	defaultMinIO       = "127.0.0.1:9000"
)

// PostgresDSN возвращает DSN из TEST_POSTGRES_DSN или дефолт dev-compose.
func PostgresDSN() string {
	if dsn := os.Getenv("TEST_POSTGRES_DSN"); dsn != "" {
		return dsn
	}
	return defaultPostgresDSN
}

// KafkaBroker возвращает адрес брокера из TEST_KAFKA_BROKER или дефолт dev-compose.
func KafkaBroker() string {
	if b := os.Getenv("TEST_KAFKA_BROKER"); b != "" {
		return b
	}
	return defaultKafkaBroker
}

// MinIOEndpoint возвращает endpoint MinIO из TEST_S3_ENDPOINT или дефолт dev-compose.
func MinIOEndpoint() string {
	if ep := os.Getenv("TEST_S3_ENDPOINT"); ep != "" {
		return ep
	}
	return defaultMinIO
}
