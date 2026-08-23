package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"qrok/pkg/fault"
)

func writeTemp(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "qrok.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// unsetEnv убирает переменную на время теста (make export .env не должен ломать дефолты).
func unsetEnv(t *testing.T, key string) {
	t.Helper()
	prev, ok := os.LookupEnv(key)
	if !ok {
		return
	}
	if err := os.Unsetenv(key); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Setenv(key, prev); err != nil {
			t.Errorf("восстановить %s: %v", key, err)
		}
	})
}

func clearServerEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"QROK_LOG_LEVEL",
		"QROK_LOG_FORMAT",
		"QROK_HTTP_ADDR",
		"QROK_HTTP_ALLOW_INSECURE_API",
		"QROK_GRPC_ADDR",
		"QROK_GATEWAY_ALLOW_INSECURE_LISTEN",
		"QROK_POSTGRES_DSN",
		"QROK_REDIS_ADDR",
		"QROK_REDIS_PASSWORD",
		"QROK_S3_ENDPOINT",
		"QROK_S3_BUCKET",
		"QROK_S3_ACCESS_KEY",
		"QROK_S3_SECRET_KEY",
		"QROK_PAYLOAD_THRESHOLD_BYTES",
		"QROK_MAX_PAYLOAD_BYTES",
	} {
		unsetEnv(t, key)
	}
}

func TestDefaults(t *testing.T) {
	t.Chdir(t.TempDir()) // пустая директория: дефолтного qrok.yaml нет
	clearServerEnv(t)
	t.Setenv(EnvConfigPath, "")

	cfg, err := LoadServer("")
	if err != nil {
		t.Fatal(err)
	}

	if cfg.Log.Level != "info" || cfg.Log.Format != "json" {
		t.Errorf("дефолты лога: %+v", cfg.Log)
	}
	if cfg.HTTP.Addr != ":8080" {
		t.Errorf("дефолтный http.addr: %q", cfg.HTTP.Addr)
	}
	if cfg.Events.PayloadThresholdBytes != 256<<10 {
		t.Errorf("дефолтный порог payload: %d", cfg.Events.PayloadThresholdBytes)
	}
	if time.Duration(cfg.HTTP.RequestTimeout) != 30*time.Second {
		t.Errorf("дефолтный request_timeout: %v", cfg.HTTP.RequestTimeout)
	}

	// Дефолты пула Postgres живут здесь, а не в pkg/postgres.
	if cfg.Postgres.MaxConns != 10 || cfg.Postgres.MinConns != 2 {
		t.Errorf("дефолты пула: %+v", cfg.Postgres)
	}
	if time.Duration(cfg.Postgres.ConnectTimeout) != 5*time.Second {
		t.Errorf("дефолтный connect_timeout: %v", cfg.Postgres.ConnectTimeout)
	}
	if time.Duration(cfg.Postgres.MaxConnLifetime) != 30*time.Minute {
		t.Errorf("дефолтный max_conn_lifetime: %v", cfg.Postgres.MaxConnLifetime)
	}
}

func TestLoadYAML(t *testing.T) {
	path := writeTemp(t, `
log:
  level: debug
  format: pretty
http:
  addr: ":9999"
  request_timeout: 5s
postgres:
  dsn: postgres://u:p@db:5432/qrok
  max_conns: 25
events:
  payload_threshold_bytes: 131072
  max_payload_bytes: 2097152
`)

	cfg, err := LoadServer(path)
	if err != nil {
		t.Fatal(err)
	}

	if cfg.Log.Level != "debug" || cfg.Log.Format != "pretty" {
		t.Errorf("лог из файла не применился: %+v", cfg.Log)
	}
	if cfg.HTTP.Addr != ":9999" {
		t.Errorf("http.addr = %q", cfg.HTTP.Addr)
	}
	if time.Duration(cfg.HTTP.RequestTimeout) != 5*time.Second {
		t.Errorf("request_timeout = %v", cfg.HTTP.RequestTimeout)
	}
	if cfg.Postgres.MaxConns != 25 {
		t.Errorf("max_conns = %d", cfg.Postgres.MaxConns)
	}
	if cfg.Events.PayloadThresholdBytes != 128<<10 {
		t.Errorf("порог = %d", cfg.Events.PayloadThresholdBytes)
	}
	// Незатронутые файлом значения остаются дефолтными.
	if cfg.GRPC.Addr != ":9090" {
		t.Errorf("grpc.addr должен остаться дефолтным: %q", cfg.GRPC.Addr)
	}
}

func TestEnvOverridesFile(t *testing.T) {
	path := writeTemp(t, `
log:
  level: warn
`)
	t.Setenv("QROK_LOG_LEVEL", "error")
	t.Setenv("QROK_POSTGRES_DSN", "postgres://env:env@envhost:5432/qrok")

	cfg, err := LoadServer(path)
	if err != nil {
		t.Fatal(err)
	}

	if cfg.Log.Level != "error" {
		t.Errorf("env должен переопределять файл: %q", cfg.Log.Level)
	}
	if cfg.Postgres.DSN != "postgres://env:env@envhost:5432/qrok" {
		t.Errorf("dsn из env не применился: %q", cfg.Postgres.DSN)
	}
}

func TestUnknownKeyRejected(t *testing.T) {
	path := writeTemp(t, `
log:
  levl: debug
`)
	_, err := LoadServer(path)
	if fault.CodeOf(err) != fault.ErrValidation {
		t.Errorf("опечатка в ключе должна давать VALIDATION_ERROR: %v", err)
	}
}

func TestBadDuration(t *testing.T) {
	path := writeTemp(t, `
http:
  request_timeout: пять секунд
`)
	_, err := LoadServer(path)
	if fault.CodeOf(err) != fault.ErrValidation {
		t.Errorf("ожидался VALIDATION_ERROR: %v", err)
	}
}

func TestBadEnvInt(t *testing.T) {
	t.Setenv("QROK_PAYLOAD_THRESHOLD_BYTES", "много")
	_, err := LoadServer("")
	if fault.CodeOf(err) != fault.ErrValidation {
		t.Errorf("ожидался VALIDATION_ERROR: %v", err)
	}
}

func TestExplicitMissingFileIsError(t *testing.T) {
	_, err := LoadServer(filepath.Join(t.TempDir(), "no-such.yaml"))
	if fault.CodeOf(err) != fault.ErrValidation {
		t.Errorf("явно указанный несуществующий файл должен давать ошибку: %v", err)
	}
}

func TestValidation(t *testing.T) {
	path := writeTemp(t, `
events:
  payload_threshold_bytes: 2097152
  max_payload_bytes: 1048576
`)
	_, err := LoadServer(path)
	if fault.CodeOf(err) != fault.ErrValidation {
		t.Errorf("max < threshold должен давать VALIDATION_ERROR: %v", err)
	}
}

func TestPostgresPoolConversion(t *testing.T) {
	p := Postgres{
		DSN:            "postgres://u:p@h:5432/db",
		MaxConns:       7,
		ConnectTimeout: Duration(3 * time.Second),
	}
	pool := p.Pool()
	if pool.DSN != p.DSN || pool.MaxConns != 7 || pool.ConnectTimeout != 3*time.Second {
		t.Errorf("конвертация в postgres.Config потеряла значения: %+v", pool)
	}
}

func TestClientDefaultsUseTLS(t *testing.T) {
	t.Parallel()

	if !defaultAgent().Agent.TLS {
		t.Fatal("agent gateway TLS must be enabled by default")
	}
	if !defaultListen().Listen.TLS {
		t.Fatal("listen gateway TLS must be enabled by default")
	}
}

func TestAgentRejectsIncompleteSASLConfig(t *testing.T) {
	t.Parallel()

	cfg := defaultAgent()
	cfg.Agent.Token = "token"
	cfg.Agent.TunnelID = "tunnel-1"
	cfg.Agent.Topics = []string{"orders"}
	cfg.Kafka.SASLMechanism = "scram-sha-256"

	err := cfg.validate()
	if fault.CodeOf(err) != fault.ErrValidation {
		t.Fatalf("validate() error = %v, want validation", err)
	}
}
