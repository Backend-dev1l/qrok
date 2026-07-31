// Package config — конфигурация бинарей qrok.
//
// Приоритет источников (от низшего к высшему): дефолты → YAML-файл → env.
// Флаги — самый высокий приоритет: main парсит их сам и переопределяет
// значения уже загруженного конфига.
package config

import (
	"bytes"
	"errors"
	"io/fs"
	"os"
	"strconv"
	"time"

	"gopkg.in/yaml.v3"

	"qrok/pkg/fault"
	"qrok/pkg/postgres"
)

// EnvConfigPath — env-переменная с путём к YAML-файлу конфига.
const EnvConfigPath = "QROK_CONFIG"

// defaultConfigFile используется, если путь не задан ни флагом, ни env;
// отсутствие этого файла — не ошибка.
const defaultConfigFile = "qrok.yaml"

type Server struct {
	Log      Log      `yaml:"log"`
	HTTP     HTTP     `yaml:"http"`
	GRPC     GRPC     `yaml:"grpc"`
	Gateway  Gateway  `yaml:"gateway"`
	Postgres Postgres `yaml:"postgres"`
	Redis    Redis    `yaml:"redis"`
	S3       S3       `yaml:"s3"`
	Events   Events   `yaml:"events"`
}

type Log struct {
	Level  string `yaml:"level"`  // debug | info | warn | error
	Format string `yaml:"format"` // json | text | pretty
}

type HTTP struct {
	Addr             string   `yaml:"addr"`
	RequestTimeout   Duration `yaml:"request_timeout"`
	MaxBodyBytes     int64    `yaml:"max_body_bytes"`
	CORSOrigins      []string `yaml:"cors_origins"`
	AllowInsecureAPI bool     `yaml:"allow_insecure_api"` // dev: REST без Bearer-токена
}

type GRPC struct {
	Addr string `yaml:"addr"`
}

type Gateway struct {
	// AllowInsecureListen — только dev: ListenStream без dev-токена до qrok login.
	AllowInsecureListen bool `yaml:"allow_insecure_listen"`
}

type Postgres struct {
	DSN             string   `yaml:"dsn"`
	MaxConns        int32    `yaml:"max_conns"`
	MinConns        int32    `yaml:"min_conns"`
	MaxConnLifetime Duration `yaml:"max_conn_lifetime"`
	MaxConnIdleTime Duration `yaml:"max_conn_idle_time"`
	ConnectTimeout  Duration `yaml:"connect_timeout"`
}

// Pool конвертирует секцию в конфиг pkg/postgres.
func (p Postgres) Pool() postgres.Config {
	return postgres.Config{
		DSN:             p.DSN,
		MaxConns:        p.MaxConns,
		MinConns:        p.MinConns,
		MaxConnLifetime: time.Duration(p.MaxConnLifetime),
		MaxConnIdleTime: time.Duration(p.MaxConnIdleTime),
		ConnectTimeout:  time.Duration(p.ConnectTimeout),
	}
}

type Redis struct {
	Addr     string `yaml:"addr"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
}

type S3 struct {
	Endpoint  string `yaml:"endpoint"`
	Bucket    string `yaml:"bucket"`
	AccessKey string `yaml:"access_key"`
	SecretKey string `yaml:"secret_key"`
	UseSSL    bool   `yaml:"use_ssl"`
}

type Events struct {
	// PayloadThresholdBytes — порог гибридного хранения: payload меньше порога
	// хранится в Postgres, от порога и выше — в object store (см. ТЗ, раздел 5.5).
	PayloadThresholdBytes int64 `yaml:"payload_threshold_bytes"`
	// MaxPayloadBytes — жёсткий лимит размера события.
	MaxPayloadBytes int64 `yaml:"max_payload_bytes"`
}

func defaultServer() *Server {
	return &Server{
		Log: Log{Level: "info", Format: "json"},
		HTTP: HTTP{
			Addr:           ":8080",
			RequestTimeout: Duration(30 * time.Second),
			MaxBodyBytes:   1 << 20, // 1 МБ
		},
		GRPC: GRPC{Addr: ":9090"},
		Postgres: Postgres{
			DSN:             "postgres://qrok:qrok@127.0.0.1:5432/qrok?sslmode=disable",
			MaxConns:        10,
			MinConns:        2,
			MaxConnLifetime: Duration(30 * time.Minute),
			MaxConnIdleTime: Duration(5 * time.Minute),
			ConnectTimeout:  Duration(5 * time.Second),
		},
		Redis: Redis{Addr: "127.0.0.1:6379"},
		S3: S3{
			Endpoint:  "127.0.0.1:9000",
			Bucket:    "qrok-payloads",
			AccessKey: "qrok",
			SecretKey: "qrok-secret",
		},
		Events: Events{
			PayloadThresholdBytes: 256 << 10, // 256 КБ
			MaxPayloadBytes:       1 << 20,   // 1 МБ
		},
	}
}

// LoadServer загружает конфиг сервера: дефолты → YAML → env.
// path == "" — берётся из QROK_CONFIG, затем qrok.yaml (если существует).
func LoadServer(path string) (*Server, error) {
	cfg := defaultServer()

	if err := loadFile(&cfg, path); err != nil {
		return nil, err
	}
	if err := applyServerEnv(cfg); err != nil {
		return nil, err
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func loadFile(cfg any, path string) error {
	explicit := path != ""
	if !explicit {
		path = os.Getenv(EnvConfigPath)
		explicit = path != ""
	}
	if path == "" {
		path = defaultConfigFile
	}

	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) && !explicit {
		return nil // дефолтного файла может не быть — работаем на дефолтах и env
	}
	if err != nil {
		return fault.ErrValidation.
			Wrapf(err, "не удалось прочитать файл конфига %s", path).
			WithOp("config.load").
			WithHint("проверьте путь в --config / " + EnvConfigPath)
	}

	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true) // опечатка в ключе — ошибка, а не молчаливое игнорирование
	if err := dec.Decode(cfg); err != nil {
		return fault.ErrValidation.
			Wrapf(err, "невалидный YAML в %s", path).
			WithOp("config.load").
			WithHint("сверьте ключи с deploy/server.example.yaml")
	}
	return nil
}

// applyServerEnv накладывает переменные окружения QROK_* поверх файла.
func applyServerEnv(cfg *Server) error {
	envStr("QROK_LOG_LEVEL", &cfg.Log.Level)
	envStr("QROK_LOG_FORMAT", &cfg.Log.Format)
	envStr("QROK_HTTP_ADDR", &cfg.HTTP.Addr)
	if err := envBool("QROK_HTTP_ALLOW_INSECURE_API", &cfg.HTTP.AllowInsecureAPI); err != nil {
		return err
	}
	envStr("QROK_GRPC_ADDR", &cfg.GRPC.Addr)
	if err := envBool("QROK_GATEWAY_ALLOW_INSECURE_LISTEN", &cfg.Gateway.AllowInsecureListen); err != nil {
		return err
	}
	envStr("QROK_POSTGRES_DSN", &cfg.Postgres.DSN)
	envStr("QROK_REDIS_ADDR", &cfg.Redis.Addr)
	envStr("QROK_REDIS_PASSWORD", &cfg.Redis.Password)
	envStr("QROK_S3_ENDPOINT", &cfg.S3.Endpoint)
	envStr("QROK_S3_BUCKET", &cfg.S3.Bucket)
	envStr("QROK_S3_ACCESS_KEY", &cfg.S3.AccessKey)
	envStr("QROK_S3_SECRET_KEY", &cfg.S3.SecretKey)

	if err := envInt64("QROK_PAYLOAD_THRESHOLD_BYTES", &cfg.Events.PayloadThresholdBytes); err != nil {
		return err
	}
	return envInt64("QROK_MAX_PAYLOAD_BYTES", &cfg.Events.MaxPayloadBytes)
}

func (c *Server) validate() error {
	if c.Events.PayloadThresholdBytes <= 0 {
		return validationErr("events.payload_threshold_bytes должен быть > 0")
	}
	if c.Events.MaxPayloadBytes < c.Events.PayloadThresholdBytes {
		return validationErr("events.max_payload_bytes не может быть меньше payload_threshold_bytes")
	}
	if c.Postgres.DSN == "" {
		return validationErr("postgres.dsn обязателен")
	}
	if c.HTTP.Addr == "" || c.GRPC.Addr == "" {
		return validationErr("http.addr и grpc.addr обязательны")
	}
	return nil
}

func validationErr(msg string) error {
	return fault.ErrValidation.New(msg).
		WithOp("config.validate").
		WithHint("исправьте значение в файле конфига или env")
}

func envStr(key string, dst *string) {
	if v, ok := os.LookupEnv(key); ok {
		*dst = v
	}
}

func envInt64(key string, dst *int64) error {
	v, ok := os.LookupEnv(key)
	if !ok {
		return nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return fault.ErrValidation.
			Wrapf(err, "%s: ожидалось целое число, получено %q", key, v).
			WithOp("config.env")
	}
	*dst = n
	return nil
}

func envBool(key string, dst *bool) error {
	v, ok := os.LookupEnv(key)
	if !ok {
		return nil
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return fault.ErrValidation.
			Wrapf(err, "%s: ожидалось true/false, получено %q", key, v).
			WithOp("config.env")
	}
	*dst = b
	return nil
}
