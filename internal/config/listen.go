package config

import "os"

// Listen — конфиг dev-клиента.
type Listen struct {
	Log    Log        `yaml:"log"`
	Listen ListenConn `yaml:"listen"`
}

type ListenConn struct {
	Gateway       string   `yaml:"gateway"`
	TLS           bool     `yaml:"tls"`
	TLSServerName string   `yaml:"tls_server_name"`
	TLSCAFile     string   `yaml:"tls_ca_file"`
	TunnelID      string   `yaml:"tunnel_id"`
	Topics        []string `yaml:"topics"`
	Forward       string   `yaml:"forward"` // http://localhost:8080/events
	Token         string   `yaml:"token"`   // dev-токен (после qrok login); пока опционален
	CatchUp       bool     `yaml:"catch_up"`
}

func defaultListen() *Listen {
	return &Listen{
		Log: Log{Level: "info", Format: "pretty"},
		Listen: ListenConn{
			Gateway: "localhost:9090",
			TLS:     true,
			Forward: "http://localhost:8080/events",
		},
	}
}

// LoadListen загружает конфиг dev-клиента.
func LoadListen(path string) (*Listen, error) {
	cfg := defaultListen()
	if err := loadFile(&cfg, path); err != nil {
		return nil, err
	}
	if err := applyListenEnv(cfg); err != nil {
		return nil, err
	}
	return cfg, cfg.validate()
}

func applyListenEnv(cfg *Listen) error {
	envStr("QROK_LOG_LEVEL", &cfg.Log.Level)
	envStr("QROK_LOG_FORMAT", &cfg.Log.Format)
	envStr("QROK_GATEWAY_ADDR", &cfg.Listen.Gateway)
	if err := envBool("QROK_GATEWAY_TLS", &cfg.Listen.TLS); err != nil {
		return err
	}
	envStr("QROK_GATEWAY_TLS_SERVER_NAME", &cfg.Listen.TLSServerName)
	envStr("QROK_GATEWAY_TLS_CA_FILE", &cfg.Listen.TLSCAFile)
	envStr("QROK_TUNNEL_ID", &cfg.Listen.TunnelID)
	envStr("QROK_FORWARD_URL", &cfg.Listen.Forward)
	envStr("QROK_DEV_TOKEN", &cfg.Listen.Token)
	if v, ok := os.LookupEnv("QROK_LISTEN_TOPICS"); ok {
		cfg.Listen.Topics = splitCSV(v)
	}
	return envBool("QROK_LISTEN_CATCH_UP", &cfg.Listen.CatchUp)
}

func (c *Listen) validate() error {
	if c.Listen.TunnelID == "" {
		return validationErr("listen.tunnel_id is required")
	}
	if c.Listen.Gateway == "" {
		return validationErr("listen.gateway is required")
	}
	if c.Listen.Forward == "" {
		return validationErr("listen.forward is required")
	}
	return nil
}
