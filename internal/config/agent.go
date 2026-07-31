package config

import (
	"os"
	"strings"
)

// Agent — конфиг агента на стейджинге.
type Agent struct {
	Log    Log        `yaml:"log"`
	Agent  AgentConn  `yaml:"agent"`
	Kafka  AgentKafka `yaml:"kafka"`
	Events Events     `yaml:"events"`
}

type AgentConn struct {
	Token      string   `yaml:"token"`
	TunnelID   string   `yaml:"tunnel_id"`
	Gateway    string   `yaml:"gateway"` // host:port gRPC
	Topics     []string `yaml:"topics"`
	SourceType string   `yaml:"source_type"` // kafka | rabbitmq
}

type AgentKafka struct {
	Brokers         []string `yaml:"brokers"`
	GroupID         string   `yaml:"group_id"`
	StartFromOldest bool     `yaml:"start_from_oldest"`
}

func defaultAgent() *Agent {
	return &Agent{
		Log: Log{Level: "info", Format: "pretty"},
		Agent: AgentConn{
			Gateway:    "localhost:9090",
			SourceType: "kafka",
		},
		Kafka: AgentKafka{
			Brokers: []string{"localhost:9092"},
		},
		Events: Events{
			PayloadThresholdBytes: 256 << 10,
			MaxPayloadBytes:       1 << 20,
		},
	}
}

// LoadAgent загружает конфиг агента: дефолты → YAML → env.
func LoadAgent(path string) (*Agent, error) {
	cfg := defaultAgent()
	if err := loadFile(&cfg, path); err != nil {
		return nil, err
	}
	if err := applyAgentEnv(cfg); err != nil {
		return nil, err
	}
	return cfg, cfg.validate()
}

func applyAgentEnv(cfg *Agent) error {
	envStr("QROK_LOG_LEVEL", &cfg.Log.Level)
	envStr("QROK_LOG_FORMAT", &cfg.Log.Format)
	envStr("QROK_AGENT_TOKEN", &cfg.Agent.Token)
	envStr("QROK_TUNNEL_ID", &cfg.Agent.TunnelID)
	envStr("QROK_GATEWAY_ADDR", &cfg.Agent.Gateway)
	envStr("QROK_AGENT_SOURCE_TYPE", &cfg.Agent.SourceType)
	if v, ok := os.LookupEnv("QROK_AGENT_TOPICS"); ok {
		cfg.Agent.Topics = splitCSV(v)
	}
	if v, ok := os.LookupEnv("QROK_KAFKA_BROKERS"); ok {
		cfg.Kafka.Brokers = splitCSV(v)
	}
	envStr("QROK_KAFKA_GROUP_ID", &cfg.Kafka.GroupID)
	if err := envBool("QROK_KAFKA_START_FROM_OLDEST", &cfg.Kafka.StartFromOldest); err != nil {
		return err
	}
	if err := envInt64("QROK_MAX_PAYLOAD_BYTES", &cfg.Events.MaxPayloadBytes); err != nil {
		return err
	}
	return nil
}

func (c *Agent) validate() error {
	if c.Agent.Token == "" {
		return validationErr("agent.token обязателен")
	}
	if c.Agent.TunnelID == "" {
		return validationErr("agent.tunnel_id обязателен")
	}
	if c.Agent.Gateway == "" {
		return validationErr("agent.gateway обязателен")
	}
	if len(c.Agent.Topics) == 0 {
		return validationErr("agent.topics обязателен (хотя бы один топик)")
	}
	if c.Agent.SourceType != "kafka" {
		return validationErr("пока поддерживается только agent.source_type=kafka")
	}
	if len(c.Kafka.Brokers) == 0 {
		return validationErr("kafka.brokers обязателен")
	}
	if c.Events.MaxPayloadBytes <= 0 {
		return validationErr("events.max_payload_bytes должен быть > 0")
	}
	return nil
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
