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
	Token         string   `yaml:"token"`
	TunnelID      string   `yaml:"tunnel_id"`
	Gateway       string   `yaml:"gateway"` // host:port gRPC
	TLS           bool     `yaml:"tls"`
	TLSServerName string   `yaml:"tls_server_name"`
	TLSCAFile     string   `yaml:"tls_ca_file"`
	Topics        []string `yaml:"topics"`
	SourceType    string   `yaml:"source_type"` // kafka | rabbitmq
}

type AgentKafka struct {
	Brokers         []string `yaml:"brokers"`
	GroupID         string   `yaml:"group_id"`
	StartFromOldest bool     `yaml:"start_from_oldest"`
	TLS             bool     `yaml:"tls"`
	TLSServerName   string   `yaml:"tls_server_name"`
	TLSCAFile       string   `yaml:"tls_ca_file"`
	SASLMechanism   string   `yaml:"sasl_mechanism"`
	SASLUsername    string   `yaml:"sasl_username"`
	SASLPassword    string   `yaml:"sasl_password"`
}

func defaultAgent() *Agent {
	return &Agent{
		Log: Log{Level: "info", Format: "pretty"},
		Agent: AgentConn{
			Gateway:    "localhost:9090",
			TLS:        true,
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
	if err := envBool("QROK_GATEWAY_TLS", &cfg.Agent.TLS); err != nil {
		return err
	}
	envStr("QROK_GATEWAY_TLS_SERVER_NAME", &cfg.Agent.TLSServerName)
	envStr("QROK_GATEWAY_TLS_CA_FILE", &cfg.Agent.TLSCAFile)
	envStr("QROK_AGENT_SOURCE_TYPE", &cfg.Agent.SourceType)
	if v, ok := os.LookupEnv("QROK_AGENT_TOPICS"); ok {
		cfg.Agent.Topics = splitCSV(v)
	}
	if v, ok := os.LookupEnv("QROK_KAFKA_BROKERS"); ok {
		cfg.Kafka.Brokers = splitCSV(v)
	}
	envStr("QROK_KAFKA_GROUP_ID", &cfg.Kafka.GroupID)
	if err := envBool("QROK_KAFKA_TLS", &cfg.Kafka.TLS); err != nil {
		return err
	}
	envStr("QROK_KAFKA_TLS_SERVER_NAME", &cfg.Kafka.TLSServerName)
	envStr("QROK_KAFKA_TLS_CA_FILE", &cfg.Kafka.TLSCAFile)
	envStr("QROK_KAFKA_SASL_MECHANISM", &cfg.Kafka.SASLMechanism)
	envStr("QROK_KAFKA_SASL_USERNAME", &cfg.Kafka.SASLUsername)
	envStr("QROK_KAFKA_SASL_PASSWORD", &cfg.Kafka.SASLPassword)
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
		return validationErr("agent.token is required")
	}
	if c.Agent.TunnelID == "" {
		return validationErr("agent.tunnel_id is required")
	}
	if c.Agent.Gateway == "" {
		return validationErr("agent.gateway is required")
	}
	if len(c.Agent.Topics) == 0 {
		return validationErr("agent.topics requires at least one topic")
	}
	if c.Agent.SourceType != "kafka" {
		return validationErr("only agent.source_type=kafka is supported for now")
	}
	if len(c.Kafka.Brokers) == 0 {
		return validationErr("kafka.brokers is required")
	}
	switch strings.ToLower(c.Kafka.SASLMechanism) {
	case "", "plain", "scram-sha-256", "scram-sha-512":
	default:
		return validationErr("kafka.sasl_mechanism must be plain, scram-sha-256, or scram-sha-512")
	}
	if c.Kafka.SASLMechanism != "" && (c.Kafka.SASLUsername == "" || c.Kafka.SASLPassword == "") {
		return validationErr("kafka SASL username and password are required")
	}
	if c.Events.MaxPayloadBytes <= 0 {
		return validationErr("events.max_payload_bytes must be greater than zero")
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
