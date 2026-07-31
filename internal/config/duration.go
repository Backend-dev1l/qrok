package config

import (
	"time"

	"gopkg.in/yaml.v3"

	"qrok/pkg/fault"
)

// Duration — обёртка над time.Duration с поддержкой YAML-строк вида "30s", "5m".
// yaml.v3 не умеет парсить time.Duration из строки самостоятельно.
type Duration time.Duration

func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	var s string
	if err := node.Decode(&s); err != nil {
		return err
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fault.ErrValidation.
			Wrapf(err, "невалидная длительность %q", s).
			WithOp("config.parse_duration").
			WithHint(`формат Go duration: "30s", "5m", "1h30m"`)
	}
	*d = Duration(parsed)
	return nil
}

func (d Duration) MarshalYAML() (any, error) {
	return time.Duration(d).String(), nil
}
