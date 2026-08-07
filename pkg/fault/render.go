package fault

import (
	"fmt"
	"os"
	"strings"
)

const (
	ansiReset = "\x1b[0m"
	ansiRed   = "\x1b[31m"
	ansiDim   = "\x1b[2m"
	ansiBold  = "\x1b[1m"
)

// RenderCLI форматирует ошибку для вывода в терминал:
//
//	✗ не удалось подключиться к Kafka (SERVICE_UNAVAILABLE)
//
//	  операция:  agent.kafka.subscribe
//	  причина:   dial tcp 10.0.1.5:9092: connection refused
//	  подсказка: проверьте --brokers и что порт 9092 доступен с этой машины
//
// Цвет отключается переменной окружения NO_COLOR (https://no-color.org).
func RenderCLI(err error) string {
	f := FromError(err)
	if f == nil {
		return ""
	}

	color := os.Getenv("NO_COLOR") == ""
	paint := func(code, s string) string {
		if !color {
			return s
		}
		return code + s + ansiReset
	}

	var b strings.Builder
	fmt.Fprintf(&b, "%s %s %s\n",
		paint(ansiRed+ansiBold, "✗"),
		paint(ansiBold, f.Message()),
		paint(ansiDim, "("+string(f.code)+")"),
	)

	details := make([][2]string, 0, 4)
	if f.op != "" {
		details = append(details, [2]string{"operation", f.op})
	}
	if f.cause != nil {
		details = append(details, [2]string{"cause", f.cause.Error()})
	}
	for k, v := range f.args {
		details = append(details, [2]string{k, v})
	}
	if f.hint != "" {
		details = append(details, [2]string{"hint", f.hint})
	}

	if len(details) > 0 {
		b.WriteString("\n")
		width := 0
		for _, d := range details {
			if len([]rune(d[0])) > width {
				width = len([]rune(d[0]))
			}
		}
		for _, d := range details {
			pad := strings.Repeat(" ", width-len([]rune(d[0])))
			fmt.Fprintf(&b, "  %s%s %s\n", paint(ansiDim, d[0]+":"), pad, d[1])
		}
	}

	return strings.TrimRight(b.String(), "\n")
}
