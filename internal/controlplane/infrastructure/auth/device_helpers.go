package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

const (
	DefaultDeviceTTL    = 10 * time.Minute
	DefaultPollInterval = 5
)

func HashDeviceCode(deviceCode string) string {
	sum := sha256.Sum256([]byte(deviceCode))
	return hex.EncodeToString(sum[:])
}

func RandomURLSafe(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func RandomUserCode() (string, error) {
	const alphabet = "BCDFGHJKLMNPQRSTVWXYZ23456789"
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	for i := range b {
		b[i] = alphabet[int(b[i])%len(alphabet)]
	}
	return fmt.Sprintf("%s-%s", string(b[:4]), string(b[4:])), nil
}

func NormalizeUserCode(code string) string {
	code = strings.ToUpper(strings.TrimSpace(code))
	code = strings.ReplaceAll(code, " ", "")
	code = strings.ReplaceAll(code, "-", "")
	if len(code) == 8 {
		return code[:4] + "-" + code[4:]
	}
	return strings.ToUpper(strings.TrimSpace(code))
}
