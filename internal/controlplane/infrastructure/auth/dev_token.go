package auth

import (
	"crypto/rand"
	"encoding/base64"
)

const devTokenPrefix = "qrok_dev_"

// GenerateDevToken создаёт dev-токен для qrok listen (показывается один раз).
func GenerateDevToken() (plaintext, hash string, err error) {
	entropy := make([]byte, tokenEntropyLen)
	if _, err := rand.Read(entropy); err != nil {
		return "", "", err
	}

	plaintext = devTokenPrefix + base64.RawURLEncoding.EncodeToString(entropy)
	return plaintext, HashAgentToken(plaintext), nil
}

// IsDevToken возвращает true для токенов формата qrok_dev_*.
func IsDevToken(plaintext string) bool {
	return len(plaintext) > len(devTokenPrefix) && plaintext[:len(devTokenPrefix)] == devTokenPrefix
}
