package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
)

const (
	agentTokenPrefix = "qrok_agt_"
	tokenEntropyLen  = 32 // 256 бит
)

// GenerateAgentToken создаёт высокоэнтропийный агентский токен.
// Plaintext показывается пользователю ровно один раз, hash хранится в БД.
func GenerateAgentToken() (plaintext, hash string, err error) {
	entropy := make([]byte, tokenEntropyLen)
	if _, err := rand.Read(entropy); err != nil {
		return "", "", err
	}

	plaintext = agentTokenPrefix + base64.RawURLEncoding.EncodeToString(entropy)
	return plaintext, HashAgentToken(plaintext), nil
}

// HashAgentToken возвращает детерминированный SHA-256 в hex-формате для
// поиска токена по уникальному индексу и использования в качестве ключа кэша.
// Argon2 здесь не нужен: токен содержит 256 бит криптографической случайности.
func HashAgentToken(plaintext string) string {
	sum := sha256.Sum256([]byte(plaintext))
	return hex.EncodeToString(sum[:])
}
