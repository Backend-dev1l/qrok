// Package auth — модуль аутентификации control plane: пароли пользователей,
// агентские токены, дальше — JWT-сессии и API-ключи (этап 1).
//
// Криптография (см. ТЗ, разделы 2 и 7):
//   - пароли пользователей — argon2id (секрет придумал человек, нужна
//     memory-hard функция против перебора) — этот файл;
//   - агентские токены — 256 бит случайности + SHA-256 (детерминированный
//     lookup) — token.go.
package auth

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"golang.org/x/crypto/argon2"
)

// Параметры argon2id — рекомендация OWASP (19 МиБ памяти, 2 итерации,
// 1 поток). Они зашиваются в PHC-строку хеша, поэтому VerifyPassword
// проверяет и старые пароли после любого ужесточения параметров;
// сами константы влияют только на новые хеши.
const (
	argonMemoryKiB = 19 * 1024
	argonTime      = 2
	argonThreads   = 1
	argonSaltLen   = 16
	argonKeyLen    = 32

	// Ограничения не дают повреждённой PHC-строке вызвать чрезмерное
	// потребление CPU или памяти во время проверки пароля.
	maxArgonMemoryKiB = 64 * 1024
	maxArgonTime      = 10
	maxArgonThreads   = 16
)

// HashPassword хеширует пароль argon2id и возвращает PHC-строку вида
// $argon2id$v=19$m=19456,t=2,p=1$<salt>$<hash> — она целиком кладётся
// в колонку users.password_hash.
func HashPassword(password string) (string, error) {
	salt := make([]byte, argonSaltLen)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}

	key := argon2.IDKey([]byte(password), salt, argonTime, argonMemoryKiB, argonThreads, argonKeyLen)

	return fmt.Sprintf("$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s",
		argon2.Version, argonMemoryKiB, argonTime, argonThreads,
		base64.RawStdEncoding.EncodeToString(salt),
		base64.RawStdEncoding.EncodeToString(key),
	), nil
}

// VerifyPassword проверяет пароль против PHC-строки из БД. Несовпадение
// пароля — это (false, nil); ошибка возвращается только если сама строка
// хеша повреждена (это внутренняя проблема данных, не проблема пользователя).
func VerifyPassword(encoded, password string) (bool, error) {
	params, salt, key, err := parsePasswordHash(encoded)
	if err != nil {
		return false, err
	}

	got := argon2.IDKey([]byte(password), salt, params.time, params.memoryKiB, params.threads, uint32(len(key)))
	return subtle.ConstantTimeCompare(got, key) == 1, nil
}

type argonParams struct {
	memoryKiB uint32
	time      uint32
	threads   uint8
}

func parsePasswordHash(encoded string) (argonParams, []byte, []byte, error) {
	var p argonParams

	parts := strings.Split(encoded, "$")
	if len(parts) != 6 || parts[0] != "" {
		return p, nil, nil, hashFormatErr("expected PHC string with 6 sections")
	}
	if parts[1] != "argon2id" {
		return p, nil, nil, hashFormatErr("unsupported algorithm " + parts[1])
	}

	version, err := parseUintParam(parts[2], "v=", 8)
	if err != nil || version != argon2.Version {
		return p, nil, nil, hashFormatErr("unsupported argon2 version: " + parts[2])
	}

	rawParams := strings.Split(parts[3], ",")
	if len(rawParams) != 3 {
		return p, nil, nil, hashFormatErr("invalid parameters: " + parts[3])
	}
	memory, errMemory := parseUintParam(rawParams[0], "m=", 32)
	iterations, errTime := parseUintParam(rawParams[1], "t=", 32)
	threads, errThreads := parseUintParam(rawParams[2], "p=", 8)
	if errMemory != nil || errTime != nil || errThreads != nil {
		return p, nil, nil, hashFormatErr("invalid parameters: " + parts[3])
	}
	p = argonParams{memoryKiB: uint32(memory), time: uint32(iterations), threads: uint8(threads)}
	if p.memoryKiB == 0 || p.time == 0 || p.threads == 0 {
		return p, nil, nil, hashFormatErr("parameters must be > 0: " + parts[3])
	}
	if p.memoryKiB > maxArgonMemoryKiB || p.time > maxArgonTime || p.threads > maxArgonThreads {
		return p, nil, nil, hashFormatErr("parameters exceed safe limit: " + parts[3])
	}

	salt, err := base64.RawStdEncoding.DecodeString(parts[4])
	if err != nil || len(salt) < 8 || len(salt) > 64 {
		return p, nil, nil, hashFormatErr("invalid base64 salt")
	}
	key, err := base64.RawStdEncoding.DecodeString(parts[5])
	if err != nil || len(key) < 16 || len(key) > 64 {
		return p, nil, nil, hashFormatErr("invalid base64 hash")
	}

	return p, salt, key, nil
}

func parseUintParam(value, prefix string, bitSize int) (uint64, error) {
	if !strings.HasPrefix(value, prefix) || len(value) == len(prefix) {
		return 0, fmt.Errorf("expected prefix %q", prefix)
	}
	return strconv.ParseUint(value[len(prefix):], 10, bitSize)
}

func hashFormatErr(detail string) error {
	return fmt.Errorf("%w: %s", ErrMalformedPasswordHash, detail)
}

var ErrMalformedPasswordHash = errors.New("malformed password hash")
