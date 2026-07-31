package auth

import (
	"encoding/base64"
	"strings"
	"testing"
)

func TestGenerateAgentToken(t *testing.T) {
	t.Parallel()

	plaintext, hash, err := GenerateAgentToken()
	if err != nil {
		t.Fatalf("GenerateAgentToken() error = %v", err)
	}
	if !strings.HasPrefix(plaintext, agentTokenPrefix) {
		t.Fatalf("token = %q, want prefix %q", plaintext, agentTokenPrefix)
	}

	entropy, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(plaintext, agentTokenPrefix))
	if err != nil {
		t.Fatalf("token entropy is not base64url: %v", err)
	}
	if len(entropy) != tokenEntropyLen {
		t.Fatalf("entropy length = %d, want %d", len(entropy), tokenEntropyLen)
	}
	if hash == plaintext {
		t.Fatal("stored hash exposes plaintext token")
	}
	if hash != HashAgentToken(plaintext) {
		t.Fatal("returned hash does not match HashAgentToken(token)")
	}
	if len(hash) != 64 {
		t.Fatalf("SHA-256 hex length = %d, want 64", len(hash))
	}
}

func TestGenerateAgentTokenIsUnique(t *testing.T) {
	t.Parallel()

	first, firstHash, err := GenerateAgentToken()
	if err != nil {
		t.Fatalf("first GenerateAgentToken() error = %v", err)
	}
	second, secondHash, err := GenerateAgentToken()
	if err != nil {
		t.Fatalf("second GenerateAgentToken() error = %v", err)
	}
	if first == second || firstHash == secondHash {
		t.Fatal("two generated tokens are equal")
	}
}

func TestHashAgentToken(t *testing.T) {
	t.Parallel()

	const want = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
	if got := HashAgentToken("abc"); got != want {
		t.Fatalf("HashAgentToken(abc) = %q, want %q", got, want)
	}
}
