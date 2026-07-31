package auth

import (
	"strings"
	"testing"
)

func TestPasswordRoundTrip(t *testing.T) {
	t.Parallel()

	const password = "correct horse battery staple"

	encoded, err := HashPassword(password)
	if err != nil {
		t.Fatalf("HashPassword() error = %v", err)
	}
	if !strings.HasPrefix(encoded, "$argon2id$v=19$m=19456,t=2,p=1$") {
		t.Fatalf("HashPassword() = %q, unexpected PHC prefix", encoded)
	}

	ok, err := VerifyPassword(encoded, password)
	if err != nil {
		t.Fatalf("VerifyPassword(correct) error = %v", err)
	}
	if !ok {
		t.Fatal("VerifyPassword(correct) = false")
	}

	ok, err = VerifyPassword(encoded, "wrong password")
	if err != nil {
		t.Fatalf("VerifyPassword(wrong) error = %v", err)
	}
	if ok {
		t.Fatal("VerifyPassword(wrong) = true")
	}
}

func TestHashPasswordUsesUniqueSalt(t *testing.T) {
	t.Parallel()

	first, err := HashPassword("same password")
	if err != nil {
		t.Fatalf("first HashPassword() error = %v", err)
	}
	second, err := HashPassword("same password")
	if err != nil {
		t.Fatalf("second HashPassword() error = %v", err)
	}
	if first == second {
		t.Fatal("two hashes are equal; expected unique random salts")
	}
}

func TestVerifyPasswordRejectsMalformedHash(t *testing.T) {
	t.Parallel()

	// Salt decodes to 16 bytes, hash to 32 bytes where those fields need
	// to be valid so each case reaches the parameter being tested.
	const salt = "MDEyMzQ1Njc4OWFiY2RlZg"
	const key = "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY"

	tests := map[string]string{
		"empty":              "",
		"algorithm":          "$argon2i$v=19$m=19456,t=2,p=1$" + salt + "$" + key,
		"version":            "$argon2id$v=18$m=19456,t=2,p=1$" + salt + "$" + key,
		"parameter format":   "$argon2id$v=19$m=19456,t=2$" + salt + "$" + key,
		"zero parameter":     "$argon2id$v=19$m=0,t=2,p=1$" + salt + "$" + key,
		"excessive memory":   "$argon2id$v=19$m=999999,t=2,p=1$" + salt + "$" + key,
		"invalid salt":       "$argon2id$v=19$m=19456,t=2,p=1$!$" + key,
		"short decoded hash": "$argon2id$v=19$m=19456,t=2,p=1$" + salt + "$YWJj",
	}

	for name, encoded := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ok, err := VerifyPassword(encoded, "password")
			if err == nil {
				t.Fatalf("VerifyPassword() = (%v, nil), want error", ok)
			}
		})
	}
}
