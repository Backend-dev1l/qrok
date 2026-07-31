package auth

import "testing"

func TestIsDevToken(t *testing.T) {
	t.Parallel()

	if !IsDevToken("qrok_dev_abc") {
		t.Fatal("expected dev token")
	}
	if IsDevToken("qrok_agt_abc") {
		t.Fatal("agent token must not match")
	}
	if IsDevToken("qrok_dev") {
		t.Fatal("prefix only is not a token")
	}
}

func TestNormalizeUserCode(t *testing.T) {
	t.Parallel()

	tests := map[string]string{
		"abcd-efgh":   "ABCD-EFGH",
		" ABCD EFGH ": "ABCD-EFGH",
		"ABCD-EFGH":   "ABCD-EFGH",
	}
	for in, want := range tests {
		if got := NormalizeUserCode(in); got != want {
			t.Fatalf("NormalizeUserCode(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestGenerateDevToken(t *testing.T) {
	t.Parallel()

	plaintext, hash, err := GenerateDevToken()
	if err != nil {
		t.Fatalf("GenerateDevToken() error = %v", err)
	}
	if !IsDevToken(plaintext) {
		t.Fatalf("token = %q", plaintext)
	}
	if hash != HashAgentToken(plaintext) {
		t.Fatal("hash mismatch")
	}
}
