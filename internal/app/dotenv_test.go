package app

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadDotEnvParsesValuesAndDoesNotOverrideExisting(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".env")
	content := `# comment
TEST_DOTENV_FOO=bar
export TEST_DOTENV_BAR = "baz"
TEST_DOTENV_EXISTING=override
TEST_DOTENV_QUOTED='trimmed'
TEST_DOTENV_SPACED=  value with spaces
INVALID_LINE
=missing_key
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write .env: %v", err)
	}

	keys := []string{
		"TEST_DOTENV_FOO",
		"TEST_DOTENV_BAR",
		"TEST_DOTENV_EXISTING",
		"TEST_DOTENV_QUOTED",
		"TEST_DOTENV_SPACED",
	}
	type envState struct {
		val string
		ok  bool
	}
	old := make(map[string]envState, len(keys))
	for _, k := range keys {
		v, ok := os.LookupEnv(k)
		old[k] = envState{val: v, ok: ok}
		_ = os.Unsetenv(k)
	}
	t.Cleanup(func() {
		for _, k := range keys {
			state := old[k]
			if state.ok {
				_ = os.Setenv(k, state.val)
				continue
			}
			_ = os.Unsetenv(k)
		}
	})

	t.Setenv("TEST_DOTENV_EXISTING", "keep")

	if err := LoadDotEnv(path); err != nil {
		t.Fatalf("load dotenv: %v", err)
	}

	if got := os.Getenv("TEST_DOTENV_FOO"); got != "bar" {
		t.Fatalf("expected TEST_DOTENV_FOO=bar, got %q", got)
	}
	if got := os.Getenv("TEST_DOTENV_BAR"); got != "baz" {
		t.Fatalf("expected TEST_DOTENV_BAR=baz, got %q", got)
	}
	if got := os.Getenv("TEST_DOTENV_QUOTED"); got != "trimmed" {
		t.Fatalf("expected TEST_DOTENV_QUOTED=trimmed, got %q", got)
	}
	if got := os.Getenv("TEST_DOTENV_SPACED"); got != "value with spaces" {
		t.Fatalf("expected TEST_DOTENV_SPACED=value with spaces, got %q", got)
	}
	if got := os.Getenv("TEST_DOTENV_EXISTING"); got != "keep" {
		t.Fatalf("expected existing env var to be preserved, got %q", got)
	}
}

func TestLoadDotEnvBlankPathIsNoop(t *testing.T) {
	if err := LoadDotEnv("   "); err != nil {
		t.Fatalf("expected nil error for blank path, got %v", err)
	}
}

func TestLoadDotEnvMissingFileIsNoop(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.env")
	if err := LoadDotEnv(path); err != nil {
		t.Fatalf("expected nil error for missing file, got %v", err)
	}
}
