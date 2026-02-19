package app

import (
	"bufio"
	"errors"
	"os"
	"strings"
)

// LoadDotEnv loads KEY=VALUE lines into the process environment.
// Existing env vars are not overwritten.
func LoadDotEnv(path string) error {
	if strings.TrimSpace(path) == "" {
		return nil
	}

	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer f.Close()

	s := bufio.NewScanner(f)
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "export ") {
			line = strings.TrimSpace(strings.TrimPrefix(line, "export "))
		}

		i := strings.IndexByte(line, '=')
		if i <= 0 {
			continue
		}

		key := strings.TrimSpace(line[:i])
		val := strings.TrimSpace(line[i+1:])
		if key == "" {
			continue
		}
		val = strings.Trim(val, "\"'")

		if _, exists := os.LookupEnv(key); exists {
			continue
		}
		_ = os.Setenv(key, val)
	}

	return s.Err()
}
