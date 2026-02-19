package app

import (
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

type watchlistFile struct {
	Watchlist []struct {
		Symbol string `yaml:"symbol"`
	} `yaml:"watchlist"`
}

// LoadWatchlist returns a set of symbols (uppercase). Ensures benchmark is present.
func LoadWatchlist(path string, benchmark string) (map[string]struct{}, error) {
	set := make(map[string]struct{})

	b, err := os.ReadFile(path)
	if err != nil {
		if benchmark != "" {
			set[strings.ToUpper(strings.TrimSpace(benchmark))] = struct{}{}
		}
		return set, nil
	}

	var wf watchlistFile
	if err := yaml.Unmarshal(b, &wf); err != nil {
		return nil, err
	}

	for _, item := range wf.Watchlist {
		s := strings.ToUpper(strings.TrimSpace(item.Symbol))
		if s == "" {
			continue
		}
		set[s] = struct{}{}
	}

	if benchmark != "" {
		set[strings.ToUpper(strings.TrimSpace(benchmark))] = struct{}{}
	}
	return set, nil
}
