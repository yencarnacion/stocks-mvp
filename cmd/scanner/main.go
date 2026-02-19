package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"databento-scanner/internal/app"
)

func main() {
	var (
		configPath   = flag.String("config", "config.yaml", "Path to config.yaml")
		watchlist    = flag.String("watchlist", "watchlist.yaml", "Path to watchlist.yaml")
		baselinesCSV = flag.String("baselines", "", "Optional override path to baselines.csv")
		dotenvPath   = flag.String("dotenv", ".env", "Optional .env file path")
	)
	flag.Parse()

	level := slog.LevelInfo
	switch strings.ToLower(strings.TrimSpace(os.Getenv("LOG_LEVEL"))) {
	case "debug":
		level = slog.LevelDebug
	case "warn", "warning":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}))

	if err := app.LoadDotEnv(*dotenvPath); err != nil {
		log.Warn("failed loading .env", "path", *dotenvPath, "err", err)
	}
	if _, err := os.Stat(*configPath); err != nil {
		log.Error("config file is required and was not found", "path", *configPath)
		os.Exit(1)
	}

	cfg, err := app.LoadConfig(*configPath)
	if err != nil {
		log.Error("failed to load config", "err", err)
		os.Exit(1)
	}
	if *baselinesCSV != "" {
		cfg.Baselines.CSVPath = *baselinesCSV
	}

	wl, err := app.LoadWatchlist(*watchlist, cfg.Databento.BenchmarkSymbol)
	if err != nil {
		log.Error("failed to load watchlist", "err", err)
		os.Exit(1)
	}

	bl, err := app.LoadBaselinesCSV(cfg.Baselines.CSVPath)
	if err != nil {
		log.Warn("failed to load baselines.csv (using fallbacks)", "path", cfg.Baselines.CSVPath, "err", err)
		bl = map[string]app.Baseline{}
	}

	sc := app.NewScanner(log, cfg, wl, bl)
	srv := app.NewServer(log, cfg, sc, wl, bl)
	log.Info(
		"startup",
		"browser_url", browserURL(cfg.Server.Host, cfg.Server.Port),
		"health_url", fmt.Sprintf("%s/api/health", browserURL(cfg.Server.Host, cfg.Server.Port)),
		"timezone", cfg.Market.Timezone,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		sigCh := make(chan os.Signal, 2)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		cancel()
	}()

	go func() {
		if err := sc.Run(ctx); err != nil {
			log.Error("live scanner stopped with error", "err", err)
			cancel()
		}
	}()

	if err := srv.Run(ctx); err != nil {
		log.Error("server stopped with error", "err", err)
		os.Exit(1)
	}
}

func browserURL(host string, port int) string {
	h := strings.TrimSpace(host)
	if h == "" || h == "0.0.0.0" || h == "::" || h == "[::]" {
		h = "localhost"
	}
	return fmt.Sprintf("http://%s:%d", h, port)
}
