// Command chatstress drives the use-case-6 (chat / messaging with retention)
// stress workload against a Redis Search on Disk (Flex) index, exposes a live
// web dashboard, and verifies the "no stale hits" correctness property.
//
// Usage:
//
//	chatstress run          -config config/smoke.yaml [-addr host:port] [-flush]
//	chatstress reload-check  -config config/smoke.yaml [-addr host:port]
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"chatstress/internal/config"
	"chatstress/internal/control"
	"chatstress/internal/metrics"
	"chatstress/internal/oracle"
	"chatstress/internal/redisx"
	"chatstress/internal/report"
	"chatstress/internal/web"
	"chatstress/internal/workload"
)

func main() {
	mode, args := parseMode(os.Args[1:])

	fs := flag.NewFlagSet("chatstress "+mode, flag.ExitOnError)
	configPath := fs.String("config", "", "path to YAML config")
	url := fs.String("url", "", "full Redis URL (redis://|rediss://); overrides -addr/-password/-tls. Prefer $CHATSTRESS_URL / a .env file to keep the password out of shell history")
	addr := fs.String("addr", "", "override Redis address (host:port)")
	password := fs.String("password", "", "override Redis password (cloud/auth)")
	username := fs.String("username", "", "override Redis ACL username (cloud)")
	useTLS := fs.Bool("tls", false, "connect over TLS (cloud endpoints)")
	webAddr := fs.String("web-addr", "", "override web dashboard address")
	durStr := fs.String("duration", "", "override run duration (e.g. 90s, 30m)")
	seed := fs.Int64("seed", 0, "override RNG seed (0 = use config)")
	flush := fs.Bool("flush", false, "FLUSHALL before starting (clean slate)")
	noWeb := fs.Bool("no-web", false, "disable the web dashboard")
	_ = fs.Parse(args)

	cfg, err := config.Load(*configPath)
	if err != nil {
		die("config: %v", err)
	}
	// Connection URL precedence: -url flag > $CHATSTRESS_URL > config `url`. A URL
	// (when present) supersedes addr/username/password/tls in redisx.New.
	if *url != "" {
		cfg.URL = *url
	} else if env := os.Getenv("CHATSTRESS_URL"); env != "" {
		cfg.URL = env
	}
	if cfg.URL != "" {
		// Redacted host:port for display/logs only — never print the URL itself.
		if a := redisx.AddrFromURL(cfg.URL); a != "" {
			cfg.Addr = a
		}
	}
	if *addr != "" {
		cfg.Addr = *addr
	}
	if *password != "" {
		cfg.Password = *password
	}
	if *username != "" {
		cfg.Username = *username
	}
	if *useTLS {
		cfg.TLS = true
	}
	if *webAddr != "" {
		cfg.Web.Addr = *webAddr
	}
	if *seed != 0 {
		cfg.Seed = *seed
	}
	if *noWeb {
		cfg.Web.Enabled = false
	}
	if *durStr != "" {
		d, perr := time.ParseDuration(*durStr)
		if perr != nil {
			die("bad -duration: %v", perr)
		}
		cfg.Duration = config.Duration(d)
	}

	// Size the pool for the max live query concurrency so scaling threads up from
	// the UI yields real concurrent connections rather than pool contention.
	pool := cfg.Ingest.Workers + cfg.Query.MaxWorkers + 32
	cli, err := redisx.New(redisx.Options{
		URL: cfg.URL, Addr: cfg.Addr, Username: cfg.Username, Password: cfg.Password,
		TLS: cfg.TLS, TLSSkipVerify: cfg.TLSSkipVerify, Index: cfg.Index, PoolSize: pool,
	})
	if err != nil {
		die("connect: %v", err)
	}
	defer cli.Close()

	// Signal-cancellable root context.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	switch mode {
	case "run":
		runMode(ctx, cfg, cli, *flush)
	case "reload-check":
		if err := reloadCheck(ctx, cfg, cli); err != nil {
			die("reload-check FAILED: %v", err)
		}
	default:
		die("unknown mode %q (want: run | reload-check)", mode)
	}
}

func runMode(ctx context.Context, cfg *config.Config, cli *redisx.Client, flush bool) {
	if flush {
		fmt.Println("FLUSHALL (clean slate)…")
		if err := cli.FlushAll(context.Background()); err != nil {
			die("flushall: %v", err)
		}
	}
	// Create the index BEFORE loading data (SKIPINITIALSCAN => no back-indexing).
	if err := cli.CreateIndex(context.Background(), cfg.Prefix); err != nil {
		die("create index: %v", err)
	}

	met := metrics.New()
	orc := oracle.New(256, cfg.Oracle.MaxTracked, cfg.Oracle.SampleRate, cfg.Oracle.GraceMs, cfg.Oracle.SkewMs)
	ctrl := control.New(cfg)
	rep := report.New(met, orc)

	if cfg.Web.Enabled {
		srv := web.New(cfg, cli, met, orc, ctrl)
		go func() {
			if err := srv.Start(ctx); err != nil {
				fmt.Fprintf(os.Stderr, "web server: %v\n", err)
			}
		}()
		fmt.Printf("dashboard: http://localhost%s\n", cfg.Web.Addr)
	}

	fmt.Printf("running for %s against %s (index %q)…\n", cfg.Duration.D(), cfg.Addr, cfg.Index)
	runner := workload.New(cfg, cli, orc, met, ctrl)

	rctx, cancel := context.WithTimeout(ctx, cfg.Duration.D())
	defer cancel()
	go printLoop(rctx, rep, cfg.Sample.Interval.D())
	runner.Run(rctx)

	summary, err := rep.Finalize(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "finalize: %v\n", err)
	}
	fmt.Print(summary.Text())
	fmt.Printf("artifacts: %s/summary.json, %s/metrics.csv\n", cfg.OutDir, cfg.OutDir)

	// If the run finished on its own (not via Ctrl-C) and the dashboard is up,
	// keep serving so the final state can be inspected.
	if cfg.Web.Enabled && ctx.Err() == nil {
		fmt.Println("workload complete — dashboard still live; press Ctrl-C to exit.")
		<-ctx.Done()
	}
}

func printLoop(ctx context.Context, rep *report.Reporter, every time.Duration) {
	if every <= 0 {
		every = 2 * time.Second
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			fmt.Println(rep.Line())
		}
	}
}

// parseMode extracts the subcommand (default "run") from the leading args.
func parseMode(argv []string) (string, []string) {
	if len(argv) > 0 {
		switch argv[0] {
		case "run", "reload-check":
			return argv[0], argv[1:]
		}
	}
	return "run", argv
}

func die(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "chatstress: "+format+"\n", a...)
	os.Exit(1)
}
