package main

import (
	"bytes"
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

//go:embed LICENSE web/*
var webAssets embed.FS

const (
	defaultListenAddr      = ":8080"
	defaultMihomoURL       = "http://127.0.0.1:9090"
	defaultPollInterval    = 5 * time.Second
	aggregateFlushInterval = 10 * time.Minute
	aggregateRetention     = 30 * 24 * time.Hour
	defaultAllowedOrigin   = "*"
)

var isContainerRuntime = func() bool {
	_, err := os.Stat("/.dockerenv")
	return err == nil
}

type config struct {
	ListenAddr   string
	MihomoURL    string
	MihomoSecret string
}

type trafficLog struct {
	Timestamp     int64    `json:"timestamp"`
	SourceIP      string   `json:"sourceIP"`
	Host          string   `json:"host"`
	DestinationIP string   `json:"destinationIP"`
	Process       string   `json:"process"`
	Outbound      string   `json:"outbound"`
	Chains        []string `json:"chains"`
	Upload        int64    `json:"upload"`
	Download      int64    `json:"download"`
}

type aggregatedData struct {
	Label    string `json:"label"`
	Upload   int64  `json:"upload"`
	Download int64  `json:"download"`
	Total    int64  `json:"total"`
	Count    int64  `json:"count"`
}

type trendPoint struct {
	Timestamp int64 `json:"timestamp"`
	Upload    int64 `json:"upload"`
	Download  int64 `json:"download"`
}

type connectionDetail struct {
	DestinationIP string   `json:"destinationIP"`
	SourceIP      string   `json:"sourceIP"`
	Process       string   `json:"process"`
	Outbound      string   `json:"outbound"`
	Chains        []string `json:"chains"`
	Upload        int64    `json:"upload"`
	Download      int64    `json:"download"`
	Total         int64    `json:"total"`
	Count         int64    `json:"count"`
}

type connection struct {
	ID       string   `json:"id"`
	Upload   int64    `json:"upload"`
	Download int64    `json:"download"`
	Chains   []string `json:"chains"`
	Metadata struct {
		SourceIP      string `json:"sourceIP"`
		Host          string `json:"host"`
		DestinationIP string `json:"destinationIP"`
		Process       string `json:"process"`
	} `json:"metadata"`
}

type connectionsResponse struct {
	Connections   []connection `json:"connections"`
	UploadTotal   int64        `json:"uploadTotal"`
	DownloadTotal int64        `json:"downloadTotal"`
}

type service struct {
	db                *sql.DB
	client            *http.Client
	cfg               config
	mu                sync.Mutex
	lastConnections   map[string]connection
	lastUploadTotal   int64
	lastDownloadTotal int64
	lastCleanup       time.Time
	lastVacuum        time.Time
	aggregateBuffer   map[string]*aggregatedEntry
}

type aggregatedEntry struct {
	BucketStart   int64
	BucketEnd     int64
	SourceIP      string
	Host          string
	DestinationIP string
	Process       string
	Outbound      string
	Chains        string
	Upload        int64
	Download      int64
	Count         int64
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	db, err := openDatabase(defaultDatabasePath())
	if err != nil {
		log.Fatalf("open database: %v", err)
	}
	defer db.Close()

	svc := &service{
		db:              db,
		client:          &http.Client{Timeout: 10 * time.Second},
		cfg:             cfg,
		lastConnections: make(map[string]connection),
		lastVacuum:      time.Now(),
		aggregateBuffer: make(map[string]*aggregatedEntry),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	collectorDone := make(chan struct{})
	go func() {
		defer close(collectorDone)
		svc.runCollector(ctx)
	}()

	server := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           svc.routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("traffic monitor listening on %s", cfg.ListenAddr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http server: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	cancel()
	select {
	case <-collectorDone:
	case <-time.After(5 * time.Second):
		log.Printf("collector shutdown timed out")
	}
	if err := svc.flushAggregateBuffer(); err != nil {
		log.Printf("flush aggregate buffer on shutdown: %v", err)
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("shutdown server: %v", err)
	}
}

func loadConfig() (config, error) {
	cfg := config{
		ListenAddr:   getenv("TRAFFIC_MONITOR_LISTEN", defaultListenAddr),
		MihomoURL:    strings.TrimRight(getenv("MIHOMO_URL", defaultMihomoURL), "/"),
		MihomoSecret: getenv("MIHOMO_SECRET", ""),
	}

	if cfg.MihomoURL == "" {
		return config{}, errors.New("MIHOMO_URL is required")
	}

	return cfg, nil
}

func defaultDatabasePath() string {
	if isContainerRuntime() {
		return "/data/traffic_monitor.db"
	}
	return "./data/traffic_monitor.db"
}

func openDatabase(path string) (*sql.DB, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}

	schema := `
	PRAGMA journal_mode=WAL;
	PRAGMA busy_timeout=5000;

	CREATE TABLE IF NOT EXISTS traffic_logs (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		timestamp INTEGER NOT NULL,
		source_ip TEXT NOT NULL,
		host TEXT NOT NULL,
		destination_ip TEXT NOT NULL DEFAULT '',
		process TEXT NOT NULL,
		outbound TEXT NOT NULL,
		chains TEXT NOT NULL DEFAULT '[]',
		upload INTEGER NOT NULL,
		download INTEGER NOT NULL
	);

	CREATE TABLE IF NOT EXISTS traffic_aggregated (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		bucket_start INTEGER NOT NULL,
		bucket_end INTEGER NOT NULL,
		source_ip TEXT NOT NULL,
		host TEXT NOT NULL,
		destination_ip TEXT NOT NULL DEFAULT '',
		process TEXT NOT NULL,
		outbound TEXT NOT NULL,
		chains TEXT NOT NULL DEFAULT '[]',
		upload INTEGER NOT NULL,
		download INTEGER NOT NULL,
		count INTEGER NOT NULL,
		UNIQUE(bucket_start, bucket_end, source_ip, host, destination_ip, process, outbound, chains)
	);

	CREATE INDEX IF NOT EXISTS idx_traffic_logs_timestamp ON traffic_logs(timestamp);
	CREATE INDEX IF NOT EXISTS idx_traffic_logs_source_ip ON traffic_logs(source_ip);
	CREATE INDEX IF NOT EXISTS idx_traffic_logs_host ON traffic_logs(host);
	CREATE INDEX IF NOT EXISTS idx_traffic_logs_process ON traffic_logs(process);
	CREATE INDEX IF NOT EXISTS idx_traffic_logs_outbound ON traffic_logs(outbound);

	CREATE INDEX IF NOT EXISTS idx_traffic_aggregated_bucket ON traffic_aggregated(bucket_start, bucket_end);
	CREATE INDEX IF NOT EXISTS idx_traffic_aggregated_source_ip ON traffic_aggregated(source_ip);
	CREATE INDEX IF NOT EXISTS idx_traffic_aggregated_host ON traffic_aggregated(host);
	CREATE INDEX IF NOT EXISTS idx_traffic_aggregated_process ON traffic_aggregated(process);
	CREATE INDEX IF NOT EXISTS idx_traffic_aggregated_outbound ON traffic_aggregated(outbound);
	`

	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, err
	}

	for _, stmt := range []string{
		`ALTER TABLE traffic_logs ADD COLUMN destination_ip TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE traffic_logs ADD COLUMN chains TEXT NOT NULL DEFAULT '[]'`,
	} {
		if _, err := db.Exec(stmt); err != nil && !strings.Contains(err.Error(), "duplicate column name") {
			db.Close()
			return nil, err
		}
	}

	currentBucketStart := (time.Now().UnixMilli() / 60000) * 60000
	if err := backfillAggregatedLogs(db, currentBucketStart); err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}

func backfillAggregatedLogs(db *sql.DB, beforeMS int64) error {
	if beforeMS <= 0 {
		return nil
	}

	var lastBucketEnd sql.NullInt64
	if err := db.QueryRow(`SELECT MAX(bucket_end) FROM traffic_aggregated`).Scan(&lastBucketEnd); err != nil {
		return err
	}

	startMS := int64(0)
	if lastBucketEnd.Valid {
		startMS = lastBucketEnd.Int64
	}
	if startMS >= beforeMS {
		return nil
	}

	_, err := db.Exec(`
		INSERT INTO traffic_aggregated
		(bucket_start, bucket_end, source_ip, host, destination_ip, process, outbound, chains, upload, download, count)
		SELECT ((timestamp / 60000) * 60000) AS bucket_start,
		       ((timestamp / 60000) * 60000) + 60000 AS bucket_end,
		       source_ip,
		       host,
		       destination_ip,
		       process,
		       outbound,
		       chains,
		       COALESCE(SUM(upload), 0) AS upload,
		       COALESCE(SUM(download), 0) AS download,
		       COUNT(*) AS count
		FROM traffic_logs
		WHERE timestamp >= ? AND timestamp < ?
		GROUP BY bucket_start, source_ip, host, destination_ip, process, outbound, chains
		ON CONFLICT(bucket_start, bucket_end, source_ip, host, destination_ip, process, outbound, chains)
		DO UPDATE SET
			upload = excluded.upload,
			download = excluded.download,
			count = excluded.count
	`, startMS, beforeMS)
	return err
}

func (s *service) runCollector(ctx context.Context) {
	s.collectOnce(ctx)

	ticker := time.NewTicker(defaultPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.collectOnce(ctx)
		}
	}
}

func (s *service) collectOnce(ctx context.Context) {
	resp, err := s.fetchConnections(ctx)
	if err != nil {
		log.Printf("poll Mihomo connections: %v", err)
		return
	}

	if err := s.processConnections(resp); err != nil {
		log.Printf("process connections: %v", err)
	}
}

func (s *service) fetchConnections(ctx context.Context) (*connectionsResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.cfg.MihomoURL+"/connections", nil)
	if err != nil {
		return nil, err
	}

	if s.cfg.MihomoSecret != "" {
		req.Header.Set("Authorization", "Bearer "+s.cfg.MihomoSecret)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %d", resp.StatusCode)
	}

	var payload connectionsResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}

	return &payload, nil
}

func (s *service) processConnections(payload *connectionsResponse) error {
	nowMS := time.Now().UnixMilli()

	s.mu.Lock()
	if payload.UploadTotal < s.lastUploadTotal || payload.DownloadTotal < s.lastDownloadTotal {
		log.Printf("detected Mihomo counter reset, clearing in-memory baselines")
		s.lastConnections = make(map[string]connection)
	}

	s.lastUploadTotal = payload.UploadTotal
	s.lastDownloadTotal = payload.DownloadTotal

	activeIDs := make(map[string]struct{}, len(payload.Connections))
	logs := make([]trafficLog, 0, len(payload.Connections))

	for _, conn := range payload.Connections {
		activeIDs[conn.ID] = struct{}{}

		prev, hasPrev := s.lastConnections[conn.ID]
		uploadDelta := conn.Upload
		downloadDelta := conn.Download

		if hasPrev {
			uploadDelta = conn.Upload - prev.Upload
			downloadDelta = conn.Download - prev.Download
		}

		if uploadDelta < 0 {
			uploadDelta = conn.Upload
		}
		if downloadDelta < 0 {
			downloadDelta = conn.Download
		}
		if uploadDelta == 0 && downloadDelta == 0 {
			s.lastConnections[conn.ID] = conn
			continue
		}

		logs = append(logs, trafficLog{
			Timestamp:     nowMS,
			SourceIP:      defaultString(conn.Metadata.SourceIP, "Inner"),
			Host:          defaultString(firstNonEmpty(conn.Metadata.Host, conn.Metadata.DestinationIP), "Unknown"),
			DestinationIP: strings.TrimSpace(conn.Metadata.DestinationIP),
			Process:       defaultString(conn.Metadata.Process, "Unknown"),
			Outbound:      outboundName(conn.Chains),
			Chains:        sanitizeChains(conn.Chains),
			Upload:        uploadDelta,
			Download:      downloadDelta,
		})

		s.lastConnections[conn.ID] = conn
	}

	for id := range s.lastConnections {
		if _, ok := activeIDs[id]; !ok {
			delete(s.lastConnections, id)
		}
	}
	s.mu.Unlock()

	if len(logs) > 0 {
		if err := s.addToAggregateBuffer(logs, nowMS); err != nil {
			return err
		}
	}

	if err := s.flushCompletedAggregateBuckets(nowMS); err != nil {
		log.Printf("flush aggregate buffer: %v", err)
	}

	if time.Since(s.lastCleanup) >= time.Hour {
		if err := s.cleanupOldLogs(nowMS); err != nil {
			log.Printf("cleanup old logs: %v", err)
		} else {
			s.lastCleanup = time.Now()
		}
	}

	return nil
}

func (s *service) insertLogs(logs []trafficLog) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(`
		INSERT INTO traffic_logs (timestamp, source_ip, host, destination_ip, process, outbound, chains, upload, download)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		tx.Rollback()
		return err
	}
	defer stmt.Close()

	for _, entry := range logs {
		chainsJSON, err := json.Marshal(sanitizeChains(entry.Chains))
		if err != nil {
			tx.Rollback()
			return err
		}

		if _, err := stmt.Exec(
			entry.Timestamp,
			entry.SourceIP,
			entry.Host,
			entry.DestinationIP,
			entry.Process,
			entry.Outbound,
			string(chainsJSON),
			entry.Upload,
			entry.Download,
		); err != nil {
			tx.Rollback()
			return err
		}
	}

	return tx.Commit()
}

func (s *service) cleanupOldLogs(nowMS int64) error {
	if _, err := s.db.Exec(`DELETE FROM traffic_logs`); err != nil {
		return err
	}

	aggCutoff := nowMS - int64(aggregateRetention/time.Millisecond)
	if _, err := s.db.Exec(`DELETE FROM traffic_aggregated WHERE bucket_end < ?`, aggCutoff); err != nil {
		return err
	}

	// 定期执行VACUUM（每周一次）
	if time.Since(s.lastVacuum) >= 7*24*time.Hour {
		if _, err := s.db.Exec(`VACUUM`); err != nil {
			log.Printf("VACUUM failed: %v", err)
		} else {
			s.lastVacuum = time.Now()
		}
	}

	return nil
}

func (s *service) addToAggregateBuffer(logs []trafficLog, nowMS int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	bucketStart := (nowMS / 60000) * 60000 // 1分钟桶
	bucketEnd := bucketStart + 60000

	for _, log := range logs {
		key := fmt.Sprintf("%d-%s-%s-%s-%s-%s-%s", bucketStart, log.SourceIP, log.Host, log.DestinationIP, log.Process, log.Outbound, strings.Join(log.Chains, ","))

		if entry, exists := s.aggregateBuffer[key]; exists {
			entry.Upload += log.Upload
			entry.Download += log.Download
			entry.Count++
		} else {
			chainsJSON, err := json.Marshal(sanitizeChains(log.Chains))
			if err != nil {
				return err
			}
			s.aggregateBuffer[key] = &aggregatedEntry{
				BucketStart:   bucketStart,
				BucketEnd:     bucketEnd,
				SourceIP:      log.SourceIP,
				Host:          log.Host,
				DestinationIP: log.DestinationIP,
				Process:       log.Process,
				Outbound:      log.Outbound,
				Chains:        string(chainsJSON),
				Upload:        log.Upload,
				Download:      log.Download,
				Count:         1,
			}
		}
	}

	return nil
}

func (s *service) flushCompletedAggregateBuckets(nowMS int64) error {
	currentBucketStart := (nowMS / int64(aggregateFlushInterval/time.Millisecond)) * int64(aggregateFlushInterval/time.Millisecond)
	return s.flushAggregateEntries(func(entry *aggregatedEntry) bool {
		return entry.BucketEnd <= currentBucketStart
	})
}

func (s *service) flushAggregateBuffer() error {
	return s.flushAggregateEntries(func(*aggregatedEntry) bool {
		return true
	})
}

func (s *service) flushAggregateEntries(shouldFlush func(*aggregatedEntry) bool) error {
	buffer := s.snapshotAggregateEntries(shouldFlush)
	if len(buffer) == 0 {
		return nil
	}

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(`
		INSERT INTO traffic_aggregated
		(bucket_start, bucket_end, source_ip, host, destination_ip, process, outbound, chains, upload, download, count)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(bucket_start, bucket_end, source_ip, host, destination_ip, process, outbound, chains)
		DO UPDATE SET
			upload = traffic_aggregated.upload + excluded.upload,
			download = traffic_aggregated.download + excluded.download,
			count = traffic_aggregated.count + excluded.count
	`)
	if err != nil {
		tx.Rollback()
		return err
	}
	defer stmt.Close()

	for _, entry := range buffer {
		if _, err := stmt.Exec(
			entry.BucketStart, entry.BucketEnd, entry.SourceIP, entry.Host, entry.DestinationIP,
			entry.Process, entry.Outbound, entry.Chains, entry.Upload, entry.Download, entry.Count,
		); err != nil {
			tx.Rollback()
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for key := range buffer {
		delete(s.aggregateBuffer, key)
	}
	return nil
}

func (s *service) snapshotAggregateEntries(shouldFlush func(*aggregatedEntry) bool) map[string]aggregatedEntry {
	s.mu.Lock()
	defer s.mu.Unlock()

	buffer := make(map[string]aggregatedEntry)
	for key, entry := range s.aggregateBuffer {
		if shouldFlush(entry) {
			buffer[key] = *entry
		}
	}
	return buffer
}

func (s *service) routes() http.Handler {
	mux := http.NewServeMux()
	staticFS, err := fs.Sub(webAssets, "web")
	if err != nil {
		panic(err)
	}
	fileServer := http.FileServer(http.FS(staticFS))
	mux.HandleFunc("/LICENSE", s.handleLicense)
	mux.Handle("/", fileServer)
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/api/traffic/aggregate", s.handleAggregate)
	mux.HandleFunc("/api/traffic/substats", s.handleSubstats)
	mux.HandleFunc("/api/traffic/proxy-stats", s.handleProxyStats)
	mux.HandleFunc("/api/traffic/devices-by-host", s.handleDevicesByHost)
	mux.HandleFunc("/api/traffic/devices-by-proxy-host", s.handleDevicesByProxyHost)
	mux.HandleFunc("/api/traffic/details", s.handleConnectionDetails)
	mux.HandleFunc("/api/traffic/trend", s.handleTrend)
	mux.HandleFunc("/api/traffic/logs", s.handleLogs)
	return s.withCORS(mux)
}

func (s *service) withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", defaultAllowedOrigin)
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		w.Header().Set("Access-Control-Allow-Methods", "GET, DELETE, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *service) handleLicense(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		writeMethodNotAllowed(w)
		return
	}

	content, err := webAssets.ReadFile("LICENSE")
	if err != nil {
		http.Error(w, "license unavailable", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	http.ServeContent(w, r, "LICENSE", time.Time{}, bytes.NewReader(content))
}

func (s *service) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *service) handleAggregate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w)
		return
	}

	dimension := r.URL.Query().Get("dimension")
	start, end, err := parseTimeRange(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	data, err := s.queryAggregate(dimension, start, end)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	writeJSON(w, http.StatusOK, data)
}

func (s *service) handleSubstats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w)
		return
	}

	dimension := r.URL.Query().Get("dimension")
	label := r.URL.Query().Get("label")
	start, end, err := parseTimeRange(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if label == "" {
		writeError(w, http.StatusBadRequest, errors.New("label is required"))
		return
	}

	data, err := s.querySubstats(dimension, label, start, end)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, data)
}

func (s *service) handleProxyStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w)
		return
	}

	dimension := r.URL.Query().Get("dimension")
	parentLabel := r.URL.Query().Get("parentLabel")
	host := r.URL.Query().Get("host")
	start, end, err := parseTimeRange(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if parentLabel == "" || host == "" {
		writeError(w, http.StatusBadRequest, errors.New("parentLabel and host are required"))
		return
	}

	data, err := s.queryProxyStats(dimension, parentLabel, host, start, end)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, data)
}

func (s *service) handleDevicesByHost(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w)
		return
	}

	host := r.URL.Query().Get("host")
	start, end, err := parseTimeRange(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if host == "" {
		writeError(w, http.StatusBadRequest, errors.New("host is required"))
		return
	}

	data, err := s.queryByFilters("source_ip", "host = ?", []any{host}, start, end)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, data)
}

func (s *service) handleDevicesByProxyHost(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w)
		return
	}

	proxy := r.URL.Query().Get("proxy")
	host := r.URL.Query().Get("host")
	start, end, err := parseTimeRange(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if proxy == "" || host == "" {
		writeError(w, http.StatusBadRequest, errors.New("proxy and host are required"))
		return
	}

	data, err := s.queryByFilters(
		"source_ip",
		"outbound = ? AND host = ?",
		[]any{proxy, host},
		start,
		end,
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, data)
}

func (s *service) handleTrend(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w)
		return
	}

	start, end, err := parseTimeRange(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	bucket := parseInt64(r.URL.Query().Get("bucket"), 60000)
	if bucket <= 0 {
		writeError(w, http.StatusBadRequest, errors.New("bucket must be positive"))
		return
	}

	data, err := s.queryTrend(start, end, bucket)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, data)
}

func (s *service) handleConnectionDetails(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w)
		return
	}

	dimension := r.URL.Query().Get("dimension")
	primary := r.URL.Query().Get("primary")
	secondary := r.URL.Query().Get("secondary")
	start, end, err := parseTimeRange(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if primary == "" || secondary == "" {
		writeError(w, http.StatusBadRequest, errors.New("primary and secondary are required"))
		return
	}

	data, err := s.queryConnectionDetails(dimension, primary, secondary, start, end)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, data)
}

func (s *service) handleLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		writeMethodNotAllowed(w)
		return
	}

	tx, err := s.db.Begin()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	if _, err := tx.Exec(`DELETE FROM traffic_logs`); err != nil {
		tx.Rollback()
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if _, err := tx.Exec(`DELETE FROM traffic_aggregated`); err != nil {
		tx.Rollback()
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if err := tx.Commit(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	s.mu.Lock()
	s.aggregateBuffer = make(map[string]*aggregatedEntry)
	s.mu.Unlock()

	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *service) queryAggregate(dimension string, start, end int64) ([]aggregatedData, error) {
	column, err := dimensionColumn(dimension)
	if err != nil {
		return nil, err
	}
	return s.queryByFilters(column, "", nil, start, end)
}

func (s *service) querySubstats(dimension, label string, start, end int64) ([]aggregatedData, error) {
	column, err := dimensionColumn(dimension)
	if err != nil {
		return nil, err
	}
	if column == "host" {
		return nil, errors.New("host is not supported for substats")
	}
	return s.queryByFilters("host", column+" = ?", []any{label}, start, end)
}

func (s *service) queryProxyStats(dimension, parentLabel, host string, start, end int64) ([]aggregatedData, error) {
	column, err := dimensionColumn(dimension)
	if err != nil {
		return nil, err
	}
	if column == "host" {
		return nil, errors.New("host is not supported for proxy stats")
	}
	return s.queryByFilters("outbound", column+" = ? AND host = ?", []any{parentLabel, host}, start, end)
}

func (s *service) queryByFilters(groupColumn, extraFilter string, extraArgs []any, start, end int64) ([]aggregatedData, error) {
	merged := make(map[string]*aggregatedData)

	items, err := s.queryByFiltersFromAggregates(groupColumn, extraFilter, extraArgs, start, end)
	if err != nil {
		return nil, err
	}
	mergeAggregatedDataRows(merged, items)

	mergeAggregatedDataRows(merged, s.queryByFiltersFromBuffer(groupColumn, extraFilter, extraArgs, start, end))

	return sortedAggregatedDataRows(merged), nil
}

func (s *service) queryConnectionDetails(dimension, primary, secondary string, start, end int64) ([]connectionDetail, error) {
	filter, args, err := detailFilter(dimension, primary, secondary)
	if err != nil {
		return nil, err
	}

	rows, err := s.db.Query(`
		SELECT destination_ip,
		       source_ip,
		       process,
		       outbound,
		       chains,
		       COALESCE(SUM(upload), 0) AS upload,
		       COALESCE(SUM(download), 0) AS download,
		       COALESCE(SUM(upload + download), 0) AS total,
		       COALESCE(SUM(count), 0) AS count
		FROM traffic_aggregated
		WHERE bucket_end > ? AND bucket_start <= ?
		  AND `+filter+`
		GROUP BY destination_ip, source_ip, process, outbound, chains
	`, append([]any{start, end}, args...)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	merged := make(map[string]*connectionDetail)
	for rows.Next() {
		var (
			item      connectionDetail
			chainsRaw string
		)
		if err := rows.Scan(
			&item.DestinationIP,
			&item.SourceIP,
			&item.Process,
			&item.Outbound,
			&chainsRaw,
			&item.Upload,
			&item.Download,
			&item.Total,
			&item.Count,
		); err != nil {
			return nil, err
		}
		item.Chains = parseChains(chainsRaw)
		mergeConnectionDetailRows(merged, []connectionDetail{item})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	mergeConnectionDetailRows(merged, s.queryConnectionDetailsFromBuffer(filter, args, start, end))
	return sortedConnectionDetailRows(merged), nil
}

func (s *service) queryTrend(start, end, bucket int64) ([]trendPoint, error) {
	buckets := make(map[int64]trendPoint)

	items, err := s.queryTrendFromAggregates(start, end, bucket)
	if err != nil {
		return nil, err
	}
	mergeTrendPoints(buckets, items)
	mergeTrendPoints(buckets, s.queryTrendFromBuffer(start, end, bucket))

	points := make([]trendPoint, 0, (end-start)/bucket+1)
	for t := start; t <= end; t += bucket {
		key := (t / bucket) * bucket
		if point, ok := buckets[key]; ok {
			points = append(points, point)
			continue
		}
		points = append(points, trendPoint{Timestamp: key})
	}
	return points, nil
}

func (s *service) queryByFiltersFromRaw(groupColumn, extraFilter string, extraArgs []any, start, end int64) ([]aggregatedData, error) {
	return s.queryByFiltersFromTable(
		"traffic_logs",
		"timestamp BETWEEN ? AND ?",
		[]any{start, end},
		groupColumn,
		extraFilter,
		extraArgs,
		"COUNT(*)",
	)
}

func (s *service) queryByFiltersFromAggregates(groupColumn, extraFilter string, extraArgs []any, start, endExclusive int64) ([]aggregatedData, error) {
	return s.queryByFiltersFromTable(
		"traffic_aggregated",
		"bucket_end > ? AND bucket_start <= ?",
		[]any{start, endExclusive},
		groupColumn,
		extraFilter,
		extraArgs,
		"COALESCE(SUM(count), 0)",
	)
}

func (s *service) queryByFiltersFromBuffer(groupColumn, extraFilter string, extraArgs []any, start, end int64) []aggregatedData {
	items := s.snapshotAggregateEntries(func(entry *aggregatedEntry) bool {
		return aggregateEntryOverlapsRange(*entry, start, end)
	})

	merged := make(map[string]*aggregatedData)
	for _, entry := range items {
		if !matchesAggregateEntryFilters(entry, extraFilter, extraArgs) {
			continue
		}
		label := aggregateEntryFieldValue(entry, groupColumn)
		if label == "" {
			continue
		}
		row, ok := merged[label]
		if !ok {
			row = &aggregatedData{Label: label}
			merged[label] = row
		}
		row.Upload += entry.Upload
		row.Download += entry.Download
		row.Total += entry.Upload + entry.Download
		row.Count += entry.Count
	}

	return sortedAggregatedDataRows(merged)
}

func (s *service) queryByFiltersFromTable(table, timeFilter string, timeArgs []any, groupColumn, extraFilter string, extraArgs []any, countExpr string) ([]aggregatedData, error) {
	base := `
		SELECT ` + groupColumn + ` AS label,
		       COALESCE(SUM(upload), 0) AS upload,
		       COALESCE(SUM(download), 0) AS download,
		       COALESCE(SUM(upload + download), 0) AS total,
		       ` + countExpr + ` AS count
		FROM ` + table + `
		WHERE ` + timeFilter

	args := append([]any{}, timeArgs...)
	if extraFilter != "" {
		base += " AND " + extraFilter
		args = append(args, extraArgs...)
	}
	base += `
		GROUP BY ` + groupColumn + `
		ORDER BY total DESC, label ASC
	`

	rows, err := s.db.Query(base, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]aggregatedData, 0)
	for rows.Next() {
		var item aggregatedData
		if err := rows.Scan(&item.Label, &item.Upload, &item.Download, &item.Total, &item.Count); err != nil {
			return nil, err
		}
		results = append(results, item)
	}
	return results, rows.Err()
}

func (s *service) queryTrendFromRaw(start, end, bucket int64) ([]trendPoint, error) {
	rows, err := s.db.Query(`
		SELECT ((timestamp / ?) * ?) AS bucket_start,
		       COALESCE(SUM(upload), 0) AS upload,
		       COALESCE(SUM(download), 0) AS download
		FROM traffic_logs
		WHERE timestamp BETWEEN ? AND ?
		GROUP BY bucket_start
		ORDER BY bucket_start ASC
	`, bucket, bucket, start, end)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]trendPoint, 0)
	for rows.Next() {
		var point trendPoint
		if err := rows.Scan(&point.Timestamp, &point.Upload, &point.Download); err != nil {
			return nil, err
		}
		results = append(results, point)
	}
	return results, rows.Err()
}

func (s *service) queryTrendFromAggregates(start, endExclusive, bucket int64) ([]trendPoint, error) {
	rows, err := s.db.Query(`
		SELECT ((bucket_start / ?) * ?) AS bucket_start,
		       COALESCE(SUM(upload), 0) AS upload,
		       COALESCE(SUM(download), 0) AS download
		FROM traffic_aggregated
		WHERE bucket_end > ? AND bucket_start <= ?
		GROUP BY bucket_start
		ORDER BY bucket_start ASC
	`, bucket, bucket, start, endExclusive)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]trendPoint, 0)
	for rows.Next() {
		var point trendPoint
		if err := rows.Scan(&point.Timestamp, &point.Upload, &point.Download); err != nil {
			return nil, err
		}
		results = append(results, point)
	}
	return results, rows.Err()
}

func (s *service) queryTrendFromBuffer(start, end, bucket int64) []trendPoint {
	items := s.snapshotAggregateEntries(func(entry *aggregatedEntry) bool {
		return aggregateEntryOverlapsRange(*entry, start, end)
	})

	merged := make(map[int64]trendPoint)
	for _, entry := range items {
		key := (entry.BucketStart / bucket) * bucket
		point := merged[key]
		point.Timestamp = key
		point.Upload += entry.Upload
		point.Download += entry.Download
		merged[key] = point
	}

	results := make([]trendPoint, 0, len(merged))
	for _, item := range merged {
		results = append(results, item)
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].Timestamp < results[j].Timestamp
	})
	return results
}

func (s *service) queryConnectionDetailsFromBuffer(filter string, args []any, start, end int64) []connectionDetail {
	items := s.snapshotAggregateEntries(func(entry *aggregatedEntry) bool {
		return aggregateEntryOverlapsRange(*entry, start, end)
	})

	merged := make(map[string]*connectionDetail)
	for _, entry := range items {
		if !matchesAggregateEntryFilters(entry, filter, args) {
			continue
		}
		item := connectionDetail{
			DestinationIP: entry.DestinationIP,
			SourceIP:      entry.SourceIP,
			Process:       entry.Process,
			Outbound:      entry.Outbound,
			Chains:        parseChains(entry.Chains),
			Upload:        entry.Upload,
			Download:      entry.Download,
			Total:         entry.Upload + entry.Download,
			Count:         entry.Count,
		}
		mergeConnectionDetailRows(merged, []connectionDetail{item})
	}

	return sortedConnectionDetailRows(merged)
}

func aggregateEntryOverlapsRange(entry aggregatedEntry, start, end int64) bool {
	return entry.BucketEnd > start && entry.BucketStart <= end
}

func matchesAggregateEntryFilters(entry aggregatedEntry, filter string, args []any) bool {
	if filter == "" {
		return true
	}

	clauses := strings.Split(filter, " AND ")
	if len(clauses) != len(args) {
		return false
	}

	for i, clause := range clauses {
		column := strings.TrimSpace(strings.TrimSuffix(clause, "= ?"))
		column = strings.TrimSpace(strings.TrimSuffix(column, " = ?"))
		if column == "" {
			return false
		}
		if aggregateEntryFieldValue(entry, column) != fmt.Sprint(args[i]) {
			return false
		}
	}

	return true
}

func aggregateEntryFieldValue(entry aggregatedEntry, column string) string {
	switch column {
	case "source_ip":
		return entry.SourceIP
	case "host":
		return entry.Host
	case "destination_ip":
		return entry.DestinationIP
	case "process":
		return entry.Process
	case "outbound":
		return entry.Outbound
	case "chains":
		return entry.Chains
	default:
		return ""
	}
}

func fullMinuteBucketRange(start, end int64) (int64, int64) {
	aggregateStart := ((start + 60000 - 1) / 60000) * 60000
	aggregateEndExclusive := ((end + 1) / 60000) * 60000
	if aggregateStart < 0 {
		aggregateStart = 0
	}
	if aggregateEndExclusive < 0 {
		aggregateEndExclusive = 0
	}
	if aggregateStart > aggregateEndExclusive {
		return aggregateStart, aggregateStart
	}
	return aggregateStart, aggregateEndExclusive
}

func mergeAggregatedDataRows(target map[string]*aggregatedData, items []aggregatedData) {
	for _, item := range items {
		existing, ok := target[item.Label]
		if !ok {
			copyItem := item
			target[item.Label] = &copyItem
			continue
		}
		existing.Upload += item.Upload
		existing.Download += item.Download
		existing.Total += item.Total
		existing.Count += item.Count
	}
}

func sortedAggregatedDataRows(items map[string]*aggregatedData) []aggregatedData {
	results := make([]aggregatedData, 0, len(items))
	for _, item := range items {
		results = append(results, *item)
	}
	sort.Slice(results, func(i, j int) bool {
		if results[i].Total == results[j].Total {
			return results[i].Label < results[j].Label
		}
		return results[i].Total > results[j].Total
	})
	return results
}

func mergeConnectionDetailRows(target map[string]*connectionDetail, items []connectionDetail) {
	for _, item := range items {
		key := connectionDetailKey(item)
		existing, ok := target[key]
		if !ok {
			copyItem := item
			target[key] = &copyItem
			continue
		}
		existing.Upload += item.Upload
		existing.Download += item.Download
		existing.Total += item.Total
		existing.Count += item.Count
	}
}

func sortedConnectionDetailRows(items map[string]*connectionDetail) []connectionDetail {
	results := make([]connectionDetail, 0, len(items))
	for _, item := range items {
		results = append(results, *item)
	}
	sort.Slice(results, func(i, j int) bool {
		if results[i].Total == results[j].Total {
			if results[i].DestinationIP == results[j].DestinationIP {
				if results[i].SourceIP == results[j].SourceIP {
					return results[i].Process < results[j].Process
				}
				return results[i].SourceIP < results[j].SourceIP
			}
			return results[i].DestinationIP < results[j].DestinationIP
		}
		return results[i].Total > results[j].Total
	})
	return results
}

func connectionDetailKey(item connectionDetail) string {
	return strings.Join([]string{
		item.DestinationIP,
		item.SourceIP,
		item.Process,
		item.Outbound,
		strings.Join(item.Chains, "\x1f"),
	}, "\x00")
}

func mergeTrendPoints(target map[int64]trendPoint, items []trendPoint) {
	for _, item := range items {
		existing := target[item.Timestamp]
		existing.Timestamp = item.Timestamp
		existing.Upload += item.Upload
		existing.Download += item.Download
		target[item.Timestamp] = existing
	}
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func parseTimeRange(r *http.Request) (int64, int64, error) {
	start := parseInt64(r.URL.Query().Get("start"), 0)
	end := parseInt64(r.URL.Query().Get("end"), 0)
	if start <= 0 || end <= 0 {
		return 0, 0, errors.New("start and end are required")
	}
	if end < start {
		return 0, 0, errors.New("end must be greater than or equal to start")
	}
	return start, end, nil
}

func dimensionColumn(dimension string) (string, error) {
	switch dimension {
	case "sourceIP":
		return "source_ip", nil
	case "host":
		return "host", nil
	case "process":
		return "process", nil
	case "outbound":
		return "outbound", nil
	default:
		return "", fmt.Errorf("unsupported dimension %q", dimension)
	}
}

func detailFilter(dimension, primary, secondary string) (string, []any, error) {
	switch dimension {
	case "sourceIP":
		return "source_ip = ? AND host = ?", []any{primary, secondary}, nil
	case "host":
		return "host = ? AND source_ip = ?", []any{primary, secondary}, nil
	case "outbound":
		return "outbound = ? AND host = ?", []any{primary, secondary}, nil
	case "process":
		return "process = ? AND host = ?", []any{primary, secondary}, nil
	default:
		return "", nil, fmt.Errorf("unsupported dimension %q", dimension)
	}
}

func outboundName(chains []string) string {
	if len(chains) == 0 || chains[0] == "" {
		return "DIRECT"
	}
	return chains[0]
}

func sanitizeChains(chains []string) []string {
	if len(chains) == 0 {
		return []string{"DIRECT"}
	}

	cleaned := make([]string, 0, len(chains))
	for _, chain := range chains {
		chain = strings.TrimSpace(chain)
		if chain != "" {
			cleaned = append(cleaned, chain)
		}
	}
	if len(cleaned) == 0 {
		return []string{"DIRECT"}
	}
	return cleaned
}

func parseChains(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return []string{"DIRECT"}
	}

	var chains []string
	if err := json.Unmarshal([]byte(raw), &chains); err != nil {
		return []string{raw}
	}
	return sanitizeChains(chains)
}

func defaultString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func getenv(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func getenvInt(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func parseInt64(value string, fallback int64) int64 {
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return fallback
	}
	return parsed
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

func writeMethodNotAllowed(w http.ResponseWriter) {
	writeError(w, http.StatusMethodNotAllowed, errors.New("method not allowed"))
}
