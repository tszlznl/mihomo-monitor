package main

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestQueryAggregate(t *testing.T) {
	svc := newTestService(t)

	insertTestAggregates(t, svc.db, []aggregatedEntry{
		{BucketStart: 0, BucketEnd: 60_000, SourceIP: "192.168.1.2", Host: "a.com", Process: "chrome", Outbound: "NodeA", Chains: `["NodeA"]`, Upload: 100, Download: 200, Count: 1},
		{BucketStart: 60_000, BucketEnd: 120_000, SourceIP: "192.168.1.2", Host: "b.com", Process: "chrome", Outbound: "NodeA", Chains: `["NodeA"]`, Upload: 50, Download: 20, Count: 1},
		{BucketStart: 60_000, BucketEnd: 120_000, SourceIP: "192.168.1.3", Host: "a.com", Process: "curl", Outbound: "DIRECT", Chains: `["DIRECT"]`, Upload: 10, Download: 30, Count: 1},
	})

	got, err := svc.queryAggregate("sourceIP", 1, 120_000)
	if err != nil {
		t.Fatalf("queryAggregate: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(got))
	}
	if got[0].Label != "192.168.1.2" || got[0].Upload != 150 || got[0].Download != 220 || got[0].Total != 370 {
		t.Fatalf("unexpected first row: %+v", got[0])
	}
}

func TestQueryTrendFillsEmptyBuckets(t *testing.T) {
	svc := newTestService(t)

	insertTestAggregates(t, svc.db, []aggregatedEntry{
		{BucketStart: 0, BucketEnd: 60_000, SourceIP: "192.168.1.2", Host: "a.com", Process: "chrome", Outbound: "NodeA", Chains: `["NodeA"]`, Upload: 100, Download: 200, Count: 1},
		{BucketStart: 120_000, BucketEnd: 180_000, SourceIP: "192.168.1.2", Host: "b.com", Process: "chrome", Outbound: "NodeA", Chains: `["NodeA"]`, Upload: 50, Download: 20, Count: 1},
	})

	got, err := svc.queryTrend(0, 180_000, 60_000)
	if err != nil {
		t.Fatalf("queryTrend: %v", err)
	}

	if len(got) != 4 {
		t.Fatalf("expected 4 buckets, got %d", len(got))
	}
	if got[1].Timestamp != 60_000 || got[1].Upload != 0 || got[1].Download != 0 {
		t.Fatalf("expected empty middle bucket, got %+v", got[1])
	}
	if got[2].Timestamp != 120_000 || got[2].Upload != 50 || got[2].Download != 20 {
		t.Fatalf("unexpected populated bucket: %+v", got[2])
	}
}

func TestOpenDatabaseAddsDetailColumns(t *testing.T) {
	svc := newTestService(t)

	rows, err := svc.db.Query(`PRAGMA table_info(traffic_logs)`)
	if err != nil {
		t.Fatalf("table_info: %v", err)
	}
	defer rows.Close()

	columns := map[string]bool{}
	for rows.Next() {
		var (
			cid        int
			name       string
			typeName   string
			notNull    int
			defaultV   any
			primaryKey int
		)
		if err := rows.Scan(&cid, &name, &typeName, &notNull, &defaultV, &primaryKey); err != nil {
			t.Fatalf("scan table_info: %v", err)
		}
		columns[name] = true
	}

	for _, name := range []string{"destination_ip", "chains"} {
		if !columns[name] {
			t.Fatalf("expected column %q to exist", name)
		}
	}
}

func TestLoadConfigDefaultsRetentionPolicy(t *testing.T) {
	for _, key := range []string{
		"TRAFFIC_MONITOR_LISTEN",
		"MIHOMO_URL",
		"MIHOMO_SECRET",
		"TRAFFIC_MONITOR_DB",
		"TRAFFIC_MONITOR_POLL_INTERVAL_MS",
		"TRAFFIC_MONITOR_RETENTION_DAYS",
		"TRAFFIC_MONITOR_AGG_RETENTION_DAYS",
		"TRAFFIC_MONITOR_ALLOWED_ORIGIN",
	} {
		if err := os.Unsetenv(key); err != nil {
			t.Fatalf("unsetenv %s: %v", key, err)
		}
	}

	if err := os.Setenv("CLASH_API", "http://10.0.0.2:9090"); err != nil {
		t.Fatalf("setenv CLASH_API: %v", err)
	}
	if err := os.Setenv("CLASH_SECRET", "legacy-secret"); err != nil {
		t.Fatalf("setenv CLASH_SECRET: %v", err)
	}
	if err := os.Setenv("TRAFFIC_MONITOR_DB", "/tmp/override.db"); err != nil {
		t.Fatalf("setenv TRAFFIC_MONITOR_DB: %v", err)
	}
	if err := os.Setenv("TRAFFIC_MONITOR_RETENTION_DAYS", "90"); err != nil {
		t.Fatalf("setenv TRAFFIC_MONITOR_RETENTION_DAYS: %v", err)
	}
	if err := os.Setenv("TRAFFIC_MONITOR_AGG_RETENTION_DAYS", "120"); err != nil {
		t.Fatalf("setenv TRAFFIC_MONITOR_AGG_RETENTION_DAYS: %v", err)
	}

	cfg, err := loadConfig()
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}

	if cfg.ListenAddr != ":8080" {
		t.Fatalf("expected listen addr default to be :8080, got %q", cfg.ListenAddr)
	}
	if cfg.MihomoURL != "http://127.0.0.1:9090" {
		t.Fatalf("expected mihomo url default to be http://127.0.0.1:9090, got %q", cfg.MihomoURL)
	}
	if cfg.MihomoSecret != "" {
		t.Fatalf("expected mihomo secret default to be empty, got %q", cfg.MihomoSecret)
	}
	original := isContainerRuntime
	isContainerRuntime = func() bool { return false }
	t.Cleanup(func() {
		isContainerRuntime = original
	})
	if defaultDatabasePath() != "./data/traffic_monitor.db" {
		t.Fatalf("expected local default database path to be ./data/traffic_monitor.db, got %q", defaultDatabasePath())
	}
}

func TestDefaultDatabasePathUsesContainerDataDir(t *testing.T) {
	original := isContainerRuntime
	isContainerRuntime = func() bool { return true }
	t.Cleanup(func() {
		isContainerRuntime = original
	})

	if defaultDatabasePath() != "/data/traffic_monitor.db" {
		t.Fatalf("expected container default database path to be /data/traffic_monitor.db, got %q", defaultDatabasePath())
	}
}

func TestOpenDatabaseCreatesParentDirectory(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "nested", "traffic_monitor.db")

	db, err := openDatabase(dbPath)
	if err != nil {
		t.Fatalf("openDatabase: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	if _, err := os.Stat(filepath.Dir(dbPath)); err != nil {
		t.Fatalf("expected parent dir to exist: %v", err)
	}
}

func TestCleanupOldLogsKeepsThirtyDaysOfAggregates(t *testing.T) {
	svc := newTestService(t)

	now := time.Date(2026, 4, 16, 12, 0, 0, 0, time.Local).UnixMilli()

	insertTestLogs(t, svc.db, []trafficLog{
		{Timestamp: now - int64(4*24*time.Hour/time.Millisecond), SourceIP: "old-raw", Host: "a.com", Process: "chrome", Outbound: "NodeA", Upload: 1, Download: 1},
		{Timestamp: now - int64(2*24*time.Hour/time.Millisecond), SourceIP: "keep-raw", Host: "a.com", Process: "chrome", Outbound: "NodeA", Upload: 2, Download: 2},
	})
	insertTestAggregates(t, svc.db, []aggregatedEntry{
		{BucketStart: now - int64(31*24*time.Hour/time.Millisecond), BucketEnd: now - int64(31*24*time.Hour/time.Millisecond) + 60000, SourceIP: "old-agg", Host: "a.com", Process: "chrome", Outbound: "NodeA", Chains: `["DIRECT"]`, Upload: 3, Download: 3, Count: 1},
		{BucketStart: now - int64(20*24*time.Hour/time.Millisecond), BucketEnd: now - int64(20*24*time.Hour/time.Millisecond) + 60000, SourceIP: "keep-agg", Host: "a.com", Process: "chrome", Outbound: "NodeA", Chains: `["DIRECT"]`, Upload: 4, Download: 4, Count: 1},
	})

	if err := svc.cleanupOldLogs(now); err != nil {
		t.Fatalf("cleanupOldLogs: %v", err)
	}

	var rawCount int
	if err := svc.db.QueryRow(`SELECT COUNT(*) FROM traffic_logs`).Scan(&rawCount); err != nil {
		t.Fatalf("count traffic_logs: %v", err)
	}
	if rawCount != 0 {
		t.Fatalf("expected cleanup to remove persisted raw logs, got %d rows", rawCount)
	}

	var aggregateCount int
	if err := svc.db.QueryRow(`SELECT COUNT(*) FROM traffic_aggregated`).Scan(&aggregateCount); err != nil {
		t.Fatalf("count traffic_aggregated: %v", err)
	}
	if aggregateCount != 1 {
		t.Fatalf("expected 1 aggregate row after cleanup, got %d", aggregateCount)
	}
}

func TestProcessConnectionsBuffersAggregatesWithoutPersistingRawLogs(t *testing.T) {
	svc := newTestService(t)

	payload := &connectionsResponse{
		Connections: []connection{
			{
				ID:       "conn-1",
				Upload:   128,
				Download: 256,
				Chains:   []string{"ProxyA", "RelayB"},
				Metadata: struct {
					SourceIP      string "json:\"sourceIP\""
					Host          string "json:\"host\""
					DestinationIP string "json:\"destinationIP\""
					Process       string "json:\"process\""
				}{
					SourceIP:      "192.168.1.8",
					Host:          "api.example.com",
					DestinationIP: "1.1.1.1",
					Process:       "curl",
				},
			},
		},
	}

	if err := svc.processConnections(payload); err != nil {
		t.Fatalf("processConnections: %v", err)
	}

	var rawCount int
	if err := svc.db.QueryRow(`SELECT COUNT(*) FROM traffic_logs`).Scan(&rawCount); err != nil {
		t.Fatalf("count traffic_logs: %v", err)
	}
	if rawCount != 0 {
		t.Fatalf("expected no raw logs to be persisted, got %d", rawCount)
	}

	if len(svc.aggregateBuffer) != 1 {
		t.Fatalf("expected 1 aggregate buffer entry, got %d", len(svc.aggregateBuffer))
	}
	for _, entry := range svc.aggregateBuffer {
		if entry.Host != "api.example.com" || entry.DestinationIP != "1.1.1.1" {
			t.Fatalf("unexpected aggregate entry: %+v", entry)
		}
		if entry.Outbound != "ProxyA" || entry.Process != "curl" {
			t.Fatalf("unexpected routing fields: %+v", entry)
		}
		if entry.Upload != 128 || entry.Download != 256 || entry.Count != 1 {
			t.Fatalf("unexpected aggregate totals: %+v", entry)
		}
	}
}

func TestQueryAggregateIncludesBufferedData(t *testing.T) {
	svc := newTestService(t)

	insertTestAggregates(t, svc.db, []aggregatedEntry{
		{
			BucketStart: 60_000,
			BucketEnd:   120_000,
			SourceIP:    "192.168.1.2",
			Host:        "a.com",
			Process:     "chrome",
			Outbound:    "NodeA",
			Chains:      `["DIRECT"]`,
			Upload:      30,
			Download:    40,
			Count:       1,
		},
	})

	svc.aggregateBuffer["pending"] = &aggregatedEntry{
		BucketStart:   120_000,
		BucketEnd:     180_000,
		SourceIP:      "192.168.1.2",
		Host:          "a.com",
		DestinationIP: "1.1.1.1",
		Process:       "chrome",
		Outbound:      "NodeA",
		Chains:        `["DIRECT"]`,
		Upload:        50,
		Download:      60,
		Count:         1,
	}

	got, err := svc.queryAggregate("sourceIP", 60_000, 180_000)
	if err != nil {
		t.Fatalf("queryAggregate: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("expected 1 row, got %d", len(got))
	}
	if got[0].Upload != 80 || got[0].Download != 100 || got[0].Total != 180 || got[0].Count != 2 {
		t.Fatalf("unexpected aggregated result: %+v", got[0])
	}
}

func TestQueryTrendRebucketsAggregatedMinuteData(t *testing.T) {
	svc := newTestService(t)

	insertTestAggregates(t, svc.db, []aggregatedEntry{
		{BucketStart: 60_000, BucketEnd: 120_000, SourceIP: "192.168.1.2", Host: "a.com", Process: "chrome", Outbound: "NodeA", Chains: `["DIRECT"]`, Upload: 10, Download: 20, Count: 1},
		{BucketStart: 120_000, BucketEnd: 180_000, SourceIP: "192.168.1.2", Host: "a.com", Process: "chrome", Outbound: "NodeA", Chains: `["DIRECT"]`, Upload: 30, Download: 40, Count: 1},
		{BucketStart: 180_000, BucketEnd: 240_000, SourceIP: "192.168.1.2", Host: "a.com", Process: "chrome", Outbound: "NodeA", Chains: `["DIRECT"]`, Upload: 50, Download: 60, Count: 1},
		{BucketStart: 240_000, BucketEnd: 300_000, SourceIP: "192.168.1.2", Host: "a.com", Process: "chrome", Outbound: "NodeA", Chains: `["DIRECT"]`, Upload: 70, Download: 80, Count: 1},
	})

	got, err := svc.queryTrend(0, 600_000, 300_000)
	if err != nil {
		t.Fatalf("queryTrend: %v", err)
	}

	if len(got) != 3 {
		t.Fatalf("expected 3 buckets, got %d", len(got))
	}
	if got[0].Timestamp != 0 || got[0].Upload != 160 || got[0].Download != 200 {
		t.Fatalf("unexpected first bucket: %+v", got[0])
	}
	if got[1].Timestamp != 300_000 || got[1].Upload != 0 || got[1].Download != 0 {
		t.Fatalf("unexpected second bucket: %+v", got[1])
	}
}

func TestQueryTrendIncludesBufferedData(t *testing.T) {
	svc := newTestService(t)

	insertTestAggregates(t, svc.db, []aggregatedEntry{
		{BucketStart: 0, BucketEnd: 60_000, SourceIP: "192.168.1.2", Host: "a.com", Process: "chrome", Outbound: "NodeA", Chains: `["DIRECT"]`, Upload: 10, Download: 20, Count: 1},
	})

	svc.aggregateBuffer["pending"] = &aggregatedEntry{
		BucketStart:   60_000,
		BucketEnd:     120_000,
		SourceIP:      "192.168.1.2",
		Host:          "a.com",
		DestinationIP: "1.1.1.1",
		Process:       "chrome",
		Outbound:      "NodeA",
		Chains:        `["DIRECT"]`,
		Upload:        30,
		Download:      40,
		Count:         1,
	}

	got, err := svc.queryTrend(0, 120_000, 60_000)
	if err != nil {
		t.Fatalf("queryTrend: %v", err)
	}

	if len(got) != 3 {
		t.Fatalf("expected 3 buckets, got %d", len(got))
	}
	if got[0].Timestamp != 0 || got[0].Upload != 10 || got[0].Download != 20 {
		t.Fatalf("unexpected first bucket: %+v", got[0])
	}
	if got[1].Timestamp != 60_000 || got[1].Upload != 30 || got[1].Download != 40 {
		t.Fatalf("unexpected second bucket: %+v", got[1])
	}
}

func TestHandleLogsClearsAggregatesAndBuffer(t *testing.T) {
	svc := newTestService(t)

	insertTestLogs(t, svc.db, []trafficLog{
		{Timestamp: 1000, SourceIP: "192.168.1.2", Host: "a.com", Process: "chrome", Outbound: "NodeA", Upload: 100, Download: 200},
	})
	insertTestAggregates(t, svc.db, []aggregatedEntry{
		{BucketStart: 0, BucketEnd: 60_000, SourceIP: "192.168.1.2", Host: "a.com", Process: "chrome", Outbound: "NodeA", Chains: `["DIRECT"]`, Upload: 100, Download: 200, Count: 1},
	})
	svc.aggregateBuffer["pending"] = &aggregatedEntry{BucketStart: 60_000, BucketEnd: 120_000, SourceIP: "192.168.1.2"}

	req := httptest.NewRequest(http.MethodDelete, "/api/traffic/logs", nil)
	rec := httptest.NewRecorder()

	svc.handleLogs(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d body=%s", rec.Code, rec.Body.String())
	}

	var rawCount int
	if err := svc.db.QueryRow(`SELECT COUNT(*) FROM traffic_logs`).Scan(&rawCount); err != nil {
		t.Fatalf("count traffic_logs: %v", err)
	}
	if rawCount != 0 {
		t.Fatalf("expected traffic_logs to be empty, got %d rows", rawCount)
	}

	var aggregateCount int
	if err := svc.db.QueryRow(`SELECT COUNT(*) FROM traffic_aggregated`).Scan(&aggregateCount); err != nil {
		t.Fatalf("count traffic_aggregated: %v", err)
	}
	if aggregateCount != 0 {
		t.Fatalf("expected traffic_aggregated to be empty, got %d rows", aggregateCount)
	}

	if len(svc.aggregateBuffer) != 0 {
		t.Fatalf("expected aggregate buffer to be empty, got %d entries", len(svc.aggregateBuffer))
	}
}

func TestHandleConnectionDetailsReturnsGroupedDetails(t *testing.T) {
	svc := newTestService(t)

	insertTestAggregates(t, svc.db, []aggregatedEntry{
		{
			BucketStart:   0,
			BucketEnd:     60_000,
			SourceIP:      "192.168.1.2",
			Host:          "a.com",
			DestinationIP: "1.1.1.1",
			Process:       "chrome",
			Outbound:      "NodeA",
			Chains:        `["NodeA","RelayA"]`,
			Upload:        100,
			Download:      200,
			Count:         1,
		},
		{
			BucketStart:   60_000,
			BucketEnd:     120_000,
			SourceIP:      "192.168.1.2",
			Host:          "a.com",
			DestinationIP: "1.1.1.1",
			Process:       "chrome",
			Outbound:      "NodeA",
			Chains:        `["NodeA","RelayA"]`,
			Upload:        10,
			Download:      20,
			Count:         1,
		},
		{
			BucketStart:   0,
			BucketEnd:     60_000,
			SourceIP:      "192.168.1.3",
			Host:          "a.com",
			DestinationIP: "8.8.8.8",
			Process:       "curl",
			Outbound:      "NodeB",
			Chains:        `["NodeB"]`,
			Upload:        30,
			Download:      40,
			Count:         1,
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/api/traffic/details?dimension=host&primary=a.com&secondary=192.168.1.2&start=1&end=120000", nil)
	rec := httptest.NewRecorder()

	svc.handleConnectionDetails(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d body=%s", rec.Code, rec.Body.String())
	}

	var got []connectionDetail
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("expected 1 grouped detail, got %d", len(got))
	}
	if got[0].DestinationIP != "1.1.1.1" {
		t.Fatalf("unexpected destination ip: %+v", got[0])
	}
	if got[0].SourceIP != "192.168.1.2" {
		t.Fatalf("unexpected source ip: %+v", got[0])
	}
	if got[0].Process != "chrome" || got[0].Outbound != "NodeA" {
		t.Fatalf("unexpected routing info: %+v", got[0])
	}
	if got[0].Upload != 110 || got[0].Download != 220 || got[0].Count != 2 {
		t.Fatalf("unexpected aggregates: %+v", got[0])
	}
	if len(got[0].Chains) != 2 || got[0].Chains[0] != "NodeA" {
		t.Fatalf("unexpected chains: %+v", got[0])
	}
}

func TestHandleConnectionDetailsIncludesBufferedData(t *testing.T) {
	svc := newTestService(t)

	svc.aggregateBuffer["pending"] = &aggregatedEntry{
		BucketStart:   120_000,
		BucketEnd:     180_000,
		SourceIP:      "192.168.1.2",
		Host:          "a.com",
		DestinationIP: "1.1.1.1",
		Process:       "chrome",
		Outbound:      "NodeA",
		Chains:        `["NodeA","RelayA"]`,
		Upload:        15,
		Download:      25,
		Count:         1,
	}

	req := httptest.NewRequest(http.MethodGet, "/api/traffic/details?dimension=host&primary=a.com&secondary=192.168.1.2&start=1&end=180000", nil)
	rec := httptest.NewRecorder()

	svc.handleConnectionDetails(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d body=%s", rec.Code, rec.Body.String())
	}

	var got []connectionDetail
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("expected 1 grouped detail, got %d", len(got))
	}
	if got[0].Upload != 15 || got[0].Download != 25 || got[0].Count != 1 {
		t.Fatalf("unexpected buffered detail aggregates: %+v", got[0])
	}
}

func TestEmbeddedIndexDisablesPeriodicAutoRefresh(t *testing.T) {
	content, err := webAssets.ReadFile("web/index.html")
	if err != nil {
		t.Fatalf("read embedded index.html: %v", err)
	}

	html := string(content)
	if !strings.Contains(html, `elements.refreshBtn.addEventListener("click", loadData)`) {
		t.Fatalf("expected manual refresh handler to remain available")
	}
	if !strings.Contains(html, `elements.range.addEventListener("change", () => {`) {
		t.Fatalf("expected range change handler to exist")
	}
	if !strings.Contains(html, "if (Number(elements.range.value) !== -1) updateCustomInputs()") {
		t.Fatalf("expected preset range change to keep custom inputs in sync")
	}
	if !strings.Contains(html, `elements.start.addEventListener("change", () => {`) {
		t.Fatalf("expected custom start time change handler to exist")
	}
	if !strings.Contains(html, `elements.end.addEventListener("change", () => {`) {
		t.Fatalf("expected custom end time change handler to exist")
	}
	if !strings.Contains(html, `elements.range.value = "-1"`) {
		t.Fatalf("expected manual datetime edits to switch range into custom mode")
	}
	if !strings.Contains(html, `elements.start.addEventListener("change", () => {`) || !strings.Contains(html, `loadData()`) {
		t.Fatalf("expected manual time edits to request fresh data")
	}
	if strings.Contains(html, "setInterval(loadData, 30000)") {
		t.Fatalf("expected periodic auto refresh to be removed")
	}
	if !strings.Contains(html, "updateCustomInputs()\n      updateViewHints()\n      loadData()") {
		t.Fatalf("expected initial page load to fetch data once")
	}
	for _, label := range []string{"1 天", "7 天", "15 天", "30 天", "自定义"} {
		if !strings.Contains(html, label) {
			t.Fatalf("expected range option %q to exist", label)
		}
	}
	for _, label := range []string{"最近 1 小时", "最近 24 小时"} {
		if strings.Contains(html, label) {
			t.Fatalf("expected old range option %q to be removed", label)
		}
	}
}

func TestEmbeddedIndexIncludesGithubAndLicenseFooter(t *testing.T) {
	content, err := webAssets.ReadFile("web/index.html")
	if err != nil {
		t.Fatalf("read embedded index.html: %v", err)
	}

	html := string(content)
	for _, want := range []string{
		`href="https://github.com/zhf883680/clash-traffic-monitor"`,
		`>GitHub<`,
		`href="/LICENSE"`,
		`>MIT License<`,
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("expected embedded index.html to contain %q", want)
		}
	}
}

func TestRoutesServeLicense(t *testing.T) {
	svc := newTestService(t)

	req := httptest.NewRequest(http.MethodGet, "/LICENSE", nil)
	rec := httptest.NewRecorder()

	svc.routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d body=%s", rec.Code, rec.Body.String())
	}
	if contentType := rec.Header().Get("Content-Type"); !strings.Contains(contentType, "text/plain") {
		t.Fatalf("expected text/plain content type, got %q", contentType)
	}
	for _, want := range []string{
		"MIT License",
		"Permission is hereby granted, free of charge",
	} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Fatalf("expected license response to contain %q", want)
		}
	}
}

func newTestService(t *testing.T) *service {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "traffic.db")
	db, err := openDatabase(dbPath)
	if err != nil {
		t.Fatalf("openDatabase: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	return &service{
		db:              db,
		lastConnections: make(map[string]connection),
		aggregateBuffer: make(map[string]*aggregatedEntry),
	}
}

func insertTestLogs(t *testing.T, db *sql.DB, logs []trafficLog) {
	t.Helper()

	for _, entry := range logs {
		chainsRaw, err := json.Marshal(entry.Chains)
		if err != nil {
			t.Fatalf("marshal chains: %v", err)
		}

		_, err = db.Exec(
			`INSERT INTO traffic_logs (timestamp, source_ip, host, destination_ip, process, outbound, chains, upload, download)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			entry.Timestamp,
			entry.SourceIP,
			entry.Host,
			entry.DestinationIP,
			entry.Process,
			entry.Outbound,
			string(chainsRaw),
			entry.Upload,
			entry.Download,
		)
		if err != nil {
			t.Fatalf("insert log: %v", err)
		}
	}
}

func insertTestAggregates(t *testing.T, db *sql.DB, entries []aggregatedEntry) {
	t.Helper()

	for _, entry := range entries {
		_, err := db.Exec(
			`INSERT INTO traffic_aggregated
			 (bucket_start, bucket_end, source_ip, host, destination_ip, process, outbound, chains, upload, download, count)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			entry.BucketStart,
			entry.BucketEnd,
			entry.SourceIP,
			entry.Host,
			entry.DestinationIP,
			entry.Process,
			entry.Outbound,
			entry.Chains,
			entry.Upload,
			entry.Download,
			entry.Count,
		)
		if err != nil {
			t.Fatalf("insert aggregate: %v", err)
		}
	}
}
