//go:build windows

package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func overrideExecutable(t *testing.T, exePath string) {
	t.Helper()
	previous := osExecutable
	osExecutable = func() (string, error) { return exePath, nil }
	t.Cleanup(func() { osExecutable = previous })
}

func overrideExecutableError(t *testing.T) {
	t.Helper()
	previous := osExecutable
	osExecutable = func() (string, error) { return "", errors.New("injected executable failure") }
	t.Cleanup(func() { osExecutable = previous })
}

func createLegacySQLiteDatabase(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create legacy directory: %v", err)
	}
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open legacy database: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY, value TEXT NOT NULL)`); err != nil {
		t.Fatalf("create legacy table: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO items (value) VALUES ('legacy-row')`); err != nil {
		t.Fatalf("insert legacy row: %v", err)
	}
}

func openSQLiteDatabase(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func countItemsRows(t *testing.T, path string, wantValue string) int {
	t.Helper()
	db := openSQLiteDatabase(t, path)
	defer db.Close()
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM items WHERE value = ?`, wantValue).Scan(&count); err != nil {
		t.Fatalf("count rows in %s: %v", path, err)
	}
	return count
}

func TestWindowsDataDirAnchorsToExecutable(t *testing.T) {
	exeDir := t.TempDir()
	overrideExecutable(t, filepath.Join(exeDir, "sub", "traffic-monitor.exe"))

	got, err := windowsDataDir()
	if err != nil {
		t.Fatalf("windowsDataDir: %v", err)
	}
	want := filepath.Join(exeDir, "sub", "data")
	if got != want {
		t.Fatalf("expected data dir %q, got %q", want, got)
	}
}

func TestWindowsDataDirFailsWithoutExecutable(t *testing.T) {
	overrideExecutableError(t)
	if _, err := windowsDataDir(); err == nil {
		t.Fatal("expected error when the executable path cannot be resolved")
	}
}

func TestWindowsDatabasePathPrefersExistingCanonical(t *testing.T) {
	root := t.TempDir()
	overrideExecutable(t, filepath.Join(root, "app", "traffic-monitor.exe"))
	chdirForTest(t, root)

	canonicalDir := filepath.Join(root, "app", "data")
	if err := os.MkdirAll(canonicalDir, 0o755); err != nil {
		t.Fatalf("create canonical dir: %v", err)
	}
	canonical := filepath.Join(canonicalDir, windowsDatabaseName)
	createLegacySQLiteDatabase(t, canonical)

	legacyPath := filepath.Join(root, "data", windowsDatabaseName)
	createLegacySQLiteDatabase(t, legacyPath)
	legacyInfo, err := os.Stat(legacyPath)
	if err != nil {
		t.Fatalf("stat legacy: %v", err)
	}

	got, err := windowsDatabasePath()
	if err != nil {
		t.Fatalf("windowsDatabasePath: %v", err)
	}
	if got != canonical {
		t.Fatalf("expected canonical path %q, got %q", canonical, got)
	}
	if _, err := os.Stat(filepath.Join(root, "app", "data", windowsDatabaseName+windowsMigratingSuffix)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected no migrating temp file, got %v", err)
	}
	stillThere, err := os.Stat(legacyPath)
	if err != nil || !stillThere.ModTime().Equal(legacyInfo.ModTime()) {
		t.Fatalf("legacy database must remain untouched: %v", err)
	}
}

func TestWindowsDatabasePathMigratesLegacyDatabase(t *testing.T) {
	root := t.TempDir()
	overrideExecutable(t, filepath.Join(root, "app", "traffic-monitor.exe"))
	chdirForTest(t, root)

	legacyPath := filepath.Join(root, "data", windowsDatabaseName)
	createLegacySQLiteDatabase(t, legacyPath)

	got, err := windowsDatabasePath()
	if err != nil {
		t.Fatalf("windowsDatabasePath: %v", err)
	}
	canonical := filepath.Join(root, "app", "data", windowsDatabaseName)
	if got != canonical {
		t.Fatalf("expected canonical path %q, got %q", canonical, got)
	}
	if countItemsRows(t, canonical, "legacy-row") != 1 {
		t.Fatal("migrated database must contain the legacy rows")
	}
	if countItemsRows(t, legacyPath, "legacy-row") != 1 {
		t.Fatal("legacy database must be preserved as backup")
	}
	if _, err := os.Stat(canonical + windowsMigratingSuffix); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected migrating temp file to be gone, got %v", err)
	}
}

func TestWindowsDatabasePathWithoutAnyDatabase(t *testing.T) {
	root := t.TempDir()
	overrideExecutable(t, filepath.Join(root, "app", "traffic-monitor.exe"))
	chdirForTest(t, root)

	got, err := windowsDatabasePath()
	if err != nil {
		t.Fatalf("windowsDatabasePath: %v", err)
	}
	canonical := filepath.Join(root, "app", "data", windowsDatabaseName)
	if got != canonical {
		t.Fatalf("expected canonical path %q, got %q", canonical, got)
	}
	if _, err := os.Stat(canonical); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("resolver must not create the database, got %v", err)
	}
}

func TestWindowsDatabasePathExecutableFailure(t *testing.T) {
	overrideExecutableError(t)
	if _, err := windowsDatabasePath(); err == nil {
		t.Fatal("expected error when executable cannot be resolved")
	}
}

func TestMigrateLegacyWindowsDatabaseKeepsUncheckpointedWAL(t *testing.T) {
	root := t.TempDir()
	src := filepath.Join(root, "legacy.db")
	dst := filepath.Join(root, "canonical", windowsDatabaseName)

	srcDB := openSQLiteDatabase(t, src)
	if _, err := srcDB.Exec(`PRAGMA journal_mode=WAL`); err != nil {
		t.Fatalf("enable WAL: %v", err)
	}
	if _, err := srcDB.Exec(`CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT NOT NULL)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := srcDB.Exec(`INSERT INTO items (value) VALUES ('wal-row')`); err != nil {
		t.Fatalf("insert row: %v", err)
	}
	// srcDB stays open: the committed row lives in the -wal file only.

	if err := migrateLegacyWindowsDatabase(src, dst); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if countItemsRows(t, dst, "wal-row") != 1 {
		t.Fatal("migrated database must include uncheckpointed WAL content")
	}
	var stillThere int
	if err := srcDB.QueryRow(`SELECT COUNT(*) FROM items`).Scan(&stillThere); err != nil || stillThere != 1 {
		t.Fatalf("source database must remain readable: %v (rows=%d)", err, stillThere)
	}
}

func TestMigrateLegacyWindowsDatabaseWithConcurrentWriter(t *testing.T) {
	root := t.TempDir()
	src := filepath.Join(root, "legacy.db")
	dst := filepath.Join(root, "canonical", windowsDatabaseName)

	srcDB := openSQLiteDatabase(t, src)
	if _, err := srcDB.Exec(`CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT NOT NULL)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := srcDB.Exec(`INSERT INTO items (value) VALUES ('first')`); err != nil {
		t.Fatalf("insert first row: %v", err)
	}

	stop := make(chan struct{})
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := srcDB.Exec(fmt.Sprintf(`INSERT INTO items (value) VALUES ('writer-%d')`, i)); err != nil {
				t.Logf("background writer stopped: %v", err)
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	err := migrateLegacyWindowsDatabase(src, dst)
	close(stop)
	<-writerDone
	if err != nil {
		t.Fatalf("migrate with concurrent writer: %v", err)
	}
	if countItemsRows(t, dst, "first") != 1 {
		t.Fatal("migrated database must contain data committed before the backup")
	}
}

func TestMigrateLegacyWindowsDatabaseBusyTimesOutWithoutCreatingTarget(t *testing.T) {
	root := t.TempDir()
	src := filepath.Join(root, "legacy.db")
	dst := filepath.Join(root, "canonical", windowsDatabaseName)

	srcDB := openSQLiteDatabase(t, src)
	if _, err := srcDB.Exec(`CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT NOT NULL)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := srcDB.Exec(`INSERT INTO items (value) VALUES ('locked')`); err != nil {
		t.Fatalf("insert row: %v", err)
	}

	// Hold an exclusive lock on a dedicated connection for the whole attempt.
	lockConn, err := srcDB.Conn(context.Background())
	if err != nil {
		t.Fatalf("acquire lock connection: %v", err)
	}
	if _, err := lockConn.ExecContext(context.Background(), `BEGIN EXCLUSIVE`); err != nil {
		lockConn.Close()
		t.Fatalf("acquire exclusive lock: %v", err)
	}
	defer func() {
		_, _ = lockConn.ExecContext(context.Background(), `ROLLBACK`)
		_ = lockConn.Close()
	}()

	previousTimeout := databaseBackupTimeout
	databaseBackupTimeout = 400 * time.Millisecond
	t.Cleanup(func() { databaseBackupTimeout = previousTimeout })

	if err := migrateLegacyWindowsDatabase(src, dst); err == nil {
		t.Fatal("expected migration to fail while the source stays locked")
	}
	if _, err := os.Stat(dst); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("target database must not be created on failure, got %v", err)
	}
	if _, err := os.Stat(dst + windowsMigratingSuffix); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("migrating temp file must be cleaned up, got %v", err)
	}
}

func TestRotatingLogWriterAppendsWithoutRotation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "traffic-monitor.log")
	w, err := newRotatingLogWriter(path)
	if err != nil {
		t.Fatalf("newRotatingLogWriter: %v", err)
	}

	if _, err := w.Write([]byte("line-one\n")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := w.Write([]byte("line-two\n")); err != nil {
		t.Fatalf("write: %v", err)
	}
	w.Close()

	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	if string(content) != "line-one\nline-two\n" {
		t.Fatalf("unexpected log content: %q", content)
	}
	if _, err := os.Stat(path + rotatingLogBackupSuffix); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("no backup expected below the size limit, got %v", err)
	}
}

func TestRotatingLogWriterRotatesAcrossThreshold(t *testing.T) {
	path := filepath.Join(t.TempDir(), "traffic-monitor.log")
	w, err := newRotatingLogWriter(path)
	if err != nil {
		t.Fatalf("newRotatingLogWriter: %v", err)
	}

	// First write lands just below the limit; the second crosses it and must
	// move the first chunk to .1 before being written to the fresh file.
	oldContent := strings.Repeat("a", rotatingLogMaxSize-4) + "\n"
	if _, err := w.Write([]byte(oldContent)); err != nil {
		t.Fatalf("write old content: %v", err)
	}
	if _, err := w.Write([]byte("fresh\n")); err != nil {
		t.Fatalf("write fresh content: %v", err)
	}
	w.Close()

	backup, err := os.ReadFile(path + rotatingLogBackupSuffix)
	if err != nil {
		t.Fatalf("read backup: %v", err)
	}
	if string(backup) != oldContent {
		t.Fatalf("backup must contain the rotated content (%d bytes), got %d bytes", len(oldContent), len(backup))
	}
	current, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read current: %v", err)
	}
	if string(current) != "fresh\n" {
		t.Fatalf("current must only contain the fresh content, got %q", current)
	}
}

func TestRotatingLogWriterWritesOversizedMessageFully(t *testing.T) {
	path := filepath.Join(t.TempDir(), "traffic-monitor.log")
	w, err := newRotatingLogWriter(path)
	if err != nil {
		t.Fatalf("newRotatingLogWriter: %v", err)
	}

	huge := strings.Repeat("z", rotatingLogMaxSize+1024) + "\n"
	if _, err := w.Write([]byte(huge)); err != nil {
		t.Fatalf("write oversized message: %v", err)
	}
	w.Close()

	current, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read current: %v", err)
	}
	if len(current) != len(huge) {
		t.Fatalf("oversized message must be written fully: got %d bytes, want %d", len(current), len(huge))
	}
	// Rotation runs before the message is written, so the empty first
	// generation becomes the .1 backup.
	backupInfo, err := os.Stat(path + rotatingLogBackupSuffix)
	if err != nil {
		t.Fatalf("first generation must be moved to backup: %v", err)
	}
	if backupInfo.Size() != 0 {
		t.Fatalf("expected empty backup of the first generation, got %d bytes", backupInfo.Size())
	}
}

func TestRotatingLogWriterKeepsSingleBackup(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "traffic-monitor.log")
	w, err := newRotatingLogWriter(path)
	if err != nil {
		t.Fatalf("newRotatingLogWriter: %v", err)
	}

	generations := []string{"gen-0\n", "gen-1\n", "gen-2\n"}
	for _, gen := range generations {
		if _, err := w.Write([]byte(strings.Repeat("g", rotatingLogMaxSize/2+5) + gen)); err != nil {
			t.Fatalf("write %s: %v", gen, err)
		}
	}
	w.Close()

	backup, err := os.ReadFile(path + rotatingLogBackupSuffix)
	if err != nil {
		t.Fatalf("read backup: %v", err)
	}
	if !strings.HasSuffix(string(backup), "gen-1\n") {
		t.Fatal("backup must hold the previous generation")
	}
	current, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read current: %v", err)
	}
	if !strings.HasSuffix(string(current), "gen-2\n") {
		t.Fatal("current must hold the newest generation")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	if len(entries) != 2 {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("expected exactly two log files, got %v", names)
	}
}

func TestRotatingLogWriterConcurrentWritesStayComplete(t *testing.T) {
	path := filepath.Join(t.TempDir(), "traffic-monitor.log")
	w, err := newRotatingLogWriter(path)
	if err != nil {
		t.Fatalf("newRotatingLogWriter: %v", err)
	}

	const writers = 8
	const perWriter = 64
	message := strings.Repeat("m", 2048) + "\n"

	var wg sync.WaitGroup
	errs := make(chan error, writers*perWriter)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perWriter; j++ {
				if _, err := w.Write([]byte(message)); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent write failed: %v", err)
	}
	w.Close()

	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read current: %v", err)
	}
	if len(content) != writers*perWriter*len(message) {
		t.Fatalf("expected %d written bytes, got %d", writers*perWriter*len(message), len(content))
	}
}

func TestRotatingLogWriterNotifiesOnRotationFailureAndKeepsWriting(t *testing.T) {
	path := filepath.Join(t.TempDir(), "traffic-monitor.log")
	w, err := newRotatingLogWriter(path)
	if err != nil {
		t.Fatalf("newRotatingLogWriter: %v", err)
	}

	if _, err := w.Write([]byte("first\n")); err != nil {
		t.Fatalf("write first: %v", err)
	}
	// A non-empty directory at the backup path blocks remove and rename.
	if err := os.MkdirAll(path+rotatingLogBackupSuffix, 0o755); err != nil {
		t.Fatalf("create blocking dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(path+rotatingLogBackupSuffix, "occupied"), []byte("x"), 0o644); err != nil {
		t.Fatalf("populate blocking dir: %v", err)
	}

	if _, err := w.Write([]byte(strings.Repeat("x", rotatingLogMaxSize+16) + "\nafter-failure\n")); err != nil {
		t.Fatalf("write across threshold: %v", err)
	}

	select {
	case rotateErr := <-w.Errors():
		if rotateErr == nil {
			t.Fatal("expected a rotation failure notification")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for rotation failure notification")
	}
	if err := w.stickyError(); err != nil {
		t.Fatalf("writer must keep writing after a failed rotation: %v", err)
	}

	if _, err := w.Write([]byte("still-alive\n")); err != nil {
		t.Fatalf("write after failure: %v", err)
	}
	w.Close()

	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read current: %v", err)
	}
	if !strings.HasPrefix(string(content), "first\n") || !strings.Contains(string(content), "after-failure\n") || !strings.HasSuffix(string(content), "still-alive\n") {
		t.Fatal("log content must be preserved and appended after the failed rotation")
	}
}

// chdirForTest changes the process working directory for the duration of the
// test. testing.T.Chdir needs go1.24, but the module targets go1.21.
func chdirForTest(t *testing.T, dir string) {
	t.Helper()
	previous, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("chdir to %s: %v", dir, err)
	}
	t.Cleanup(func() {
		if err := os.Chdir(previous); err != nil {
			t.Fatalf("restore working directory: %v", err)
		}
	})
}
