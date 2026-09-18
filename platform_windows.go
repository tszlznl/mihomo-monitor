//go:build windows

package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"time"

	sqlite3 "github.com/mattn/go-sqlite3"
	"golang.org/x/sys/windows"
)

var osExecutable = os.Executable

const (
	windowsDataDirName     = "data"
	windowsDatabaseName    = "traffic_monitor.db"
	windowsLogName         = "traffic-monitor.log"
	windowsMigratingSuffix = ".migrating"
)

// windowsDataDir returns the directory next to the executable that holds the
// database and log. Autostart via HKCU Run gives no working directory, so all
// local state must be anchored to the executable location instead of the cwd.
func windowsDataDir() (string, error) {
	exePath, err := osExecutable()
	if err != nil {
		return "", fmt.Errorf("resolve executable path: %w", err)
	}
	exeDir := filepath.Dir(exePath)
	if strings.TrimSpace(exeDir) == "" {
		return "", errors.New("executable path has no directory")
	}
	return filepath.Join(exeDir, windowsDataDirName), nil
}

// windowsDatabasePath returns the canonical database path next to the
// executable and, on first run of the new layout, migrates a legacy database
// found in the working directory without ever deleting it.
func windowsDatabasePath() (string, error) {
	dataDir, err := windowsDataDir()
	if err != nil {
		return "", err
	}
	canonical := filepath.Join(dataDir, windowsDatabaseName)

	if _, err := os.Stat(canonical); err == nil {
		return canonical, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("stat canonical database: %w", err)
	}

	legacy, err := filepath.Abs(filepath.Join(windowsDataDirName, windowsDatabaseName))
	if err != nil {
		return "", fmt.Errorf("resolve legacy database path: %w", err)
	}
	if sameWindowsPath(legacy, canonical) {
		return canonical, nil
	}

	if _, err := os.Stat(legacy); err == nil {
		if err := migrateLegacyWindowsDatabase(legacy, canonical); err != nil {
			return "", err
		}
		return canonical, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("stat legacy database: %w", err)
	}

	return canonical, nil
}

func sameWindowsPath(a, b string) bool {
	return strings.EqualFold(filepath.Clean(a), filepath.Clean(b))
}

// databaseBackupTimeout bounds the SQLite Online Backup used by the legacy
// database migration. It is a variable so tests can shorten it.
var databaseBackupTimeout = 10 * time.Second

// migrateLegacyWindowsDatabase snapshots the legacy database into dstPath via
// the SQLite Online Backup API, so uncheckpointed WAL content is included and
// concurrent writers observe a consistent snapshot. The legacy file is kept as
// a manually recoverable backup; this function never deletes it.
func migrateLegacyWindowsDatabase(srcPath, dstPath string) error {
	if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
		return fmt.Errorf("create data directory: %w", err)
	}

	migratingPath := dstPath + windowsMigratingSuffix
	cleanupMigrating := func(cause error) error {
		if err := os.Remove(migratingPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			log.Printf("remove failed migration temp file %s: %v", migratingPath, err)
		}
		return cause
	}

	if err := backupSQLiteDatabase(srcPath, migratingPath, databaseBackupTimeout); err != nil {
		return cleanupMigrating(fmt.Errorf("back up legacy database %s: %w", srcPath, err))
	}
	if err := quickCheckSQLiteDatabase(migratingPath); err != nil {
		return cleanupMigrating(err)
	}
	if err := syncFile(migratingPath); err != nil {
		return cleanupMigrating(fmt.Errorf("sync migrated database: %w", err))
	}
	if err := os.Rename(migratingPath, dstPath); err != nil {
		return cleanupMigrating(fmt.Errorf("move migrated database into place: %w", err))
	}

	log.Printf("migrated legacy database %s to %s; original file kept as backup", srcPath, dstPath)
	return nil
}

// backupSQLiteDatabase copies srcPath into dstPath with the Online Backup API.
// Both databases are private one-shot handles used only during startup, so the
// raw driver connections are held for the duration of the copy.
func backupSQLiteDatabase(srcPath, dstPath string, timeout time.Duration) error {
	srcDB, err := sql.Open("sqlite3", srcPath+"?_busy_timeout=1000")
	if err != nil {
		return fmt.Errorf("open source database: %w", err)
	}
	defer srcDB.Close()

	dstDB, err := sql.Open("sqlite3", dstPath)
	if err != nil {
		return fmt.Errorf("open target database: %w", err)
	}
	defer dstDB.Close()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	srcConn, err := srcDB.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire source connection: %w", err)
	}
	defer srcConn.Close()

	dstConn, err := dstDB.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire target connection: %w", err)
	}
	defer dstConn.Close()

	var srcRaw, dstRaw *sqlite3.SQLiteConn
	if err := srcConn.Raw(func(driverConn any) error {
		conn, ok := driverConn.(*sqlite3.SQLiteConn)
		if !ok {
			return errors.New("source driver connection is not a sqlite connection")
		}
		srcRaw = conn
		return nil
	}); err != nil {
		return fmt.Errorf("access source connection: %w", err)
	}
	if err := dstConn.Raw(func(driverConn any) error {
		conn, ok := driverConn.(*sqlite3.SQLiteConn)
		if !ok {
			return errors.New("target driver connection is not a sqlite connection")
		}
		dstRaw = conn
		return nil
	}); err != nil {
		return fmt.Errorf("access target connection: %w", err)
	}

	backup, err := dstRaw.Backup("main", srcRaw, "main")
	if err != nil {
		return fmt.Errorf("start sqlite backup: %w", err)
	}

	deadline := time.Now().Add(timeout)
	var backupErr error
	for {
		done, stepErr := backup.Step(-1)
		if stepErr != nil {
			backupErr = stepErr
			if !isSQLiteBusyOrLocked(stepErr) {
				break
			}
			if !time.Now().Before(deadline) {
				backupErr = errors.New("sqlite backup timed out waiting for the source database")
				break
			}
			time.Sleep(50 * time.Millisecond)
			continue
		}
		if done {
			backupErr = nil
			break
		}
		// Step returned without finishing: the backup restarted because the
		// source changed, or pages remain. Keep stepping until it completes.
		if !time.Now().Before(deadline) {
			backupErr = errors.New("sqlite backup timed out before completing")
			break
		}
	}
	if finishErr := backup.Finish(); backupErr == nil {
		backupErr = finishErr
	}
	if backupErr != nil {
		return fmt.Errorf("copy database: %w", backupErr)
	}
	return nil
}

func isSQLiteBusyOrLocked(err error) bool {
	var sqliteErr sqlite3.Error
	if errors.As(err, &sqliteErr) {
		return sqliteErr.Code == sqlite3.ErrBusy || sqliteErr.Code == sqlite3.ErrLocked
	}
	return false
}

func quickCheckSQLiteDatabase(path string) error {
	db, err := sql.Open("sqlite3", "file:"+filepath.ToSlash(path)+"?mode=ro")
	if err != nil {
		return fmt.Errorf("open migrated database for verification: %w", err)
	}
	defer db.Close()

	rows, err := db.Query(`PRAGMA quick_check`)
	if err != nil {
		return fmt.Errorf("quick_check migrated database: %w", err)
	}
	defer rows.Close()

	var results []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			return fmt.Errorf("read quick_check result: %w", err)
		}
		results = append(results, line)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read quick_check result: %w", err)
	}
	if len(results) != 1 || results[0] != "ok" {
		return fmt.Errorf("quick_check reported: %s", strings.Join(results, "; "))
	}
	return nil
}

func syncFile(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

const (
	rotatingLogMaxSize      = 5 * 1024 * 1024
	rotatingLogBackupSuffix = ".1"
)

// rotatingLogWriter is a mutex-protected io.Writer for the standard log
// package that rotates the current file once it exceeds rotatingLogMaxSize and
// keeps a single ".1" backup. Rotation failures are reported on a non-blocking
// channel; writing continues by reopening the current file.
type rotatingLogWriter struct {
	mu         sync.Mutex
	path       string
	backupPath string
	file       *os.File
	size       int64
	errCh      chan error
	stickyErr  error
}

func newRotatingLogWriter(path string) (*rotatingLogWriter, error) {
	w := &rotatingLogWriter{
		path:       path,
		backupPath: path + rotatingLogBackupSuffix,
		errCh:      make(chan error, 1),
	}
	if err := w.openCurrentLocked(); err != nil {
		w.stickyErr = err
		return w, fmt.Errorf("open log file %s: %w", path, err)
	}
	return w, nil
}

func (w *rotatingLogWriter) Errors() <-chan error {
	return w.errCh
}

// stickyError reports an unrecoverable writer failure: logging cannot
// continue until the process restarts.
func (w *rotatingLogWriter) stickyError() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.stickyErr
}

func (w *rotatingLogWriter) notify(err error) {
	select {
	case w.errCh <- err:
	default:
	}
}

func (w *rotatingLogWriter) openCurrentLocked() error {
	f, err := os.OpenFile(w.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}
	w.file = f
	w.size = info.Size()
	return nil
}

func (w *rotatingLogWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		if err := w.openCurrentLocked(); err != nil {
			w.stickyErr = err
			return 0, fmt.Errorf("reopen log file %s: %w", w.path, err)
		}
	}

	// Rotation happens before the oversized message is written so the message
	// itself always lands complete in the fresh file.
	if w.size+int64(len(p)) > rotatingLogMaxSize {
		if err := w.rotateLocked(); err != nil {
			w.notify(fmt.Errorf("rotate log: %w", err))
			if w.file == nil {
				return 0, err
			}
		}
	}

	n, err := w.file.Write(p)
	if n > 0 {
		w.size += int64(n)
	}
	if err != nil {
		w.notify(fmt.Errorf("write log: %w", err))
		return n, err
	}
	return n, nil
}

// rotateLocked moves the current file to the ".1" backup and opens a fresh
// one. Whatever fails, it reopens the current path afterwards so logging
// continues; callers report the returned error to the supervisor.
func (w *rotatingLogWriter) rotateLocked() error {
	var errs []error
	if w.file != nil {
		if err := w.file.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close current log: %w", err))
		}
		w.file = nil
		w.size = 0
	}

	if err := os.Remove(w.backupPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		errs = append(errs, fmt.Errorf("remove old log backup: %w", err))
	}
	if err := os.Rename(w.path, w.backupPath); err != nil {
		errs = append(errs, fmt.Errorf("rename log to backup: %w", err))
	}

	if err := w.openCurrentLocked(); err != nil {
		w.stickyErr = err
		errs = append(errs, fmt.Errorf("reopen current log: %w", err))
		return errors.Join(errs...)
	}
	return errors.Join(errs...)
}

func (w *rotatingLogWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	return err
}

var windowsLog *rotatingLogWriter

// runPlatform starts the application with state anchored next to the
// executable and logs into a rotating file, because a GUI-subsystem process
// has no console to print diagnostics to.
func runPlatform() error {
	cfg, err := loadConfig()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	dataDir, err := windowsDataDir()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return fmt.Errorf("create data directory: %w", err)
	}

	logWriter, err := newRotatingLogWriter(filepath.Join(dataDir, windowsLogName))
	if err != nil {
		return err
	}
	windowsLog = logWriter
	log.SetOutput(logWriter)
	log.Printf("traffic monitor starting (pid %d)", os.Getpid())

	dbPath, err := windowsDatabasePath()
	if err != nil {
		return err
	}

	app, err := newApplication(cfg, dbPath)
	if err != nil {
		return err
	}

	app.start()
	if err := logWriter.stickyError(); err != nil {
		app.shutdown()
		return fmt.Errorf("log writer: %w", err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)

	var runErr error
	select {
	case err := <-app.errors():
		log.Printf("server error: %v", err)
		if shutdownErr := app.shutdown(); shutdownErr != nil {
			log.Printf("shutdown after server error: %v", shutdownErr)
		}
		runErr = err
	case <-sigCh:
		runErr = app.shutdown()
	}

	if err := logWriter.stickyError(); err != nil {
		runErr = errors.Join(runErr, fmt.Errorf("log writer: %w", err))
	}
	log.Printf("traffic monitor exiting")
	windowsLog.Close()
	return runErr
}

func reportPlatformFatal(err error) {
	log.Printf("fatal: %v", err)
	showErrorMessageBox(fmt.Sprintf("Traffic Monitor failed to start:\n\n%v\n\nSee traffic-monitor.log in the data folder next to the program for details.", err))
	if windowsLog != nil {
		windowsLog.Close()
	}
}

func showErrorMessageBox(text string) {
	title, err := windows.UTF16PtrFromString("Traffic Monitor")
	if err != nil {
		return
	}
	message, err := windows.UTF16PtrFromString(text)
	if err != nil {
		return
	}
	_, _ = windows.MessageBox(0, message, title, windows.MB_OK|windows.MB_ICONERROR)
}
