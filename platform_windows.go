//go:build windows

package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
)

// runPlatform is the minimal Windows entrypoint for the lifecycle refactor.
// The tray shell, autostart and single-instance handling are added on top of
// this in the tray implementation; console logging is kept until then.
func runPlatform() error {
	cfg, err := loadConfig()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	app, err := newApplication(cfg, defaultDatabasePath())
	if err != nil {
		return err
	}

	app.start()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)

	select {
	case err := <-app.errors():
		log.Printf("server error: %v", err)
		if shutdownErr := app.shutdown(); shutdownErr != nil {
			log.Printf("shutdown after server error: %v", shutdownErr)
		}
		return err
	case <-sigCh:
	}

	return app.shutdown()
}

func reportPlatformFatal(err error) {
	log.Printf("fatal: %v", err)
}
