package runtimes

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
	"bytes"
	"encoding/json"
	"net/http"

	"github.com/absmach/propeller/pkg/mqtt"
	"github.com/absmach/propeller/proplet"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
)

type wazeroRuntime struct {
	mutex     sync.Mutex
	runtimes  map[string]wazero.Runtime
	pubsub    mqtt.PubSub
	domainID  string
	channelID string
	logger    *slog.Logger
}

func NewWazeroRuntime(logger *slog.Logger, pubsub mqtt.PubSub, domainID, channelID string) proplet.Runtime {
	return &wazeroRuntime{
		runtimes:  make(map[string]wazero.Runtime),
		pubsub:    pubsub,
		domainID:  domainID,
		channelID: channelID,
		logger:    logger,
	}
}

func (w *wazeroRuntime) StartApp(ctx context.Context, wasmBinary []byte, cliArgs []string, id, functionName string, daemon bool, env map[string]string, webhook string, args ...uint64) error {
	r := wazero.NewRuntime(ctx)

	w.mutex.Lock()
	w.runtimes[id] = r
	w.mutex.Unlock()

	// Instantiate WASI, which implements host functions needed for TinyGo to
	// implement `panic`.
	wasi_snapshot_preview1.MustInstantiate(ctx, r)

	module, err := r.InstantiateWithConfig(ctx, wasmBinary, wazero.NewModuleConfig().WithStartFunctions("_initialize"))
	if err != nil {
		return errors.Join(errors.New("failed to instantiate Wasm module"), err)
	}

	function := module.ExportedFunction(functionName)
	if function == nil {
		return errors.New("failed to find exported function")
	}

	go func() {
		results, err := function.Call(ctx, args...)
		if err != nil {
			w.logger.Error("failed to call function", slog.String("id", id), slog.String("function", functionName), slog.String("error", err.Error()))

			return
		}

		if err := w.StopApp(ctx, id); err != nil {
			w.logger.Error("failed to stop app", slog.String("id", id), slog.String("error", err.Error()))
		}

		payload := map[string]interface{}{
			"source":   "proplet",
			"task_id": id,
			"results": results,
		}

		topic := fmt.Sprintf(proplet.ResultsTopic, w.domainID, w.channelID)
		if err := w.pubsub.Publish(ctx, topic, payload); err != nil {
			w.logger.Error("failed to publish results", slog.String("id", id), slog.String("error", err.Error()))

			return
		}
		// --- NEW: SEND DIRECTLY TO USER IF WEBHOOK EXISTS ---
		if webhook != "" {
			w.logger.Info("Attempting to send result to webhook", slog.String("url", webhook))
			jsonData, err := json.Marshal(payload)
			if err == nil {
				// Fire and forget (don't block the proplet if the user's server is down)
				go func() {
					// Use a short timeout to avoid hanging if the user's server is unresponsive
					client := &http.Client{Timeout: 5 * time.Second}
					resp, err := client.Post(webhook, "application/json", bytes.NewReader(jsonData))
					if err != nil {
						w.logger.Warn("Webhook delivery failed", slog.String("error", err.Error()))
					} else {
						defer resp.Body.Close()
						w.logger.Info("Webhook delivered successfully", slog.Int("status", resp.StatusCode))
					}
				}()
			}
		}
		// ---------------------------------------------------------

		w.logger.Info("Finished running app", slog.String("id", id))
	}()

	time.Sleep(5 * time.Second)

	return nil
}

func (w *wazeroRuntime) StopApp(ctx context.Context, id string) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	r, exists := w.runtimes[id]
	if !exists {
		return errors.New("there is no runtime for this id")
	}

	if err := r.Close(ctx); err != nil {
		return err
	}

	delete(w.runtimes, id)

	return nil
}
