package proplet

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	pkgmqtt "github.com/absmach/propeller/pkg/mqtt"
	"github.com/absmach/propeller/task"
)

const pollingInterval = 5 * time.Second

var (
	aliveTopicTemplate        = "m/%s/c/%s/messages/control/proplet/alive"
	discoveryTopicTemplate    = "m/%s/c/%s/messages/control/proplet/create"
	startTopicTemplate        = "m/%s/c/%s/messages/control/manager/start"
	stopTopicTemplate         = "m/%s/c/%s/messages/control/manager/stop"
	registryResponseTopic     = "m/%s/c/%s/messages/registry/server"
	fetchRequestTopicTemplate = "m/%s/c/%s/messages/registry/proplet"
	cacheUpdateTopicTemplate  = "m/%s/c/%s/messages/control/proplet/cache_update"
)

type PropletService struct {
	domainID           string
	channelID          string
	clientID           string
	clientKey          string
	k8sNamespace       string
	livelinessInterval time.Duration
	pubsub             pkgmqtt.PubSub
	chunks             map[string][][]byte
	chunkMetadata      map[string]*ChunkPayload
	chunksMutex        sync.Mutex
	runtime            Runtime
	logger             *slog.Logger
	dataDir            string
}

type ChunkPayload struct {
	AppName     string `json:"app_name"`
	ChunkIdx    int    `json:"chunk_idx"`
	TotalChunks int    `json:"total_chunks"`
	Data        []byte `json:"data"`
}

func NewService(ctx context.Context, domainID, channelID, clientID, clientKey, k8sNamespace string, livelinessInterval time.Duration, pubsub pkgmqtt.PubSub, logger *slog.Logger, runtime Runtime, dataDir string) (*PropletService, error) {
	topic := fmt.Sprintf(discoveryTopicTemplate, domainID, channelID)
	payload := map[string]interface{}{
		"namespace":  k8sNamespace,
		"proplet_id": clientID,
	}
	if err := pubsub.Publish(ctx, topic, payload); err != nil {
		return nil, errors.Join(errors.New("failed to publish discovery"), err)
	}

	p := &PropletService{
		domainID:           domainID,
		channelID:          channelID,
		clientID:           clientID,
		clientKey:          clientKey,
		k8sNamespace:       k8sNamespace,
		livelinessInterval: livelinessInterval,
		pubsub:             pubsub,
		chunks:             make(map[string][][]byte),
		chunkMetadata:      make(map[string]*ChunkPayload),
		runtime:            runtime,
		logger:             logger,
		dataDir:            dataDir,
	}

	go p.startLivelinessUpdates(ctx)

	return p, nil
}

func (p *PropletService) Run(ctx context.Context, logger *slog.Logger) error {
	topic := fmt.Sprintf(startTopicTemplate, p.domainID, p.channelID)
	if err := p.pubsub.Subscribe(ctx, topic, p.handleStartCommand(ctx)); err != nil {
		return fmt.Errorf("failed to subscribe to start topic: %w", err)
	}

	topic = fmt.Sprintf(stopTopicTemplate, p.domainID, p.channelID)
	if err := p.pubsub.Subscribe(ctx, topic, p.handleStopCommand(ctx)); err != nil {
		return fmt.Errorf("failed to subscribe to stop topic: %w", err)
	}

	topic = fmt.Sprintf(registryResponseTopic, p.domainID, p.channelID) + "/" + p.clientID
	if err := p.pubsub.Subscribe(ctx, topic, p.handleChunk(ctx)); err != nil {
		return fmt.Errorf("failed to subscribe to registry topics: %w", err)
	}

	logger.Info("Proplet service is running.")
	<-ctx.Done()

	return nil
}

func (p *PropletService) startLivelinessUpdates(ctx context.Context) {
	ticker := time.NewTicker(p.livelinessInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			p.logger.Info("stopping liveliness updates")

			return
		case <-ticker.C:
			topic := fmt.Sprintf(aliveTopicTemplate, p.domainID, p.channelID)
			payload := map[string]interface{}{
				"status":     "alive",
				"namespace":  p.k8sNamespace,
				"proplet_id": p.clientID,
			}

			if err := p.pubsub.Publish(ctx, topic, payload); err != nil {
				p.logger.Error("failed to publish liveliness message", slog.Any("error", err))
			}

			p.logger.Debug("Published liveliness message", slog.String("topic", topic))
		}
	}
}

func (p *PropletService) handleStartCommand(ctx context.Context) func(topic string, msg map[string]interface{}) error {
	return func(topic string, msg map[string]interface{}) error {

		// --- START OF NEW CODE ---
		// Check if the message is addressed to this specific proplet.
		targetID, ok := msg["target_proplet_id"].(string)
		if !ok {
			p.logger.Warn("start command received without a target_proplet_id")
			return nil
		}

		if targetID != p.clientID {
			p.logger.Debug("Ignoring start command meant for another proplet", slog.String("target_id", targetID))
			return nil
		}
		// --- END OF NEW CODE ---

		data, err := json.Marshal(msg)
		if err != nil {
			return err
		}

		var payload task.Task
		if err := json.Unmarshal(data, &payload); err != nil {
			return err
		}

		req := startRequest{
			ID:           payload.ID,
			CLIArgs:      payload.CLIArgs,
			FunctionName: payload.Name,
			WasmFile:     payload.File,
			imageURL:     payload.ImageURL,
			Params:       payload.Inputs,
			Daemon:       payload.Daemon,
			Env:          payload.Env,
		}
		if err := req.Validate(); err != nil {
			return err
		}

		p.logger.Info("Received start command", slog.String("app_name", req.FunctionName))

		// --- START OF NEW LOGIC ---

		// If the task comes from an image URL, check for a cached copy first.
		if req.imageURL != "" {
			safeFilename := strings.ReplaceAll(req.imageURL, "/", "_")
			filePath := filepath.Join(p.dataDir, safeFilename)

			wasmBinary, err := os.ReadFile(filePath)
			if err == nil {
				// Cache hit
				p.logger.Info("Found cached Wasm binary locally", slog.String("path", filePath))
				if err := p.runtime.StartApp(ctx, wasmBinary, req.CLIArgs, req.ID, req.FunctionName, req.Daemon, req.Env, req.Params...); err != nil {
					p.logger.Error("Failed to start app from cache", slog.String("app_name", req.imageURL), slog.Any("error", err))
					return err
				}
				return nil
			}
		}
		// --- END OF NEW LOGIC ---

		if req.WasmFile != nil {
			if err := p.runtime.StartApp(ctx, req.WasmFile, req.CLIArgs, req.ID, req.FunctionName, req.Daemon, req.Env, req.Params...); err != nil {
				return err
			}

			return nil
		}

		pl := map[string]interface{}{
			"app_name": req.imageURL,
			"proplet_id": p.clientID,
		}
		tp := fmt.Sprintf(fetchRequestTopicTemplate, p.domainID, p.channelID)
		if err := p.pubsub.Publish(ctx, tp, pl); err != nil {
			return err
		}

		go func() {
			p.logger.Info("Waiting for chunks", slog.String("app_name", req.imageURL))

			for {
				p.chunksMutex.Lock()
				metadata, exists := p.chunkMetadata[req.imageURL]
				receivedChunks := len(p.chunks[req.imageURL])
				p.chunksMutex.Unlock()

				if exists && receivedChunks == metadata.TotalChunks {
					p.logger.Info("All chunks received, deploying app", slog.String("app_name", req.imageURL))
					wasmBinary := assembleChunks(p.chunks[req.imageURL])

					// --- START OF MODIFIED SECTION ---

					safeFilename := strings.ReplaceAll(req.imageURL, "/", "_")
					filePath := filepath.Join(p.dataDir, safeFilename)
					if err := os.WriteFile(filePath, wasmBinary, 0644); err != nil {
						p.logger.Error("Failed to save Wasm binary to cache", slog.String("path", filePath), slog.Any("error", err))
					} else {
						p.logger.Info("Saved Wasm binary to cache", slog.String("path", filePath))
					}

					//START OF NEW CODE
					cacheUpdateTopic := fmt.Sprintf(cacheUpdateTopicTemplate, p.domainID, p.channelID)
					cacheUpdatePayload := map[string]interface{}{
						"proplet_id": p.clientID,
						"image_url":  req.imageURL,
					}
					if err := p.pubsub.Publish(ctx, cacheUpdateTopic, cacheUpdatePayload); err != nil {
						p.logger.Error("Failed to publish cache update message", slog.Any("error", err))
					} else {
						p.logger.Info("Published cache update message", slog.String("image_url", req.imageURL))
					}

					// Clean up the in-memory chunk storage for this app to prevent memory leaks
					p.chunksMutex.Lock()
					delete(p.chunks, req.imageURL)
					delete(p.chunkMetadata, req.imageURL)
					p.chunksMutex.Unlock()

					//END OF NEW CODE
					// --- END OF MODIFIED SECTION ---
					
					if err := p.runtime.StartApp(ctx, wasmBinary, req.CLIArgs, req.ID, req.FunctionName, req.Daemon, req.Env, req.Params...); err != nil {
						p.logger.Error("Failed to start app", slog.String("app_name", req.imageURL), slog.Any("error", err))
					}

					break
				}

				time.Sleep(pollingInterval)
			}
		}()

		return nil
	}
}

func (p *PropletService) handleStopCommand(ctx context.Context) func(topic string, msg map[string]interface{}) error {
	return func(topic string, msg map[string]interface{}) error {
		data, err := json.Marshal(msg)
		if err != nil {
			return err
		}

		var req stopRequest
		if err := json.Unmarshal(data, &req); err != nil {
			return err
		}

		if err := req.Validate(); err != nil {
			return err
		}

		if err := p.runtime.StopApp(ctx, req.ID); err != nil {
			return err
		}

		return nil
	}
}

func (p *PropletService) handleChunk(_ context.Context) func(topic string, msg map[string]interface{}) error {
	return func(topic string, msg map[string]interface{}) error {
		data, err := json.Marshal(msg)
		if err != nil {
			return err
		}

		var chunk ChunkPayload
		if err := json.Unmarshal(data, &chunk); err != nil {
			return err
		}

		if err := chunk.Validate(); err != nil {
			return err
		}

		p.chunksMutex.Lock()
		defer p.chunksMutex.Unlock()

		if _, exists := p.chunkMetadata[chunk.AppName]; !exists {
			p.chunkMetadata[chunk.AppName] = &chunk
		}

		p.chunks[chunk.AppName] = append(p.chunks[chunk.AppName], chunk.Data)

		log.Printf("Received chunk %d/%d for app '%s'\n", chunk.ChunkIdx+1, chunk.TotalChunks, chunk.AppName)

		return nil
	}
}

func assembleChunks(chunks [][]byte) []byte {
	var wasmBinary []byte
	for _, chunk := range chunks {
		wasmBinary = append(wasmBinary, chunk...)
	}

	return wasmBinary
}

func (c *ChunkPayload) Validate() error {
	if c.AppName == "" {
		return errors.New("chunk validation: app_name is required but missing")
	}
	if c.ChunkIdx < 0 || c.TotalChunks <= 0 {
		return fmt.Errorf("chunk validation: invalid chunk_idx (%d) or total_chunks (%d)", c.ChunkIdx, c.TotalChunks)
	}
	if len(c.Data) == 0 {
		return errors.New("chunk validation: data is empty")
	}

	return nil
}
