package proxy

import (
	"context"
	"fmt"
	"log/slog"

	pkgmqtt "github.com/absmach/propeller/pkg/mqtt"
	"github.com/absmach/propeller/proplet"
)

const (
	requestBuffer = 100 
	chunkBuffer = 10

	connTimeout    = 10
	reconnTimeout  = 1
	disconnTimeout = 250
	PubTopic       = "m/%s/c/%s/messages/registry/server"
	SubTopic       = "m/%s/c/%s/messages/registry/proplet"
)

type ProxyService struct {
	orasconfig    HTTPProxyConfig
	pubsub        pkgmqtt.PubSub
	domainID      string
	channelID     string
	logger        *slog.Logger
	containerChan chan FetchRequest
	dataChan      chan Targetedchunk
}

type FetchRequest struct {
	AppName  string
	PropletID string
}

type Targetedchunk struct {
	Payload   proplet.ChunkPayload
	TargetPropletID string
}

func NewService(ctx context.Context, pubsub pkgmqtt.PubSub, domainID, channelID string, httpCfg HTTPProxyConfig, logger *slog.Logger) (*ProxyService, error) {
	return &ProxyService{
		orasconfig:    httpCfg,
		pubsub:        pubsub,
		domainID:      domainID,
		channelID:     channelID,
		logger:        logger,
		containerChan: make(chan FetchRequest, requestBuffer),
		dataChan:      make(chan Targetedchunk, chunkBuffer),
	}, nil
}

func (s *ProxyService) ContainerChan() chan<- FetchRequest {
	return s.containerChan
}

func (s *ProxyService) StreamHTTP(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case request := <-s.containerChan:
			chunks, err := s.orasconfig.FetchFromReg(ctx, request.AppName, s.orasconfig.ChunkSize)
			if err != nil {
				s.logger.Error("failed to fetch container",
					slog.Any("container name", request.AppName),
					slog.Any("error", err))

				continue
			}

			// Send each chunk through the data channel
			for _, chunk := range chunks {
				targetedChunk := Targetedchunk{
					Payload:       chunk,
					TargetPropletID: request.PropletID,
				}
				select {
				case s.dataChan <- targetedChunk:
					s.logger.Info("sent container chunk to MQTT stream",
						slog.Any("container", request.AppName),
						slog.Int("chunk", chunk.ChunkIdx),
						slog.Int("total", chunk.TotalChunks))
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
	}
}

func (s *ProxyService) StreamMQTT(ctx context.Context) error {
	containerChunks := make(map[string]int)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case targetedChunk := <-s.dataChan:
			topic := fmt.Sprintf(PubTopic, s.domainID, s.channelID) + "/" + targetedChunk.TargetPropletID
			if err := s.pubsub.Publish(ctx, topic, targetedChunk.Payload); err != nil {
				s.logger.Error("failed to publish container chunk",
					slog.Any("error", err),
					slog.Int("chunk", targetedChunk.Payload.ChunkIdx),
					slog.Int("total", targetedChunk.Payload.TotalChunks))

				continue
			}

			appName := targetedChunk.Payload.AppName
			containerChunks[appName]++

			if containerChunks[appName] == targetedChunk.Payload.TotalChunks {
				s.logger.Info("successfully sent all chunks",
					slog.String("container", appName),
					slog.Int("total_chunks", targetedChunk.Payload.TotalChunks))
				delete(containerChunks, appName)
			}
		}
	}
}
