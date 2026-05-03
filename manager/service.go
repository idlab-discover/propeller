package manager

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
	"bytes"
	"encoding/json"
	"net/http"

	"github.com/0x6flab/namegenerator"
	pkgerrors "github.com/absmach/propeller/pkg/errors"
	"github.com/absmach/propeller/pkg/mqtt"
	"github.com/absmach/propeller/pkg/scheduler"
	"github.com/absmach/propeller/pkg/storage"
	"github.com/absmach/propeller/proplet"
	"github.com/absmach/propeller/task"
	"github.com/google/uuid"
)

const (
	defOffset         = 0
	defLimit          = 100
	aliveHistoryLimit = 10
)

var (
	baseTopic = "m/%s/c/%s/messages"
	namegen   = namegenerator.NewGenerator()
)

type service struct {
	tasksDB       storage.Storage
	propletsDB    storage.Storage
	taskPropletDB storage.Storage
	scheduler     scheduler.Scheduler
	baseTopic     string
	pubsub        mqtt.PubSub
	logger        *slog.Logger
}

func NewService(
	tasksDB, propletsDB, taskPropletDB storage.Storage,
	s scheduler.Scheduler, pubsub mqtt.PubSub,
	domainID, channelID string, logger *slog.Logger,
) Service {
	return &service{
		tasksDB:       tasksDB,
		propletsDB:    propletsDB,
		taskPropletDB: taskPropletDB,
		scheduler:     s,
		baseTopic:     fmt.Sprintf(baseTopic, domainID, channelID),
		pubsub:        pubsub,
		logger:        logger,
	}
}

func (svc *service) GetProplet(ctx context.Context, propletID string) (proplet.Proplet, error) {
	data, err := svc.propletsDB.Get(ctx, propletID)
	if err != nil {
		return proplet.Proplet{}, err
	}
	w, ok := data.(proplet.Proplet)
	if !ok {
		return proplet.Proplet{}, pkgerrors.ErrInvalidData
	}
	w.SetAlive()

	return w, nil
}

func (svc *service) ListProplets(ctx context.Context, offset, limit uint64) (proplet.PropletPage, error) {
	data, total, err := svc.propletsDB.List(ctx, offset, limit)
	if err != nil {
		return proplet.PropletPage{}, err
	}
	proplets := make([]proplet.Proplet, total)
	for i := range data {
		w, ok := data[i].(proplet.Proplet)
		if !ok {
			return proplet.PropletPage{}, pkgerrors.ErrInvalidData
		}
		w.SetAlive()
		proplets[i] = w
	}

	return proplet.PropletPage{
		Offset:   offset,
		Limit:    limit,
		Total:    total,
		Proplets: proplets,
	}, nil
}

func (svc *service) SelectProplet(ctx context.Context, t task.Task) (proplet.Proplet, *proplet.Proplet, error) {
	proplets, err := svc.ListProplets(ctx, defOffset, defLimit)
	if err != nil {
		return proplet.Proplet{}, nil, err
	}

	return svc.scheduler.SelectProplet(t, proplets.Proplets)
}

func (svc *service) CreateTask(ctx context.Context, t task.Task) (task.Task, error) {
	t.ID = uuid.NewString()
	t.CreatedAt = time.Now()

	if err := svc.tasksDB.Create(ctx, t.ID, t); err != nil {
		return task.Task{}, err
	}

	return t, nil
}

func (svc *service) GetTask(ctx context.Context, taskID string) (task.Task, error) {
	data, err := svc.tasksDB.Get(ctx, taskID)
	if err != nil {
		return task.Task{}, err
	}
	t, ok := data.(task.Task)
	if !ok {
		return task.Task{}, pkgerrors.ErrInvalidData
	}

	return t, nil
}

func (svc *service) ListTasks(ctx context.Context, offset, limit uint64) (task.TaskPage, error) {
	data, total, err := svc.tasksDB.List(ctx, offset, limit)
	if err != nil {
		return task.TaskPage{}, err
	}

	tasks := make([]task.Task, total)
	for i := range data {
		t, ok := data[i].(task.Task)
		if !ok {
			return task.TaskPage{}, pkgerrors.ErrInvalidData
		}

		tasks[i] = t
	}

	return task.TaskPage{
		Offset: offset,
		Limit:  limit,
		Total:  total,
		Tasks:  tasks,
	}, nil
}

func (svc *service) UpdateTask(ctx context.Context, t task.Task) (task.Task, error) {
	dbT, err := svc.GetTask(ctx, t.ID)
	if err != nil {
		return task.Task{}, err
	}
	dbT.UpdatedAt = time.Now()
	if t.Name != "" {
		dbT.Name = t.Name
	}
	if t.Inputs != nil {
		dbT.Inputs = t.Inputs
	}
	if t.File != nil {
		dbT.File = t.File
	}

	if err := svc.tasksDB.Update(ctx, dbT.ID, dbT); err != nil {
		return task.Task{}, err
	}

	return dbT, nil
}

func (svc *service) DeleteTask(ctx context.Context, taskID string) error {
	return svc.tasksDB.Delete(ctx, taskID)
}

func (svc *service) StartTask(ctx context.Context, taskID string) error {
	t, err := svc.GetTask(ctx, taskID)
	if err != nil {
		return err
	}

	var p proplet.Proplet
	var prefetchNode *proplet.Proplet
	switch t.PropletID {
	case "":
		scheduleStart := time.Now()
		p, prefetchNode, err = svc.SelectProplet(ctx, t)
		if err != nil {
			return err
		}
		scheduleDuration := time.Since(scheduleStart)
		svc.logger.Info("Scheduling decision completed", 
			slog.String("task_id", t.ID),
			slog.String("selected_node", p.ID),
			slog.Duration("scheduler_time", scheduleDuration))
	default:
		p, err = svc.GetProplet(ctx, t.PropletID)
		if err != nil {
			return err
		}
		if !p.Alive {
			return fmt.Errorf("specified proplet %s is not alive", t.PropletID)
		}
	}

	svc.logger.Info("Scheduler selected proplet, preparing to send start command", slog.String("proplet_id", p.ID), slog.String("task_id", t.ID))

	payload := map[string]interface{}{
		"id":        t.ID,
		"name":      t.Name,
		"state":     t.State,
		"image_url": t.ImageURL,
		"file":      t.File,
		"inputs":    t.Inputs,
		"cli_args":  t.CLIArgs,
		"daemon":    t.Daemon,
		"env":       t.Env,
		"target_proplet_id": p.ID,
		"webhook":   t.WebHook,
	}

	topic := svc.baseTopic + "/control/manager/start"

	t.State = task.Running
	t.UpdatedAt = time.Now()
	t.StartTime = time.Now()

	if err := svc.tasksDB.Update(ctx, t.ID, t); err != nil {
		return err
	}

	if err := svc.pubsub.Publish(ctx, topic, payload); err != nil {
		return err
	}

	if err := svc.taskPropletDB.Create(ctx, taskID, p.ID); err != nil {
		return err
	}

	p.TaskCount++
	if err := svc.propletsDB.Update(ctx, p.ID, p); err != nil {
		return err
	}

	// IF the scheduler returned a prefetch node, send the command!
	if prefetchNode != nil {
		svc.logger.Info("Triggering background prefetch", 
			slog.String("target_proplet", prefetchNode.ID), 
			slog.String("image", t.ImageURL))

		prefetchPayload := map[string]interface{}{
			"image_url": t.ImageURL,
		}
		
		// Create a targeted topic for the prefetch node
		prefetchTopic := fmt.Sprintf("%s/control/manager/prefetch/%s", svc.baseTopic, prefetchNode.ID)
			
		// Fire and forget
		go svc.pubsub.Publish(context.Background(), prefetchTopic, prefetchPayload)
	}
	
	return nil
}

func (svc *service) StopTask(ctx context.Context, taskID string) error {
	t, err := svc.GetTask(ctx, taskID)
	if err != nil {
		return err
	}

	data, err := svc.taskPropletDB.Get(ctx, taskID)
	if err != nil {
		return err
	}
	propellerID, ok := data.(string)
	if !ok {
		return pkgerrors.ErrInvalidData
	}
	p, err := svc.GetProplet(ctx, propellerID)
	if err != nil {
		return err
	}

	topic := svc.baseTopic + "/control/manager/stop"
	if err := svc.pubsub.Publish(ctx, topic, t); err != nil {
		return err
	}

	if err := svc.taskPropletDB.Delete(ctx, taskID); err != nil {
		return err
	}

	p.TaskCount--
	if err := svc.propletsDB.Update(ctx, p.ID, p); err != nil {
		return err
	}

	return nil
}

func (svc *service) Subscribe(ctx context.Context) error {
	topic := svc.baseTopic + "/#"

	if err := svc.pubsub.Subscribe(ctx, topic, svc.handle(ctx)); err != nil {
		return err
	}

	return nil
}

func (svc *service) handle(ctx context.Context) func(topic string, msg map[string]interface{}) error {
	return func(topic string, msg map[string]interface{}) error {
		switch topic {
		case svc.baseTopic + "/control/proplet/create":
			if err := svc.createPropletHandler(ctx, msg); err != nil {
				return err
			}
			svc.logger.InfoContext(ctx, "successfully created proplet")
		case svc.baseTopic + "/control/proplet/alive":
			return svc.updateLivenessHandler(ctx, msg)
		case svc.baseTopic + "/control/proplet/results":
			return svc.updateResultsHandler(ctx, msg)
		case svc.baseTopic + "/control/proplet/cache_update":
			return svc.updateCacheHandler(ctx, msg)
		}

		return nil
	}
}

func (svc *service) createPropletHandler(ctx context.Context, msg map[string]interface{}) error {
	propletID, ok := msg["proplet_id"].(string)
	if !ok {
		return errors.New("invalid proplet_id")
	}
	if propletID == "" {
		return errors.New("proplet id is empty")
	}

	region, _ := msg["region"].(string)

	p := proplet.Proplet{
		ID:   propletID,
		Name: namegen.Generate(),
		Region: region,
	}
	if err := svc.propletsDB.Create(ctx, p.ID, p); err != nil {
		return err
	}

	return nil
}

func (svc *service) updateLivenessHandler(ctx context.Context, msg map[string]interface{}) error {
	propletID, ok := msg["proplet_id"].(string)
	if !ok {
		return errors.New("invalid proplet_id")
	}
	if propletID == "" {
		return errors.New("proplet id is empty")
	}

	p, err := svc.GetProplet(ctx, propletID)
	if errors.Is(err, pkgerrors.ErrNotFound) {
		return svc.createPropletHandler(ctx, msg)
	}
	if err != nil {
		return err
	}
	if reg, ok := msg["region"].(string); ok && reg != "" {
		p.Region = reg
	}

	p.Alive = true
	p.AliveHistory = append(p.AliveHistory, time.Now())
	if len(p.AliveHistory) > aliveHistoryLimit {
		p.AliveHistory = p.AliveHistory[1:]
	}
	if err := svc.propletsDB.Update(ctx, propletID, p); err != nil {
		return err
	}

	return nil
}

func (svc *service) updateResultsHandler(ctx context.Context, msg map[string]interface{}) error {
	taskID, ok := msg["task_id"].(string)
	if !ok {
		return errors.New("invalid task_id")
	}
	if taskID == "" {
		return errors.New("task id is empty")
	}

	t, err := svc.GetTask(ctx, taskID)
	if err != nil {
		return err
	}
	t.Results = msg["results"]
	t.State = task.Completed
	t.UpdatedAt = time.Now()
	t.FinishTime = time.Now()

	if errMsg, ok := msg["error"].(string); ok {
		t.Error = errMsg
	}

	if err := svc.tasksDB.Update(ctx, t.ID, t); err != nil {
		return err
	}

	// --- NEW: FIRE WEBHOOK FROM MANAGER ---
	if t.WebHook != "" {
		go func(webhookURL string, taskData task.Task) {
			// Add a flag so the Python script knows this came from the Manager
			payload := map[string]interface{}{
				"source":  "manager",
				"task_id": taskData.ID,
				"results": taskData.Results,
				"error":   taskData.Error,
			}
			jsonData, _ := json.Marshal(payload)
			
			client := &http.Client{Timeout: 5 * time.Second}
			resp, err := client.Post(webhookURL, "application/json", bytes.NewReader(jsonData))
			if err != nil {
				svc.logger.Warn("Manager webhook failed", slog.String("error", err.Error()))
			} else {
				resp.Body.Close()
				svc.logger.Info("Manager webhook delivered")
			}
		}(t.WebHook, t)
	}
	// --------------------------------------

	return nil
}

func (svc *service) updateCacheHandler(ctx context.Context, msg map[string]interface{}) error {
    propletID, ok := msg["proplet_id"].(string)
    if !ok {
        return errors.New("invalid proplet_id in cache update message")
    }
    imageURL, ok := msg["image_url"].(string)
    if !ok {
        return errors.New("invalid image_url in cache update message")
    }

    p, err := svc.GetProplet(ctx, propletID)
    if err != nil {
        return fmt.Errorf("failed to get proplet for cache update: %w", err)
    }

    // Initialize the map if it's nil
    if p.CachedImages == nil {
        p.CachedImages = make(map[string]bool)
    }

    p.CachedImages[imageURL] = true

    if err := svc.propletsDB.Update(ctx, p.ID, p); err != nil {
        return fmt.Errorf("failed to update proplet with new cache info: %w", err)
    }

    svc.logger.Info("Updated proplet cache", slog.String("proplet_id", propletID), slog.String("image_url", imageURL))
    return nil
}