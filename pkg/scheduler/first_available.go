package scheduler

import (
    "github.com/absmach/propeller/proplet"
    "github.com/absmach/propeller/task"
	"sort"
)

// FirstAvailable is a simple scheduler that picks the first alive proplet.
type FirstAvailable struct{}

// NewFirstAvailable creates a new FirstAvailable scheduler.
func NewFirstAvailable() Scheduler {
    return &FirstAvailable{}
}

// SelectProplet implements the Scheduler interface.
func (f *FirstAvailable) SelectProplet(t task.Task, proplets []proplet.Proplet) (proplet.Proplet, *proplet.Proplet, error) {
    if len(proplets) == 0 {
        return proplet.Proplet{}, nil, ErrNoProplet
    }

	// Sort the proplets by ID to ensure a deterministic order
    sort.Slice(proplets, func(i, j int) bool {
        return proplets[i].ID < proplets[j].ID
    })

    for _, p := range proplets {
        if p.Alive {
            return p, nil, nil // Return the first one we find that is alive
        }
    }

    return proplet.Proplet{}, nil, ErrDeadProplers
}