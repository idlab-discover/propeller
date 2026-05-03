package scheduler

import (
	"sort"

	"github.com/absmach/propeller/proplet"
	"github.com/absmach/propeller/task"
)

// LocalityAware is a scheduler that prioritizes proplets that have the task's image cached.
type LocalityAware struct{}

func NewLocalityAware() Scheduler {
	return &LocalityAware{}
}

// SelectProplet implements the Scheduler interface.
func (l *LocalityAware) SelectProplet(t task.Task, proplets []proplet.Proplet) (proplet.Proplet, *proplet.Proplet, error) {
	if len(proplets) == 0 {
		return proplet.Proplet{}, nil, ErrNoProplet
	}

	warmProplets := []proplet.Proplet{}
	coldProplets := []proplet.Proplet{}

	for _, p := range proplets {
		if !p.Alive {
			continue
		}
		
		isCached := false
		if p.CachedImages != nil {
			_, isCached = p.CachedImages[t.ImageURL]
		}

		if isCached {
			warmProplets = append(warmProplets, p)
		} else {
			coldProplets = append(coldProplets, p)
		}
	}

	// 1. If there are any warm proplets, choose the one with the fewest tasks
	if len(warmProplets) > 0 {
		sort.Slice(warmProplets, func(i, j int) bool {
			return warmProplets[i].TaskCount < warmProplets[j].TaskCount
		})
		return warmProplets[0], nil, nil
	}

	// 2. If there are no warm proplets, it's a true cold start.
	// Choose the cold proplet with the fewest tasks.
	if len(coldProplets) > 0 {
		sort.Slice(coldProplets, func(i, j int) bool {
			return coldProplets[i].TaskCount < coldProplets[j].TaskCount
		})
		return coldProplets[0], nil, nil
	}

	return proplet.Proplet{}, nil, ErrDeadProplers
}