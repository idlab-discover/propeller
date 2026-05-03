package scheduler

import (
	"sort"
	
	"github.com/absmach/propeller/proplet"
	"github.com/absmach/propeller/task"
)

type RoundRobin struct {
	LastProplet int
}

func NewRoundRobin() Scheduler {
	return &RoundRobin{
		LastProplet: 0,
	}
}

func (r *RoundRobin) SelectProplet(t task.Task, proplets []proplet.Proplet) (proplet.Proplet, *proplet.Proplet, error) {
	if len(proplets) == 0 {
		return proplet.Proplet{}, nil, ErrNoProplet
	}

	alive := 0
	for i := range proplets {
		if proplets[i].Alive {
			alive += 1
		}
	}
	if alive == 0 {
		return proplet.Proplet{}, nil, ErrDeadProplers
	}

	if len(proplets) == 1 {
		return proplets[0], nil, nil
	}

	// --- FIX: Sort the proplets by ID to guarantee a stable order ---
	sort.Slice(proplets, func(i, j int) bool {
		return proplets[i].ID < proplets[j].ID
	})
	// ----------------------------------------------------------------------

	r.LastProplet = (r.LastProplet + 1) % len(proplets)

	p := proplets[r.LastProplet]
	if !p.Alive {
		return r.SelectProplet(t, proplets)
	}
	p.TaskCount += 1

	return p, nil, nil
}
