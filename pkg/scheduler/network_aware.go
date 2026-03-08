package scheduler

import (
	"sort"

	"github.com/absmach/propeller/proplet"
	"github.com/absmach/propeller/task"
)

// NetworkAware is a scheduler that selects nodes based on simulated network distance
type NetworkAware struct {
	// A simple static Cost Matrix. 
	// Row: User Region. Column: Proplet Region. Value: Ping Latency (ms)
	costMatrix map[string]map[string]int
}

func NewNetworkAware() Scheduler {
	return &NetworkAware{
		costMatrix: map[string]map[string]int{
			"user-gent": {
				"gent-node": 5,    
				"brussels-node": 15, 
				"paris-node": 50,   
			},
			"user-paris": {
				"gent-node": 50,
				"brussels-node": 35,
				"paris-node": 5,
			},
		},
	}
}

func (n *NetworkAware) SelectProplet(t task.Task, proplets []proplet.Proplet) (proplet.Proplet, error) {
	if len(proplets) == 0 {
		return proplet.Proplet{}, ErrNoProplet
	}

	// Filter out dead proplets
	var aliveProplets []proplet.Proplet
	for _, p := range proplets {
		if p.Alive {
			aliveProplets = append(aliveProplets, p)
		}
	}

	if len(aliveProplets) == 0 {
		return proplet.Proplet{}, ErrDeadProplers
	}

	reqRegion := t.RequesterRegion
	if reqRegion == "" {
		reqRegion = "user-gent" // Default user location if none provided
	}

	// Sort proplets based on the Cost Matrix
	sort.Slice(aliveProplets, func(i, j int) bool {
		regionI := aliveProplets[i].Region
		regionJ := aliveProplets[j].Region

		costI := 9999 // Default high cost if unknown region
		costJ := 9999

		// Look up costs
		if destMap, ok := n.costMatrix[reqRegion]; ok {
			if c, exists := destMap[regionI]; exists { costI = c }
			if c, exists := destMap[regionJ]; exists { costJ = c }
		}

		// If costs are identical, fall back to Least Loaded
		if costI == costJ {
			return aliveProplets[i].TaskCount < aliveProplets[j].TaskCount
		}

		// Otherwise, pick the lowest cost (closest node)
		return costI < costJ
	})

	return aliveProplets[0], nil
}