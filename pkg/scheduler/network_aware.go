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
			// Rows: Where the User is located
			// Cols: Where the Workers are located
			// ~ AWS Latencies (simulated)
			"eu-central-1":   {"eu-north-1": 23, "eu-south-1": 12, "eu-west-1": 21, "eusc-de-east-1": 19},
			"eu-central-2":   {"eu-north-1": 28, "eu-south-1": 7,  "eu-west-1": 29, "eusc-de-east-1": 33},
			"eu-north-1":     {"eu-north-1": 1,  "eu-south-1": 31, "eu-west-1": 37, "eusc-de-east-1": 35},
			"eu-south-1":     {"eu-north-1": 31, "eu-south-1": 1,  "eu-west-1": 25, "eusc-de-east-1": 44},
			"eu-south-2":     {"eu-north-1": 69, "eu-south-1": 33, "eu-west-1": 32, "eusc-de-east-1": 80},
			"eu-west-1":      {"eu-north-1": 34, "eu-south-1": 37, "eu-west-1": 1,  "eusc-de-east-1": 53},
			"eu-west-2":      {"eu-north-1": 26, "eu-south-1": 27, "eu-west-1": 11, "eusc-de-east-1": 28},
			"eu-west-3":      {"eu-north-1": 31, "eu-south-1": 20, "eu-west-1": 18, "eusc-de-east-1": 30},
			"eusc-de-east-1": {"eu-north-1": 25, "eu-south-1": 25, "eu-west-1": 30, "eusc-de-east-1": 1},
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
		reqRegion = "eu-central-1" // Default user location if none provided
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