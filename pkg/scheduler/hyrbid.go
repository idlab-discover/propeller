package scheduler

import (
	"sort"
	"strings"

	"github.com/absmach/propeller/proplet"
	"github.com/absmach/propeller/task"
)

type Hybrid struct {
	costMatrix map[string]map[string]int
}

func NewHybrid() Scheduler {
	return &Hybrid{
		costMatrix: map[string]map[string]int{
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

func (h *Hybrid) SelectProplet(t task.Task, proplets []proplet.Proplet) (proplet.Proplet, *proplet.Proplet, error) {
	if len(proplets) == 0 {
		return proplet.Proplet{}, nil, ErrNoProplet
	}

	var aliveProplets []proplet.Proplet
	for _, p := range proplets {
		if p.Alive {
			aliveProplets = append(aliveProplets, p)
		}
	}

	if len(aliveProplets) == 0 {
		return proplet.Proplet{}, nil, ErrDeadProplers
	}

	reqRegion := t.RequesterRegion
	if reqRegion == "" {
		reqRegion = "eu-central-1"
	}

	// 1. DETERMINE DOWNLOAD PENALTY
	fetchPenalty := 150 // default for small app
	if strings.Contains(t.ImageURL, "heavy") {
		fetchPenalty = 700 // default for heavy app
	}

	type scoredProplet struct {
		p             proplet.Proplet
		totalCost     int
		routingCost   int
		isCached      bool
	}
	var scored []scoredProplet

	// 2. SCORE ALL NODES
	for _, p := range aliveProplets {
		isCached := false
		if p.CachedImages != nil {
			_, isCached = p.CachedImages[t.ImageURL]
		}

		currentFetchPenalty := fetchPenalty
		if isCached {
			currentFetchPenalty = 0
		}

		routingCost := 9999
		if destMap, ok := h.costMatrix[reqRegion]; ok {
			if c, exists := destMap[p.Region]; exists {
				routingCost = c
			}
		}

		totalCost := currentFetchPenalty + routingCost
		scored = append(scored, scoredProplet{p, totalCost, routingCost, isCached})
	}

	// 3. FIND THE BEST EXECUTION NODE (Lowest Total Cost)
	sort.Slice(scored, func(i, j int) bool {
		if scored[i].totalCost == scored[j].totalCost {
			return scored[i].p.TaskCount < scored[j].p.TaskCount
		}
		return scored[i].totalCost < scored[j].totalCost
	})
	executionNode := scored[0].p

	// 4. DETERMINE PREFETCH NODE
	// If the Execution Node is NOT the absolute closest node to the user, 
	// prefetch on the closest node for the future.
	
	var prefetchNode *proplet.Proplet = nil

	// Sort purely by Routing Cost to find the absolute closest node
	sort.Slice(scored, func(i, j int) bool {
		return scored[i].routingCost < scored[j].routingCost
	})
	closestNode := scored[0]

	// If the closest node doesn't have the image, tell it to fetch it!
	if closestNode.p.ID != executionNode.ID && !closestNode.isCached {
		prefetchNode = &closestNode.p
	}

	return executionNode, prefetchNode, nil
}