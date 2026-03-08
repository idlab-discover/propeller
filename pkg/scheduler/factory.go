package scheduler

import "fmt"

func NewScheduler(name string) (Scheduler, error) {
    switch name {
    case "round-robin":
        return NewRoundRobin(), nil

	case "first-available":
        return NewFirstAvailable(), nil
		
    case "locality":
        return NewLocalityAware(), nil

    case "network":  
		return NewNetworkAware(), nil

    default:
        return nil, fmt.Errorf("unknown scheduler: '%s'", name)
    }
}