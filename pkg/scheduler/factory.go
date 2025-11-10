package scheduler

import "fmt"

func NewScheduler(name string) (Scheduler, error) {
    switch name {
    case "round-robin":
        return NewRoundRobin(), nil

	case "first-available": // Add this new case
        return NewFirstAvailable(), nil
		
    // case "locality":
    //     return NewLocalityAware(), nil
    default:
        return nil, fmt.Errorf("unknown scheduler: '%s'", name)
    }
}