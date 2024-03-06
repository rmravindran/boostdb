package executor

import "errors"

type PlanIterator struct {
	queryPlan *QueryPlan

	// Currently executing depth
	currentDepth int

	// PlanNodes at the current depth
	itPlanNodes []*ExecutablePlanNode

	// Is done
	done bool
}

// Create a new plan iterator
func NewPlanIterator(queryPlan *QueryPlan) *PlanIterator {
	return &PlanIterator{
		queryPlan:    queryPlan,
		currentDepth: -1,
		itPlanNodes:  nil,
		done:         false,
	}
}

// Get the next plan nodes
func (pi *PlanIterator) Next() (bool, error) {
	if pi.done {
		return false, errors.New("iterator is done")
	}

	if pi.currentDepth == -1 {
		pi.currentDepth = 1
	} else {
		pi.currentDepth++
	}

	pi.itPlanNodes = pi.queryPlan.NodesAtDepth(pi.currentDepth)
	if len(pi.itPlanNodes) == 0 || pi.itPlanNodes == nil {
		pi.done = true
		return false, nil
	}
	return true, nil
}

// Return true if the iterator is done, otherwise returns false
func (pi *PlanIterator) Done() bool {
	return pi.done
}

// Return the nodes at the current depth
func (pi *PlanIterator) PlanNodes() []*ExecutablePlanNode {
	return pi.itPlanNodes
}
