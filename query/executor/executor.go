package executor

import (
	m3client "github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/x/ident"
	"github.com/rmravindran/boostdb/client"
	"github.com/rmravindran/boostdb/query/base"
	"github.com/rmravindran/boostdb/stdlib"
)

type Executor struct {

	// Namespace
	namespace string

	// Boost client session
	session *client.BoostSession

	// Map of all series families
	seriesFamilies map[string]*client.M3DBSeriesFamily

	// Iterators for the fetched series
	seriesIterators map[string]client.BoostSeriesIterator

	// WhereOp ColumnName dependencies
	whereOpColumnNames []string
}

func NewExecutor(namespace string, session m3client.Session, maxSymTables int) *Executor {
	boostSession := client.NewBoostSession(
		session,
		maxSymTables,
		8)
	return &Executor{
		namespace:          namespace,
		session:            boostSession,
		seriesFamilies:     make(map[string]*client.M3DBSeriesFamily),
		seriesIterators:    make(map[string]client.BoostSeriesIterator),
		whereOpColumnNames: make([]string, 0)}
}

// Execute a query plan
func (e *Executor) ExecutePlan(queryPlan *QueryPlan) error {

	// Create a plan iterator
	pi := NewPlanIterator(queryPlan)

	// Iterate through the plan nodes
	for {
		ok, err := pi.Next()
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		planNodes := pi.PlanNodes()
		for _, planNode := range planNodes {
			e.ExecutePlanNode(planNode)
		}
	}

	return nil
}

// Execute a plan node
func (e *Executor) ExecutePlanNode(planNode *ExecutablePlanNode) {
	switch planNode.planNodeType {
	case PlanNodeTypeFetch:
		e.ExecuteSourceFetchOp(planNode.name, planNode.fetchOp)
	case PlanNodeTypeSelectSeries:
		e.ExecuteSelectSeriesOp(planNode.name, planNode.expression)
	}
}

// Execute the source fetch operation. For m3db, this amounts to just simply
// creating the series family instances.
func (e *Executor) ExecuteSourceFetchOp(name string, fetchOp *SourceFetchOp) {

	// Get the series family
	_, ok := e.seriesFamilies[fetchOp.seriesFamily]
	if ok {
		return
	}

	// Create the series family
	seriesFamily := client.NewM3DBSeriesFamily(
		fetchOp.seriesFamily,
		fetchOp.domain,
		ident.StringID(e.namespace),
		1,
		e.session,
		64,
		100000000,
		8)

	// Execute the fetch operation
	e.seriesFamilies[fetchOp.seriesFamily] = seriesFamily
}

// Execute the select series operation
func (e *Executor) ExecuteSelectSeriesOp(
	name string,
	expression *stdlib.MaybeOp[base.Expression]) {

	// Ensure that the expression is a ColumnNameExpression

	// Find the series family from the expression

	// Extract the series name, generate the seriesID and use the series family
	// to fetch the series.

	// Store the seriesIterator in the seriesIterators map
}

// Execute the where operation
func (e *Executor) ExecuteWhereOp(
	name string,
	expression *stdlib.MaybeOp[base.Expression]) {

	// Now that all series have been fetched, the where operation needs to be
	// executed by creating an expression state and evaluating the expression.

	// Ensure that the expression is a LogicalExpression

	// Prepare the expression state. Record all the column name expressions in
	// the whereOpColumnNames list.

	// Iterate through all the series iterators. Then sent the columns name
	// values for all the column names in the whereOpColumnNames in the state
	// and then evaluate the expression
}
