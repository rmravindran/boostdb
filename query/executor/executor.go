package executor

import (
	"errors"
	"strings"
	"time"

	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
	"github.com/rmravindran/boostdb/client"
	"github.com/rmravindran/boostdb/query/base"
	"github.com/rmravindran/boostdb/stdlib"
)

type SeriesIteratorInfo struct {
	domain         string
	seriesFamily   string
	seriesName     string
	seriesId       ident.ID
	seriesIterator *client.BoostSeriesIterator
}

type SelectFieldInfo struct {
	domain         string
	seriesFamily   string
	seriesName     string
	seriesId       ident.ID
	attributeName  string
	nodeName       string
	seriesIterator *client.BoostSeriesIterator
}

// Callback function type that return the distribution factor for a series
type DistributionFactorFn func(namespace string, domain string, seriesFamily string) uint16

type Executor struct {

	// Namespace
	namespace string

	// Default domain name to be used if domain name could not be resolved
	defaultDomain string

	// Distribution factor function
	distributionFactorFn DistributionFactorFn

	// Boost client session
	session *client.BoostSession

	// Map of all series families
	seriesFamilies map[string]*client.M3DBSeriesFamily

	// Series Iterators
	seriesIterators map[string]*SeriesIteratorInfo

	// WhereOp ColumnName dependencies
	whereOpColumnNames []string

	// Result set fields
	resultSetFields []SelectFieldInfo

	// PlanNodeName to SelectFieldInfo map
	planNodeNameToSelectFieldIndex map[string]int

	// Start time
	startTime xtime.UnixNano

	// End time
	endTime xtime.UnixNano

	// Execution window size
	executionWindowSize time.Duration

	// Initial allocation size
	batchSize int

	// Is Done executing
	isDone bool

	// Error from the executor
	executionError error

	// Query Plan
	queryPlan *QueryPlan

	// Plan Iterator
	planIt *PlanIterator

	// Results of execution
	resultSet *base.ResultSet

	// Result size
	resultSize int

	// Current exection window start time
	windowStartTime xtime.UnixNano

	// Current exection window end time
	windowEndTime xtime.UnixNano

	// Number of executions
	executionCount int

	// Number of batch calls
	batchCount int

	// Pending completion from last execution
	pendingCompletionNodes []*ExecutionPendingNode
}

type ExecutionPendingNode struct {
	planNode *ExecutablePlanNode
	parents  []*ExecutablePlanNode
}

func NewExecutor(
	namespace string,
	defaultDomain string,
	distributionFactorFn DistributionFactorFn,
	queryPlan *QueryPlan,
	boostSession *client.BoostSession,
	startTime xtime.UnixNano,
	endTime xtime.UnixNano,
	executionWindowSize time.Duration,
	batchSize int) *Executor {
	return &Executor{
		namespace:                      namespace,
		defaultDomain:                  defaultDomain,
		distributionFactorFn:           distributionFactorFn,
		session:                        boostSession,
		seriesFamilies:                 make(map[string]*client.M3DBSeriesFamily),
		seriesIterators:                make(map[string]*SeriesIteratorInfo),
		whereOpColumnNames:             make([]string, 0),
		resultSetFields:                make([]SelectFieldInfo, 0),
		planNodeNameToSelectFieldIndex: make(map[string]int),
		startTime:                      startTime,
		endTime:                        endTime,
		executionWindowSize:            executionWindowSize,
		batchSize:                      batchSize,
		isDone:                         false,
		executionError:                 nil,
		queryPlan:                      queryPlan,
		planIt:                         nil,
		resultSet:                      nil,
		resultSize:                     0,
		executionCount:                 0,
		batchCount:                     0,
		pendingCompletionNodes:         make([]*ExecutionPendingNode, 0)}
}

// Execute the query plan associated with the executor. Returns an error and a
// boolean indicating whether there are results from the execution. Batch size
// is used to optimize the initial allocation size of many internal structure.
// executionWindowSize determines how many times this function will needs to be
// called until complete results are returned. After each Execute call, the
// results are available in the ResultSet. The columns information is available
// in the Fields.
func (e *Executor) Execute() (error, bool) {

	// TODO:
	// - Select attribute name
	// - Where logical expression
	// - Make the ResultSet use a sparsed representation
	// - Allocate from an arena

	if e.executionError != nil {
		return e.executionError, false
	}

	if e.isDone {
		return nil, false
	}

	if e.resultSet == nil {
		e.resultSet = base.NewResultSet(e.batchSize)
	} else {
		e.resultSet.Clear()
	}
	e.resultSize = 0

	// Before creating another execution plan, iterate through the pending
	// nodes and complete them
	if len(e.pendingCompletionNodes) > 0 {
		pendingCompletionNodes := e.pendingCompletionNodes
		e.pendingCompletionNodes = make([]*ExecutionPendingNode, 0)
		for _, nodeInfo := range pendingCompletionNodes {
			pendingCompletion, err := e.executePlanNode(
				nodeInfo.planNode, nodeInfo.parents)
			if err != nil {
				// TODO release resources
				return err, false
			}

			if pendingCompletion {
				e.pendingCompletionNodes = append(
					e.pendingCompletionNodes, nodeInfo)
			}
		}

		// We return with whatever we got from the pending node
		// procesing
		e.batchCount++
		thresholdTime := e.endTime.Add(-time.Nanosecond)
		if e.windowEndTime.After(thresholdTime) &&
			len(e.pendingCompletionNodes) == 0 {
			e.isDone = true
		}

		return nil, e.resultSize > 0
	}

	// Set the execution start and end times accodging to the window size
	if e.executionCount == 0 {
		e.windowStartTime = e.startTime
		e.windowEndTime = e.startTime.Add(e.executionWindowSize)
		if e.windowEndTime.After(e.endTime) {
			e.windowEndTime = e.endTime
		}
	} else {
		e.windowStartTime = e.windowEndTime
		e.windowEndTime = e.windowEndTime.Add(e.executionWindowSize)
		if e.windowEndTime.After(e.endTime) {
			e.windowEndTime = e.endTime
		}
	}

	e.resultSetFields = make([]SelectFieldInfo, 0, len(e.resultSetFields))

	err := e.executePlan()
	if err != nil {
		e.executionError = err
	}

	thresholdTime := e.endTime.Add(-time.Nanosecond)
	if e.windowEndTime.After(thresholdTime) &&
		len(e.pendingCompletionNodes) == 0 {
		e.isDone = true
	}
	e.batchCount++
	e.executionCount++

	return err, e.resultSize > 0
}

// Return the result set from the most recent Execute call. Note that the
// result set is valid only until the next call to Execute.
func (e *Executor) ResultSet() (*base.ResultSet, error) {
	if e.executionError != nil {
		return nil, e.executionError
	}

	return e.resultSet, nil
}

// Return the fields associated with the result set. The fields are the columns
// of the result set. The fields are valid only until the next call to Execute.
func (e *Executor) Fields() []string {
	fieldNames := make([]string, len(e.resultSetFields))

	for i, field := range e.resultSetFields {
		fieldNames[i] = field.nodeName
	}

	return fieldNames
}

//-----------------
//- PRIVATE METHODS
//-----------------

// Execute a query plan
func (e *Executor) executePlan() error {

	// Create a plan iterator, if we haven't done so yet
	e.planIt = NewPlanIterator(e.queryPlan)

	// Iterate through the plan nodes
	for {
		ok, err := e.planIt.Next()
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		planNodes := e.planIt.PlanNodes()
		for _, planNode := range planNodes {
			parents, err := e.queryPlan.Parents(planNode)
			if err == nil {
				pendingCompletion, err := e.executePlanNode(planNode, parents)
				if err != nil {
					// TODO release resources
					return err
				}

				if pendingCompletion {
					e.pendingCompletionNodes = append(
						e.pendingCompletionNodes,
						&ExecutionPendingNode{planNode, parents})
				}
			}
		}
	}

	return nil
}

// Execute a plan node
func (e *Executor) executePlanNode(
	planNode *ExecutablePlanNode,
	parents []*ExecutablePlanNode) (bool, error) {

	switch planNode.planNodeType {
	case PlanNodeTypeFetchFamily:
		return e.executeSourceFetchOp(planNode.name, planNode.fetchOp, parents)
	case PlanNodeTypeFetchSeries:
		return e.executeSeriesFetchOp(planNode.name, planNode.expression, parents)
	case PlanNodeTypeSelectSeries:
		return e.executeSelectSeriesOp(planNode.name, planNode.expression, parents)
	case PlanNodeTypeWhere:
		args := planNode.ExpressionArgs()
		argsArray := []any{}
		if args != nil {
			argsArray = args.([]any)
		}
		return e.executeWhereOp(
			planNode.name,
			planNode.expression,
			planNode.ExpressionState(),
			argsArray,
			parents)
	}

	return false, errors.New("unsupported query plan node")
}

// Execute the source fetch operation. For m3db, this amounts to just simply
// creating the series family instances.
func (e *Executor) executeSourceFetchOp(
	name string, fetchOp *SourceFetchOp, parents []*ExecutablePlanNode) (bool, error) {

	// If series family already exists, then return
	_, ok := e.seriesFamilies[name]
	if ok {
		return false, nil
	}

	// get the distribution factor
	distFactor := e.distributionFactorFn(e.namespace, fetchOp.domain, fetchOp.seriesFamily)

	// Create the series family
	seriesFamily := client.NewM3DBSeriesFamily(
		fetchOp.seriesFamily,
		fetchOp.domain,
		ident.StringID(e.namespace),
		1,
		e.session,
		distFactor,
		100000000,
		8)

	// Save it
	e.seriesFamilies[name] = seriesFamily

	return false, nil
}

// Execute the select series operation
func (e *Executor) executeSeriesFetchOp(
	name string,
	expression *stdlib.MaybeOp[base.Expression],
	parents []*ExecutablePlanNode) (bool, error) {

	// Ensure that the expression is a ColumnNameExpression

	// Split the name by the '.' and get the parts
	parts := strings.Split(name, ".")
	lenParts := len(parts)
	_, seriesName := parts[lenParts-2], parts[lenParts-1]

	// If there are multiple parents, then it is an error since the select op
	// only depends on a parent plan node of type PlanNodeFetchType
	if parents == nil {
		return false, errors.New("select series operation must have one parent")
	}
	if len(parents) != 1 {
		return false, errors.New("select series operation can only have one parent")
	}

	source := parents[0].name

	seriesId := ident.StringID(seriesName)
	// Find the series family from the expression
	seriesFamily, ok := e.seriesFamilies[source]
	if !ok {
		// TODO error
		return false, errors.New("series family referenced by the series not found")
	}

	// Extract the series name, generate the seriesID and use the series family
	// to fetch the series.
	seriesIterator, err := seriesFamily.Fetch(
		seriesId,
		e.windowStartTime,
		e.windowEndTime,
		true)

	if err != nil {
		return false, err
	}

	e.seriesIterators[name] = &SeriesIteratorInfo{
		domain:         parents[0].fetchOp.domain,
		seriesFamily:   parents[0].fetchOp.seriesFamily,
		seriesName:     seriesName,
		seriesId:       seriesId,
		seriesIterator: seriesIterator}

	return false, nil
}

// Execute the select series operation
func (e *Executor) executeSelectSeriesOp(
	name string,
	expression *stdlib.MaybeOp[base.Expression],
	parents []*ExecutablePlanNode) (bool, error) {

	// Ensure that the expression is a ColumnNameExpression

	// Split the name by the '.' and get the parts
	parts := strings.Split(name, ".")
	lenParts := len(parts)
	seriesName, attributeName := parts[lenParts-2], parts[lenParts-1]

	// If there are multiple parents, then it is an error since the select op
	// only depends on a parent plan node of type PlanNodeFetchType
	if parents == nil {
		return false, errors.New("select series operation must have one parent")
	}
	if len(parents) != 1 {
		return false, errors.New("select series operation can only have one parent")
	}

	source := parents[0].name

	// Get Series Iterator
	seriesIteratorInfo, ok := e.seriesIterators[source]
	if !ok {
		// TODO error
		return false, errors.New("series referenced by the select field not found")
	}

	seriesIterator := seriesIteratorInfo.seriesIterator

	e.resultSetFields = append(e.resultSetFields, SelectFieldInfo{
		domain:         parents[0].fetchOp.domain,
		seriesFamily:   source,
		seriesName:     seriesName,
		seriesId:       seriesIteratorInfo.seriesId,
		attributeName:  attributeName,
		nodeName:       name,
		seriesIterator: seriesIterator})

	e.planNodeNameToSelectFieldIndex[name] = len(e.resultSetFields) - 1

	return false, nil
}

// Execute the where operation
func (e *Executor) executeWhereOp(
	name string,
	expression *stdlib.MaybeOp[base.Expression],
	expressionState base.ExpressionState,
	expressionArgs []any,
	parents []*ExecutablePlanNode) (bool, error) {

	// Now that all series have been fetched, the where operation needs to be
	// executed by creating an expression state and evaluating the expression.

	// Ensure that the expression is a LogicalExpression or a
	// LiteralConstBoolExpression
	switch expression.Value().(type) {
	case *base.LogicalExpression:
		return e.executeLogicalExpression(name, expression, expressionState, expressionArgs, parents)
	case *base.LiteralBoolExpression:
		return e.executeNOPFilterExpression(name, parents)
	}

	return false, nil
}

// Execute the NOP where expression by selecting all the series in the parents
func (e *Executor) executeNOPFilterExpression(
	name string,
	parents []*ExecutablePlanNode) (bool, error) {

	// Iterate through all the series iterators and select all the values
	// upto the batch size.

	outOfSpace := false

	// Iterate through the resultSetFields and for each seriesIterator, select
	// the value and add it to the result set at the repective index.
	colSize := len(e.resultSetFields)
	e.resultSet.ReDim(colSize)
	// Map of completed fields
	fieldsCompleted := make(map[int]bool)

	// Find the select field info that is applicable to each of the parent
	// plan nodes
	resultSetFieldIndices := make([]int, 0)
	for _, parent := range parents {
		resultSetFieldIndices = append(
			resultSetFieldIndices, e.planNodeNameToSelectFieldIndex[parent.name])
	}

	// If the iterator has reached the end (from another use)
	// then restart
	for _, colIndx := range resultSetFieldIndices {
		seriesField := &e.resultSetFields[colIndx]
		//if seriesField.seriesIterator.IsDone() {
		seriesField.seriesIterator.Begin()
		//}
	}

	for _, colIndx := range resultSetFieldIndices {
		row := 0
		seriesField := &e.resultSetFields[colIndx]
		for len(fieldsCompleted) < len(resultSetFieldIndices) {
			// If the distIndx is already completed, then skip
			if fieldsCompleted[colIndx] {
				continue
			}

			if seriesField.seriesIterator.Next() {
				dp, _, _ := seriesField.seriesIterator.Current()
				e.resultSet.Resize(row + 1)
				if seriesField.attributeName == "value" {
					e.resultSet.Set(row, colIndx, dp.Value)
				} else {
					attributes := seriesField.seriesIterator.Attributes()
					if attributes != nil && attributes.Next() {
						attribute := attributes.Current()
						if attribute.Name.String() == seriesField.attributeName {
							e.resultSet.Set(row, colIndx, attribute.Value.String())
						}
					}
				}
				row++

				if row == e.batchSize {
					outOfSpace = true
					break
				}
			} else {
				fieldsCompleted[colIndx] = true
			}
		}
		e.resultSize = max(row, e.resultSize)
	}

	return outOfSpace, nil
}

// Execute the where operation
func (e *Executor) executeLogicalExpression(
	name string,
	expression *stdlib.MaybeOp[base.Expression],
	expressionState base.ExpressionState,
	expressionArgs []any,
	parents []*ExecutablePlanNode) (bool, error) {

	// Iterate through all the series iterators and select all the values
	// upto the batch size.

	outOfSpace := false

	// Iterate through the resultSetFields and for each seriesIterator, select
	// the value and add it to the result set at the repective index.
	colSize := len(e.resultSetFields)
	e.resultSet.ReDim(colSize)
	// Map of completed fields
	fieldsCompleted := make(map[int]bool)

	// Find the select field info that is applicable to each of the parent
	// plan nodes
	whereFieldIndices := make([]int, 0)
	for _, parent := range parents {
		whereFieldIndices = append(
			whereFieldIndices, e.planNodeNameToSelectFieldIndex[parent.name])
	}

	// Iterate through all the series iterators. Then sent the columns name
	// values for all the column names in the whereOpColumnNames in the state
	// and then evaluate the expression

	row := 0

	tmpValues := make([]any, len(e.resultSetFields))
	for {

		// TODO: We should evaluate the expression first and  only if the
		// expression is true, then extract the values and attributes since
		// attribute extraction might be unnecessary for many points.

		// First read all the columns's value into the array.
		for colIndx, seriesField := range e.resultSetFields {
			if fieldsCompleted[colIndx] {
				continue
			}
			if seriesField.seriesIterator.Next() {
				dp, _, _ := seriesField.seriesIterator.Current()
				if seriesField.attributeName == "value" {
					tmpValues[colIndx] = dp.Value
				} else {
					attributes := seriesField.seriesIterator.Attributes()
					if attributes != nil {
						attribute := attributes.Current()
						if attribute.Name.String() == seriesField.attributeName {
							e.resultSet.Set(row, colIndx, attribute.Value.String())
						}
					}
				}
			} else {
				fieldsCompleted[colIndx] = true
			}
		}

		for _, colIndx := range whereFieldIndices {
			seriesField := &e.resultSetFields[colIndx]
			if seriesField.attributeName == "value" {
				argName := seriesField.seriesName + ".value"
				expressionState.SetValue(argName, tmpValues[colIndx])
			} else {
				// TODO handle attributes
			}
		}

		// Evaluate the expression
		expressionArgs := expressionState.ToArgs()
		result := expression.Evaluate(expressionArgs)
		if result.Error() != nil {
			return false, result.Error()
		} else if result.Value().(*base.LiteralBoolExpression).Bool() {
			e.resultSet.Resize(row + 1)

			for colIndx := range e.resultSetFields {
				e.resultSet.Set(row, colIndx, tmpValues[colIndx])
			}
			e.resultSize = max(row, e.resultSize)
			row++
		}
		if row == e.batchSize {
			outOfSpace = true
			break
		} else if len(fieldsCompleted) == len(e.resultSetFields) {
			// All iterators are done
			break
		}
	}

	return outOfSpace, nil
}
