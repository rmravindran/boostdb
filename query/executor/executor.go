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

// Return the fully qualified series name from the select field info
func (s *SelectFieldInfo) fullyQualifiedSeriesName() string {
	return s.domain + "." + s.seriesFamily + "." + s.seriesName
}

// Return the fully qualifed select field name from the series iterator info
func (s *SelectFieldInfo) fullyQualifiedFieldName() string {
	return s.domain + "." + s.seriesFamily + "." + s.seriesName + "." + s.attributeName
}

type NOPWhereExecState struct {
	iteratorPos map[int]*client.BoostSeriesIteratorPosition
}

type LogicalWhereExecState struct {
	whereIterPos map[int]*client.BoostSeriesIteratorPosition
	iteratorPos  map[int]*client.BoostSeriesIteratorPosition
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

	// Where only fields
	whereOnlyFields map[string]*SelectFieldInfo

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

	// Number of batch calls
	batchCount int

	// Number of time slices during execution
	timeSlices int

	// Pending completion from last execution
	pendingCompletionNodes []*ExecutionPendingNode
}

type ExecutionPendingNode struct {
	planNode  *ExecutablePlanNode
	parents   []*ExecutablePlanNode
	execState interface{}
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
	ex := &Executor{
		namespace:                      namespace,
		defaultDomain:                  defaultDomain,
		distributionFactorFn:           distributionFactorFn,
		session:                        boostSession,
		seriesFamilies:                 make(map[string]*client.M3DBSeriesFamily),
		seriesIterators:                make(map[string]*SeriesIteratorInfo),
		whereOpColumnNames:             make([]string, 0),
		resultSetFields:                make([]SelectFieldInfo, 0),
		whereOnlyFields:                make(map[string]*SelectFieldInfo),
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
		batchCount:                     0,
		timeSlices:                     0,
		pendingCompletionNodes:         make([]*ExecutionPendingNode, 0)}

	return ex
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
			pendingCompletion, execState, err := e.executePlanNode(
				nodeInfo.planNode, nodeInfo.parents, nodeInfo.execState)
			nodeInfo.execState = execState
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
	if e.timeSlices == 0 {
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
	for _, selectFieldName := range e.queryPlan.selectFieldNames {
		e.resultSetFields = append(e.resultSetFields, SelectFieldInfo{})
		e.planNodeNameToSelectFieldIndex[selectFieldName] = len(e.resultSetFields) - 1
	}

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
	e.timeSlices++

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

// Return the number of batched executions performed by this executor
func (e *Executor) NumBatchesExecuted() int {
	return e.batchCount
}

// Return the number of time slices performed during the execution
func (e *Executor) NumTimeSlices() int {
	return e.timeSlices
}

// Return the size of the batch that was provided during the construction of
// this executor
func (e *Executor) BatchSize() int {
	return e.batchSize
}

// Return the most recently executed time slice. Note that returned value is
// valid only after the first execution.
func (e *Executor) CurrentTimeSlice() (xtime.UnixNano, xtime.UnixNano) {
	return e.windowStartTime, e.windowEndTime
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
				pendingCompletion, execState, err := e.executePlanNode(planNode, parents, nil)
				if err != nil {
					// TODO release resources
					return err
				}

				if pendingCompletion {
					e.pendingCompletionNodes = append(
						e.pendingCompletionNodes,
						&ExecutionPendingNode{planNode, parents, execState})
				}
			}
		}
	}

	return nil
}

// Execute a plan node
func (e *Executor) executePlanNode(
	planNode *ExecutablePlanNode,
	parents []*ExecutablePlanNode,
	prevExecutorState interface{}) (bool, interface{}, error) {

	switch planNode.planNodeType {
	case PlanNodeTypeFetchFamily:
		return e.executeSourceFetchOp(planNode.name, planNode.fetchOp, parents, prevExecutorState)
	case PlanNodeTypeFetchSeries:
		return e.executeSeriesFetchOp(planNode.name, planNode.expression, parents, prevExecutorState)
	case PlanNodeTypeSelectSeries:
		return e.executeSelectSeriesOp(planNode.name, planNode.expression, parents, prevExecutorState)
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
			parents,
			prevExecutorState)
	}

	return false, nil, errors.New("unsupported query plan node")
}

// Execute the source fetch operation. For m3db, this amounts to just simply
// creating the series family instances.
func (e *Executor) executeSourceFetchOp(
	name string, fetchOp *SourceFetchOp,
	parents []*ExecutablePlanNode,
	prevExecutorState interface{}) (bool, interface{}, error) {

	// If series family already exists, then return
	_, ok := e.seriesFamilies[name]
	if ok {
		return false, nil, nil
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

	return false, nil, nil
}

// Execute the select series operation
func (e *Executor) executeSeriesFetchOp(
	name string,
	expression *stdlib.MaybeOp[base.Expression],
	parents []*ExecutablePlanNode,
	prevExecutorState interface{}) (bool, interface{}, error) {

	// Ensure that the expression is a ColumnNameExpression

	// Split the name by the '.' and get the parts
	parts := strings.Split(name, ".")
	lenParts := len(parts)
	_, seriesName := parts[lenParts-2], parts[lenParts-1]

	// If there are multiple parents, then it is an error since the select op
	// only depends on a parent plan node of type PlanNodeFetchType
	if parents == nil {
		return false, nil, errors.New("select series operation must have one parent")
	}
	if len(parents) != 1 {
		return false, nil, errors.New("select series operation can only have one parent")
	}

	source := parents[0].name

	seriesId := ident.StringID(seriesName)
	// Find the series family from the expression
	seriesFamily, ok := e.seriesFamilies[source]
	if !ok {
		// TODO error
		return false, nil, errors.New("series family referenced by the series not found")
	}

	// Extract the series name, generate the seriesID and use the series family
	// to fetch the series.
	seriesIterator, err := seriesFamily.Fetch(
		seriesId,
		e.windowStartTime,
		e.windowEndTime,
		true)

	if err != nil {
		return false, nil, err
	}

	e.seriesIterators[name] = &SeriesIteratorInfo{
		domain:         parents[0].fetchOp.domain,
		seriesFamily:   parents[0].fetchOp.seriesFamily,
		seriesName:     seriesName,
		seriesId:       seriesId,
		seriesIterator: seriesIterator}

	return false, nil, nil
}

// Execute the select series operation
func (e *Executor) executeSelectSeriesOp(
	name string,
	expression *stdlib.MaybeOp[base.Expression],
	parents []*ExecutablePlanNode,
	prevExcecutorState interface{}) (bool, interface{}, error) {

	// Ensure that the expression is a ColumnNameExpression

	// Split the name by the '.' and get the parts
	parts := strings.Split(name, ".")
	lenParts := len(parts)
	seriesName, attributeName := parts[lenParts-2], parts[lenParts-1]

	// If there are multiple parents, then it is an error since the select op
	// only depends on a parent plan node of type PlanNodeFetchType
	if parents == nil {
		return false, nil, errors.New("select series operation must have one parent")
	}
	if len(parents) != 1 {
		return false, nil, errors.New("select series operation can only have one parent")
	}

	source := parents[0].name

	// Get Series Iterator
	seriesIteratorInfo, ok := e.seriesIterators[source]
	if !ok {
		// TODO error
		return false, nil, errors.New("series referenced by the select field not found")
	}

	seriesIterator := seriesIteratorInfo.seriesIterator

	index, ok := e.planNodeNameToSelectFieldIndex[name]
	if ok {
		e.resultSetFields[index] = SelectFieldInfo{
			domain:         parents[0].fetchOp.domain,
			seriesFamily:   seriesIteratorInfo.seriesFamily,
			seriesName:     seriesName,
			seriesId:       seriesIteratorInfo.seriesId,
			attributeName:  attributeName,
			nodeName:       name,
			seriesIterator: seriesIterator}
	} else {
		e.whereOnlyFields[name] = &SelectFieldInfo{
			domain:         parents[0].fetchOp.domain,
			seriesFamily:   seriesIteratorInfo.seriesFamily,
			seriesName:     seriesName,
			seriesId:       seriesIteratorInfo.seriesId,
			attributeName:  attributeName,
			nodeName:       name,
			seriesIterator: seriesIterator}
	}

	//e.planNodeNameToSelectFieldIndex[name] = len(e.resultSetFields) - 1

	return false, nil, nil
}

// Execute the where operation
func (e *Executor) executeWhereOp(
	name string,
	expression *stdlib.MaybeOp[base.Expression],
	expressionState base.ExpressionState,
	expressionArgs []any,
	parents []*ExecutablePlanNode,
	prevExecutorState interface{}) (bool, interface{}, error) {

	// Now that all series have been fetched, the where operation needs to be
	// executed by creating an expression state and evaluating the expression.

	// Ensure that the expression is a LogicalExpression or a
	// LiteralConstBoolExpression
	switch expression.Value().(type) {
	case *base.LogicalExpression:
		return e.executeLogicalExpression(name, expression, expressionState, expressionArgs, parents, prevExecutorState)
	case *base.LiteralBoolExpression:
		return e.executeNOPFilterExpression(name, parents, prevExecutorState)
	}

	return false, nil, nil
}

// Execute the NOP where expression by selecting all the series in the parents
func (e *Executor) executeNOPFilterExpression(
	name string,
	parents []*ExecutablePlanNode,
	prevExecutorState interface{}) (bool, interface{}, error) {

	var execState NOPWhereExecState
	if prevExecutorState != nil {
		execState = prevExecutorState.(NOPWhereExecState)
	} else {
		execState = NOPWhereExecState{iteratorPos: make(map[int]*client.BoostSeriesIteratorPosition)}
	}

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
		iterPos, ok := execState.iteratorPos[colIndx]
		if !ok {
			seriesField.seriesIterator.Begin()
		} else {
			seriesField.seriesIterator.Seek(iterPos)
		}
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
					execState.iteratorPos[colIndx] = seriesField.seriesIterator.Position()
					outOfSpace = true
					break
				}
			} else {
				fieldsCompleted[colIndx] = true
				delete(execState.iteratorPos, colIndx)
			}
		}
		e.resultSize = max(row, e.resultSize)
	}

	return outOfSpace, execState, nil
}

// Execute the where operation
func (e *Executor) executeLogicalExpression(
	name string,
	expression *stdlib.MaybeOp[base.Expression],
	expressionState base.ExpressionState,
	expressionArgs []any,
	parents []*ExecutablePlanNode,
	prevExecutorState interface{}) (bool, interface{}, error) {

	// Get/Init an executor state
	var execState LogicalWhereExecState
	if prevExecutorState != nil {
		execState = prevExecutorState.(LogicalWhereExecState)
	} else {
		execState = LogicalWhereExecState{
			whereIterPos: make(map[int]*client.BoostSeriesIteratorPosition),
			iteratorPos:  make(map[int]*client.BoostSeriesIteratorPosition)}
	}

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
	type WhereFieldInfo struct {
		name            string
		index           int
		selectFieldInfo *SelectFieldInfo
	}
	whereFieldInfos := make([]WhereFieldInfo, 0)
	numWhereOnlyFields := 0
	for _, parent := range parents {
		index, ok := e.planNodeNameToSelectFieldIndex[parent.name]
		var selectFieldInfo *SelectFieldInfo = nil
		if !ok {
			selectFieldInfo = e.whereOnlyFields[parent.name]
			index = numWhereOnlyFields
			numWhereOnlyFields++
		} else {
			selectFieldInfo = &e.resultSetFields[index]
		}
		whereFieldInfos = append(
			whereFieldInfos,
			WhereFieldInfo{
				name:            parent.name,
				index:           index,
				selectFieldInfo: selectFieldInfo})
	}
	// TODO: This should be workspace_size
	whereResultSet := base.NewResultSet(e.batchSize)
	whereResultSet.ReDim(numWhereOnlyFields)

	// Iterator row reference to move them to the next row
	iterCurrentRow := make(map[string]int)

	// If the iterator has reached the end (from another use), then restart
	for _, whereFieldInfo := range whereFieldInfos {
		iterPos, ok := execState.whereIterPos[whereFieldInfo.index]
		iterCurrentRow[whereFieldInfo.selectFieldInfo.fullyQualifiedSeriesName()] = -1
		if !ok {
			whereFieldInfo.selectFieldInfo.seriesIterator.Begin()
		} else {
			whereFieldInfo.selectFieldInfo.seriesIterator.Seek(iterPos)
		}
	}

	// Iterate through all the series iterators. Then sent the columns name
	// values for all the column names in the whereOpColumnNames in the state
	// and then evaluate the expression

	row := 0

	// Extract the values for all the fields in the where clause
	for {
		atleastOneNonNullCol := false
		for colIndx, whereFieldInfo := range whereFieldInfos {
			if fieldsCompleted[colIndx] {
				continue
			}

			hasSomething := moveIteratorToRow(iterCurrentRow, whereFieldInfo.selectFieldInfo, row)
			if hasSomething {
				atleastOneNonNullCol = true
				whereResultSet.Resize(row + 1)
				dp, _, _ := whereFieldInfo.selectFieldInfo.seriesIterator.Current()
				if whereFieldInfo.selectFieldInfo.attributeName == "value" {
					whereResultSet.Set(row, colIndx, dp.Value)
				} else {
					// TODO: This is suboptimal. We should cache the attributes
					// from an iterator and then reuse it for all fields referenced
					// from the same iterator.
					attributes := whereFieldInfo.selectFieldInfo.seriesIterator.Attributes()
					if attributes != nil {
						attribute := attributes.Current()
						if attribute.Name.String() == whereFieldInfo.selectFieldInfo.attributeName {
							whereResultSet.Set(row, colIndx, attribute.Value.String())
						}
					}
				}
			} else {
				fieldsCompleted[colIndx] = true
			}
		}
		if atleastOneNonNullCol {
			row++
		}
		if row >= e.batchSize || len(fieldsCompleted) == len(whereFieldInfos) {
			// Done for now
			break
		}
	}

	if row == e.batchSize {
		outOfSpace = true
	}

	iterCurrentRow = make(map[string]int)

	// If the iterator has reached the end (from another use), then restart
	for colIndx, seriesField := range e.resultSetFields {
		iterPos, ok := execState.iteratorPos[colIndx]
		iterCurrentRow[seriesField.fullyQualifiedSeriesName()] = -1
		if !ok {
			seriesField.seriesIterator.Begin()
		} else {
			seriesField.seriesIterator.Seek(iterPos)
		}
	}

	// Evaluate the expression for each row
	fieldsCompleted = make(map[int]bool)
	resultSetRow := 0
	for row := 0; row < whereResultSet.NumRows(); row++ {
		for colIndx, whereFieldInfo := range whereFieldInfos {
			if whereFieldInfo.selectFieldInfo.attributeName == "value" {
				argName := whereFieldInfo.selectFieldInfo.seriesName + ".value"
				val, _ := whereResultSet.GetFloat(row, colIndx)
				expressionState.SetValue(argName, val)
			} else {
				// TODO handle attributes
			}
		}

		// Evaluate the expression
		expressionArgs := expressionState.ToArgs()
		result := expression.Evaluate(expressionArgs)
		if result.Error() != nil {
			return false, result.Error(), nil
		} else if result.Value().(*base.LiteralBoolExpression).Bool() {

			e.resultSet.Resize(resultSetRow + 1)

			// Read every column values for this row into the resultset array.
			for colIndx, seriesField := range e.resultSetFields {
				if fieldsCompleted[colIndx] {
					continue
				}
				hasSomething := moveIteratorToRow(iterCurrentRow, &seriesField, row)
				if hasSomething {
					dp, _, _ := seriesField.seriesIterator.Current()
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
				} else {
					fieldsCompleted[colIndx] = true
				}
			}

			resultSetRow++
		} else {
			// Skip iterators past this row
			for _, seriesField := range e.resultSetFields {
				moveIteratorToRow(iterCurrentRow, &seriesField, row)
			}
		}
	}
	e.resultSize = resultSetRow

	// Delete all iterator positions if we are done
	if !outOfSpace {
		delete(execState.whereIterPos, 0)
		delete(execState.iteratorPos, 0)
	}

	return outOfSpace, execState, nil
}

// Moves the iterator referenced by the given SELECT field to the specified
// row. Returns true if the iterator has something at the row, false otherwise.
// Behavior is undefined if the iterator has already reached the end.
func moveIteratorToRow(iterCurrentRow map[string]int, selectFieldInfo *SelectFieldInfo, row int) bool {
	hasSomething := true
	for iterCurrentRow[selectFieldInfo.fullyQualifiedSeriesName()] < row {
		hasSomething = selectFieldInfo.seriesIterator.Next()
		if hasSomething {
			iterCurrentRow[selectFieldInfo.fullyQualifiedSeriesName()]++
		} else {
			break
		}
	}
	return hasSomething
}
