package executor

import (
	"fmt"

	"github.com/dominikbraun/graph"
	"github.com/rmravindran/boostdb/query/base"
	"github.com/rmravindran/boostdb/stdlib"
)

// A planner to generate an execution plan that specifies the steps to be
// performed to generate the result set.
type Planner struct {
}

type SourceFetchOp struct {
	namespace    string
	seriesFamily string
	domain       string
}

// ExecutablePlanNode Type
type PlanNodeType int

const (
	// Fetch Operation
	PlanNodeTypeFetch PlanNodeType = iota

	// Select Series Operation
	PlanNodeTypeSelectSeries

	// Where Operation
	PlanNodeTypeWhere
)

type ExecutablePlanNode struct {
	name         string
	planNodeType PlanNodeType
	fetchOp      *SourceFetchOp
	expression   *stdlib.MaybeOp[base.Expression]
}

func hashFunc(e *ExecutablePlanNode) string {
	return e.name
}

type QueryPlan struct {
	queryOps *base.QueryOps
	qGraph   graph.Graph[string, *ExecutablePlanNode]

	rootNodes []string
}

// Create a new planner
func NewPlanner() *Planner {
	return &Planner{}
}

func NewFetchPlanNode(
	name string,
	domain string,
	familyName string) *ExecutablePlanNode {
	return &ExecutablePlanNode{
		name:         name,
		planNodeType: PlanNodeTypeFetch,
		fetchOp: &SourceFetchOp{
			namespace:    "default",
			domain:       domain,
			seriesFamily: familyName,
		},
		expression: nil,
	}
}

// Create a new selection field plan node with column name expression
func NewSelectPlanNode(
	name string,
	source string,
	fieldName string) *ExecutablePlanNode {
	fullName := source + "." + fieldName
	return &ExecutablePlanNode{
		name:         name,
		planNodeType: PlanNodeTypeSelectSeries,
		fetchOp:      nil,
		expression:   stdlib.JustOp[base.Expression](base.NewColumnNameExpression(fullName, base.Select)),
	}
}

// Generate a query plan
func (p *Planner) GeneratePlan(queryOps *base.QueryOps) *QueryPlan {
	qp := &QueryPlan{
		queryOps:  queryOps,
		qGraph:    graph.New(hashFunc, graph.Directed()),
		rootNodes: make([]string, 0),
	}

	// Alias/Source map
	aliasSourceMap := make(map[string]string)
	firstSource := ""

	// Add source fetch operations to the graph
	for _, sourceFetchOp := range queryOps.SourceFetchOps() {
		name := sourceFetchOp.Alias
		aliasSourceMap[sourceFetchOp.Source] = sourceFetchOp.Alias
		if sourceFetchOp.Source != sourceFetchOp.Alias {
			aliasSourceMap[sourceFetchOp.Alias] = sourceFetchOp.Alias
		}
		if firstSource == "" {
			firstSource = sourceFetchOp.Alias
		}
		err := qp.qGraph.AddVertex(NewFetchPlanNode(
			name, sourceFetchOp.Domain, sourceFetchOp.Source))
		if err != nil {
			// TODO return error
			return nil
		}
		qp.rootNodes = append(qp.rootNodes, name)
	}

	// Add select field operations to the graph. The field operations are
	// dependent on the source fetch operations
	selectFieldNames := make([]string, 0)
	for _, selectFieldOp := range queryOps.SelectFieldOps() {
		name := selectFieldOp.SeriesName + "." + selectFieldOp.AttributeName
		source := selectFieldOp.SourceAlias

		// Apply the first source if the source is empty
		if source == "" {
			source = firstSource
		}

		// Get the alias for the source
		alias, ok := aliasSourceMap[source]
		if !ok {
			// TODO return error
		} else {

			qp.qGraph.AddVertex(NewSelectPlanNode(name, source, name))
			selectFieldNames = append(selectFieldNames, name)

			// Create an edge from the fetch node to the select node
			err := qp.qGraph.AddEdge(alias, name)
			if err != nil {
				// TODO return error
				return nil
			}
		}
	}

	// Now add the WHERE expression to the graph. While adding the expression
	// we need to add the edges from the fetch nodes to indicate the dependency
	rootWhereExpression := queryOps.WhereExpression()
	if rootWhereExpression == nil {
		// Add a trivial boolean literal expression. This is necessary since
		// the result accumulation happens in the where expression executor
		for _, name := range selectFieldNames {
			nodeName := name + ".NOPFilterExpression()"
			qp.qGraph.AddVertex(&ExecutablePlanNode{
				name:         nodeName,
				planNodeType: PlanNodeTypeWhere,
				fetchOp:      nil,
				expression: stdlib.JustOp[base.Expression](
					base.NewLiteralBoolConstExpression(nodeName, true)),
			})

			// Create an edge from the fetch node to the select node
			err := qp.qGraph.AddEdge(name, nodeName)
			if err != nil {
				// TODO return error
				return nil
			}
		}
	}

	return qp
}

// Return the nodes in the query plan with depths less than the specified depth
func (qp *QueryPlan) NodesAtOrBelowDepth(depthLimit int) []*ExecutablePlanNode {

	ret := make([]*ExecutablePlanNode, 0)

	for _, node := range qp.rootNodes {
		_ = qp.dfsWithDepth(node, func(node string, depth int) bool {
			if depth <= depthLimit {
				planNode, err := qp.qGraph.Vertex(node)
				if err != nil {
					ret = append(ret, planNode)
				}
			}
			return depth > depthLimit
		})
	}

	return ret
}

func (qp *QueryPlan) dfsWithDepth(start string, visit func(string, int) bool) error {
	adjacencyMap, err := qp.qGraph.AdjacencyMap()
	if err != nil {
		return fmt.Errorf("could not get adjacency map: %w", err)
	}

	if _, ok := adjacencyMap[start]; !ok {
		return fmt.Errorf("could not find start vertex with hash %v", start)
	}

	queue := make([]string, 0)
	visited := make(map[string]bool)

	visited[start] = true
	queue = append(queue, start)
	depth := 0

	stopProcessing := false

	for len(queue) > 0 && !stopProcessing {
		toProcess := len(queue)
		processQueue := queue[:toProcess]
		depth++
		for _, currentHash := range processQueue {

			// Stop if the visitor returns true
			stopProcessing = visit(currentHash, depth)
			if stopProcessing {
				break
			}

			for adjacency := range adjacencyMap[currentHash] {
				if _, ok := visited[adjacency]; !ok {
					visited[adjacency] = true
					queue = append(queue, adjacency)
				}
			}
		}
		queue = queue[toProcess:]
	}

	return nil
}

// Return the nodes in the query plan at the specified depth
func (qp *QueryPlan) NodesAtDepth(depth int) []*ExecutablePlanNode {
	ret := make([]*ExecutablePlanNode, 0)

	for _, node := range qp.rootNodes {
		_ = qp.dfsWithDepth(node, func(node string, d int) bool {
			if d == depth {
				planNode, err := qp.qGraph.Vertex(node)
				if err == nil {
					ret = append(ret, planNode)
				}
			}
			return false //d > depth
		})
	}

	return ret
}

// Return the nodes that are adjacent to the specified nodes
func (qp *QueryPlan) AdjacentNodes(nodes []*ExecutablePlanNode) ([]*ExecutablePlanNode, error) {
	ret := make([]*ExecutablePlanNode, 0)

	adjacencyMap, err := qp.qGraph.AdjacencyMap()
	if err != nil {
		return ret, fmt.Errorf("could not get adjacency map: %w", err)
	}

	for _, node := range nodes {
		if _, ok := adjacencyMap[node.name]; ok {
			for adjacency := range adjacencyMap[node.name] {
				planNode, err := qp.qGraph.Vertex(adjacency)
				if err == nil {
					ret = append(ret, planNode)
				}
			}
		}
	}

	return ret, nil
}

// Return the nodes that are parents of the specified node
func (qp *QueryPlan) Parents(node *ExecutablePlanNode) ([]*ExecutablePlanNode, error) {
	ret := make([]*ExecutablePlanNode, 0)

	predMap, err := qp.qGraph.PredecessorMap()
	if err != nil {
		return ret, fmt.Errorf("could not get adjacency map: %w", err)
	}

	if _, ok := predMap[node.name]; ok {
		for pred := range predMap[node.name] {
			planNode, err := qp.qGraph.Vertex(pred)
			if err == nil {
				ret = append(ret, planNode)
			}
		}
	}

	return ret, nil
}

// Return an iterator for the query plan
func (qp *QueryPlan) Iterator() *PlanIterator {
	return NewPlanIterator(qp)
}
