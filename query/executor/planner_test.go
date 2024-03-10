package executor

import (
	"os"
	"testing"

	"github.com/dominikbraun/graph/draw"
	"github.com/rmravindran/boostdb/query/parser"
	"github.com/stretchr/testify/require"
)

// Unit test the parser
func TestPlanner_SimpleSelect(t *testing.T) {

	px := parser.NewParser()
	queryOps, err := px.Parse("select a, b from j.s")
	require.Nil(t, err)
	require.NotNil(t, queryOps)

	planner := NewPlanner()
	plan := planner.GeneratePlan(queryOps)
	require.NotNil(t, plan)

	file, _ := os.Create("./mygraph.gv")
	_ = draw.DOT(plan.qGraph, file)

	// Expecting 1 fetch operation at depth == 1
	fetchOps := plan.NodesAtDepth(1)
	require.Equal(t, 1, len(fetchOps))
	require.Equal(t, PlanNodeTypeFetch, fetchOps[0].planNodeType)
	require.NotNil(t, fetchOps[0].fetchOp)
	require.Equal(t, "default", fetchOps[0].fetchOp.namespace)
	require.Equal(t, "s", fetchOps[0].fetchOp.seriesFamily)

	// Expecting 2 select field operations at depth == 2
	selectFieldOps := plan.NodesAtDepth(2)
	require.Equal(t, 2, len(selectFieldOps))
	require.Equal(t, PlanNodeTypeSelectSeries, selectFieldOps[0].planNodeType)
	require.NotNil(t, selectFieldOps[0].expression)
	require.Equal(t, "a.value", selectFieldOps[0].name)
	require.Equal(t, PlanNodeTypeSelectSeries, selectFieldOps[1].planNodeType)
	require.NotNil(t, selectFieldOps[1].expression)
	require.Equal(t, "b.value", selectFieldOps[1].name)
}

// Unit test the parser
func TestPlanner_SimpleSelectWithWhere(t *testing.T) {

	px := parser.NewParser()
	queryOps, err := px.Parse("SELECT a FROM s WHERE a < 10")
	require.Nil(t, err)
	require.NotNil(t, queryOps)

	planner := NewPlanner()
	plan := planner.GeneratePlan(queryOps)
	require.NotNil(t, plan)

	// Expecting 1 fetch operation at depth == 1
	fetchOps := plan.NodesAtDepth(1)
	require.Equal(t, 1, len(fetchOps))
	require.Equal(t, PlanNodeTypeFetch, fetchOps[0].planNodeType)
	require.NotNil(t, fetchOps[0].fetchOp)
	require.Equal(t, "default", fetchOps[0].fetchOp.namespace)
	require.Equal(t, "s", fetchOps[0].fetchOp.seriesFamily)

	// Expecting 2 select field operations at depth == 2
	selectFieldOps := plan.NodesAtDepth(2)
	require.Equal(t, 1, len(selectFieldOps))
	require.Equal(t, PlanNodeTypeSelectSeries, selectFieldOps[0].planNodeType)
	require.NotNil(t, selectFieldOps[0].expression)
	require.Equal(t, "a.value", selectFieldOps[0].name)

	// Expecting the where clause with some specific structure
	whereNodes := plan.NodesAtDepth(3)
	// There should only be one root node at depth 3
	require.Equal(t, 1, len(whereNodes))

	// Check the where clause
	require.NotNil(t, whereNodes[0].expression)
	require.Equal(t, PlanNodeTypeWhere, whereNodes[0].planNodeType)
	rootExp := whereNodes[0].expression
	require.NotNil(t, rootExp)

	// Check the root expression and the plan state
	require.NotNil(t, whereNodes[0].ExpressionState())
	args := whereNodes[0].ExpressionArgs().([]any)
	require.Equal(t, 2, len(args))
}
