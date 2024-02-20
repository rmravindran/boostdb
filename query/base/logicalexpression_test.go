// Generate unit test for the BoolExpression type

// Path: query/base/boolexpression_test.go
// Generate unit test for the BoolExpression type
package base

import (
	"testing"

	"github.com/rmravindran/boostdb/stdlib"
	"github.com/stretchr/testify/require"
)

// Unit test the NewLogicalExpression function
func TestBooleanExpressionNew(t *testing.T) {

	// Test case 1
	res := NewLogicalExpression(Literal, LogicalAnd, nil, nil)
	_, argValues := PrepareInitialValues(res, nil)
	require.NotNil(t, res)
	require.False(t, res.Evaluate(argValues).Value().(*LiteralBoolExpression).Bool())

	// Test case 2
	res = NewLogicalExpression(Literal, LogicalOr, nil, nil)
	_, argValues = PrepareInitialValues(res, nil)
	require.NotNil(t, res)
	require.False(t, res.Evaluate(argValues).Value().(*LiteralBoolExpression).Bool())

	// Test case 3 (nil left and liberalboolexpression with true expressions)
	trueExpr := stdlib.JustOp[Expression](NewLiteralBoolConstExpression("", true))
	res = NewLogicalExpression(Literal, LogicalAnd, nil, trueExpr)
	_, argValues = PrepareInitialValues(res, nil)
	require.NotNil(t, res)
	require.False(t, res.Evaluate(argValues).Value().(*LiteralBoolExpression).Bool())

	// Test case 3 (with Or)
	res = NewLogicalExpression(Literal, LogicalOr, nil, trueExpr)
	_, argValues = PrepareInitialValues(res, nil)
	require.NotNil(t, res)
	require.True(t, res.Evaluate(argValues).Value().(*LiteralBoolExpression).Bool())

	// Test case 4 (nil right and liberalboolexpression with true expressions)
	res = NewLogicalExpression(Literal, LogicalAnd, trueExpr, nil)
	_, argValues = PrepareInitialValues(res, nil)
	require.NotNil(t, res)
	require.False(t, res.Evaluate(argValues).Value().(*LiteralBoolExpression).Bool())

	// Test case 4 (with Or)
	res = NewLogicalExpression(Literal, LogicalOr, trueExpr, nil)
	_, argValues = PrepareInitialValues(res, nil)
	require.NotNil(t, res)
	require.True(t, res.Evaluate(argValues).Value().(*LiteralBoolExpression).Bool())
}

// Test with the And operator
func TestBoolExpressionAnd(t *testing.T) {
	trueExpr := stdlib.JustOp[Expression](NewLiteralBoolConstExpression("", true))
	falseExpr := stdlib.JustOp[Expression](NewLiteralBoolConstExpression("", false))

	// Test case 1 (true and true)
	res := NewLogicalExpression(Literal, LogicalAnd, trueExpr, trueExpr)
	_, argValues := PrepareInitialValues(res, nil)
	require.True(t, res.Evaluate(argValues).Value().(*LiteralBoolExpression).Bool())

	// Test case 2 (true and false)
	res = NewLogicalExpression(Literal, LogicalAnd, trueExpr, falseExpr)
	_, argValues = PrepareInitialValues(res, nil)
	require.False(t, res.Evaluate(argValues).Value().(*LiteralBoolExpression).Bool())

	// Test case 3 (false and true)
	res = NewLogicalExpression(Literal, LogicalAnd, falseExpr, trueExpr)
	_, argValues = PrepareInitialValues(res, nil)
	require.False(t, res.Evaluate(argValues).Value().(*LiteralBoolExpression).Bool())

	// Test case 4 (false and false)
	res = NewLogicalExpression(Literal, LogicalAnd, falseExpr, falseExpr)
	_, argValues = PrepareInitialValues(res, nil)
	require.False(t, res.Evaluate(argValues).Value().(*LiteralBoolExpression).Bool())
}

// Test with the Or operator
func TestBoolExpressionOr(t *testing.T) {
	trueExpr := stdlib.JustOp[Expression](NewLiteralBoolConstExpression("", true))
	falseExpr := stdlib.JustOp[Expression](NewLiteralBoolConstExpression("", false))

	// Test case 1 (true or true)
	res := NewLogicalExpression(Literal, LogicalOr, trueExpr, trueExpr)
	_, argValues := PrepareInitialValues(res, nil)
	require.True(t, res.Evaluate(argValues).Value().(*LiteralBoolExpression).Bool())

	// Test case 2 (true or false)
	res = NewLogicalExpression(Literal, LogicalOr, trueExpr, falseExpr)
	_, argValues = PrepareInitialValues(res, nil)
	require.True(t, res.Evaluate(argValues).Value().(*LiteralBoolExpression).Bool())

	// Test case 3 (false or true)
	res = NewLogicalExpression(Literal, LogicalOr, falseExpr, trueExpr)
	_, argValues = PrepareInitialValues(res, nil)
	require.True(t, res.Evaluate(argValues).Value().(*LiteralBoolExpression).Bool())

	// Test case 4 (false or false)
	res = NewLogicalExpression(Literal, LogicalOr, falseExpr, falseExpr)
	_, argValues = PrepareInitialValues(res, nil)
	require.False(t, res.Evaluate(argValues).Value().(*LiteralBoolExpression).Bool())
}

// Test with boolean expressions with depth of 2
func TestBoolExpressionDepth2(t *testing.T) {
	trueExpr := stdlib.JustOp[Expression](NewLiteralBoolConstExpression("", true))
	falseExpr := stdlib.JustOp[Expression](NewLiteralBoolConstExpression("", false))

	// Test case 1 (true or (true and false))
	andExpr := NewLogicalExpression(Literal, LogicalAnd, trueExpr, falseExpr)
	res := NewLogicalExpression(Literal, LogicalOr, trueExpr, andExpr)
	_, argValues := PrepareInitialValues(res, nil)
	require.True(t, res.Evaluate(argValues).Value().(*LiteralBoolExpression).Bool())

	// Test case 2 ((true and false) or true)
	res = NewLogicalExpression(Literal, LogicalOr, andExpr, trueExpr)
	_, argValues = PrepareInitialValues(res, nil)
	require.True(t, res.Evaluate(argValues).Value().(*LiteralBoolExpression).Bool())

	// Test case 3 (false and (true or false))
	orExpr := NewLogicalExpression(Literal, LogicalOr, trueExpr, falseExpr)
	res = NewLogicalExpression(Literal, LogicalAnd, falseExpr, orExpr)
	_, argValues = PrepareInitialValues(res, nil)
	require.False(t, res.Evaluate(argValues).Value().(*LiteralBoolExpression).Bool())

	// Test case 4 ((true or false) and false)
	res = NewLogicalExpression(Literal, LogicalAnd, orExpr, falseExpr)
	_, argValues = PrepareInitialValues(res, nil)
	require.False(t, res.Evaluate(argValues).Value().(*LiteralBoolExpression).Bool())

	// Test case 5 ((true or false) and (true or false))
	res = NewLogicalExpression(Literal, LogicalAnd, orExpr, orExpr)
	_, argValues = PrepareInitialValues(res, nil)
	evalRes := res.Evaluate(argValues)
	require.Nil(t, evalRes.Error())
	require.True(t, evalRes.Value().(*LiteralBoolExpression).Bool())
}

var varNames = make([]string, 0)

func regCallback(name string) {
	varNames = append(varNames, name)
}

// Test with boolean expressions with depth of 2
func TestBoolNonConstExpression(t *testing.T) {
	trueExpr := stdlib.JustOp[Expression](NewLiteralBoolConstExpression("", true))
	falseExpr := stdlib.JustOp[Expression](NewLiteralBoolExpression("varFalse"))

	varNames = make([]string, 0)

	// Test case 1 (true and false)
	res := NewLogicalExpression(Literal, LogicalAnd, trueExpr, falseExpr)
	_, argValues := PrepareInitialValues(res, nil)
	require.Equal(t, len(varNames), 1)
	require.Equal(t, varNames[0], "varFalse")
	require.True(t, res.Evaluate(argValues).Value().(*LiteralBoolExpression).Bool())
}
