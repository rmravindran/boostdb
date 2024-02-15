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
	require.NotNil(t, res)
	require.False(t, res.Evaluate().Value().(*LiteralBoolExpression).Bool())

	// Test case 2
	res = NewLogicalExpression(Literal, LogicalOr, nil, nil)
	require.NotNil(t, res)
	require.False(t, res.Evaluate().Value().(*LiteralBoolExpression).Bool())

	// Test case 3 (nil left and liberalboolexpression with true expressions)
	trueExpr := stdlib.JustOp[Expression](NewLiteralBoolExpression(true))
	res = NewLogicalExpression(Literal, LogicalAnd, nil, trueExpr)
	require.NotNil(t, res)
	require.False(t, res.Evaluate().Value().(*LiteralBoolExpression).Bool())

	// Test case 3 (with Or)
	res = NewLogicalExpression(Literal, LogicalOr, nil, trueExpr)
	require.NotNil(t, res)
	require.True(t, res.Evaluate().Value().(*LiteralBoolExpression).Bool())

	// Test case 4 (nil right and liberalboolexpression with true expressions)
	res = NewLogicalExpression(Literal, LogicalAnd, trueExpr, nil)
	require.NotNil(t, res)
	require.False(t, res.Evaluate().Value().(*LiteralBoolExpression).Bool())

	// Test case 4 (with Or)
	res = NewLogicalExpression(Literal, LogicalOr, trueExpr, nil)
	require.NotNil(t, res)
	require.True(t, res.Evaluate().Value().(*LiteralBoolExpression).Bool())
}

// Test with the And operator
func TestBoolExpressionAnd(t *testing.T) {
	trueExpr := stdlib.JustOp[Expression](NewLiteralBoolExpression(true))
	falseExpr := stdlib.JustOp[Expression](NewLiteralBoolExpression(false))

	// Test case 1 (true and true)
	res := NewLogicalExpression(Literal, LogicalAnd, trueExpr, trueExpr)
	require.True(t, res.Evaluate().Value().(*LiteralBoolExpression).Bool())

	// Test case 2 (true and false)
	res = NewLogicalExpression(Literal, LogicalAnd, trueExpr, falseExpr)
	require.False(t, res.Evaluate().Value().(*LiteralBoolExpression).Bool())

	// Test case 3 (false and true)
	res = NewLogicalExpression(Literal, LogicalAnd, falseExpr, trueExpr)
	require.False(t, res.Evaluate().Value().(*LiteralBoolExpression).Bool())

	// Test case 4 (false and false)
	res = NewLogicalExpression(Literal, LogicalAnd, falseExpr, falseExpr)
	require.False(t, res.Evaluate().Value().(*LiteralBoolExpression).Bool())
}

// Test with the Or operator
func TestBoolExpressionOr(t *testing.T) {
	trueExpr := stdlib.JustOp[Expression](NewLiteralBoolExpression(true))
	falseExpr := stdlib.JustOp[Expression](NewLiteralBoolExpression(false))

	// Test case 1 (true or true)
	res := NewLogicalExpression(Literal, LogicalOr, trueExpr, trueExpr)
	require.True(t, res.Evaluate().Value().(*LiteralBoolExpression).Bool())

	// Test case 2 (true or false)
	res = NewLogicalExpression(Literal, LogicalOr, trueExpr, falseExpr)
	require.True(t, res.Evaluate().Value().(*LiteralBoolExpression).Bool())

	// Test case 3 (false or true)
	res = NewLogicalExpression(Literal, LogicalOr, falseExpr, trueExpr)
	require.True(t, res.Evaluate().Value().(*LiteralBoolExpression).Bool())

	// Test case 4 (false or false)
	res = NewLogicalExpression(Literal, LogicalOr, falseExpr, falseExpr)
	require.False(t, res.Evaluate().Value().(*LiteralBoolExpression).Bool())
}

// Test with boolean expressions with depth of 2
func TestBoolExpressionDepth2(t *testing.T) {
	trueExpr := stdlib.JustOp[Expression](NewLiteralBoolExpression(true))
	falseExpr := stdlib.JustOp[Expression](NewLiteralBoolExpression(false))

	// Test case 1 (true or (true and false))
	andExpr := NewLogicalExpression(Literal, LogicalAnd, trueExpr, falseExpr)
	res := NewLogicalExpression(Literal, LogicalOr, trueExpr, andExpr)
	require.True(t, res.Evaluate(NewNullArgs()).Value().(*LiteralBoolExpression).Bool())

	// Test case 2 ((true and false) or true)
	res = NewLogicalExpression(Literal, LogicalOr, andExpr, trueExpr)
	require.True(t, res.Evaluate(NewNullArgs()).Value().(*LiteralBoolExpression).Bool())

	// Test case 3 (false and (true or false))
	orExpr := NewLogicalExpression(Literal, LogicalOr, trueExpr, falseExpr)
	res = NewLogicalExpression(Literal, LogicalAnd, falseExpr, orExpr)
	require.False(t, res.Evaluate(NewNullArgs()).Value().(*LiteralBoolExpression).Bool())

	// Test case 4 ((true or false) and false)
	res = NewLogicalExpression(Literal, LogicalAnd, orExpr, falseExpr)
	require.False(t, res.Evaluate(NewNullArgs()).Value().(*LiteralBoolExpression).Bool())

	// Test case 5 ((true or false) and (true or false))
	res = NewLogicalExpression(Literal, LogicalAnd, orExpr, orExpr)
	evalRes := res.Evaluate(NewNullArgs(), NewNullArgs())
	require.Nil(t, evalRes.Error())
	require.True(t, evalRes.Value().(*LiteralBoolExpression).Bool())
}
