package base

import (
	"errors"

	"github.com/rmravindran/boostdb/stdlib"
)

// Logical Operator Type
type LogicalOpType int

const (
	// Greater than or equal
	LogicalGEQ LogicalOpType = iota

	// Less than or equal
	LogicalLEQ

	// Equal
	LogicalEQ

	// Not Equal
	LogicalNEQ

	// Less than
	LogicalLT

	// Greater than
	LogicalGT

	// AND
	LogicalAnd

	// OR
	LogicalOr
)

// Boolean Expression
type LogicalExpression struct {
	sqlOpType SqlOpType
	opType    LogicalOpType
	leftExpr  *stdlib.MaybeOp[Expression]
	rightExpr *stdlib.MaybeOp[Expression]
}

// Create a new boolean expression
func NewLogicalExpression(
	sqlOpType SqlOpType,
	opType LogicalOpType,
	leftExpr *stdlib.MaybeOp[Expression],
	rightExpr *stdlib.MaybeOp[Expression]) *stdlib.MaybeOp[Expression] {

	if leftExpr == nil {
		leftExpr = stdlib.JustOp[Expression](NewLiteralBoolConstExpression(false))
	}

	if rightExpr == nil {
		rightExpr = stdlib.JustOp[Expression](NewLiteralBoolConstExpression(false))
	}

	expr := &LogicalExpression{
		sqlOpType: sqlOpType,
		opType:    opType,
		leftExpr:  leftExpr,
		rightExpr: rightExpr,
	}
	return stdlib.JustOp[Expression](expr)
}

// Set the left expression
func (be *LogicalExpression) SetLeft(leftExpr *stdlib.MaybeOp[Expression]) {
	be.leftExpr = leftExpr
}

// Set the right expression
func (be *LogicalExpression) SetRight(rightExpr *stdlib.MaybeOp[Expression]) {
	be.rightExpr = rightExpr
}

// Return the left expression
func (be *LogicalExpression) Left() *stdlib.MaybeOp[Expression] {
	return be.leftExpr
}

// Return the right expression
func (be *LogicalExpression) Right() *stdlib.MaybeOp[Expression] {
	return be.rightExpr
}

// Return the type of the sql expression that generated this expression
func (be *LogicalExpression) SqlOp() SqlOpType {
	return be.sqlOpType
}

// Evaluate the expression
func (be *LogicalExpression) Evaluate(args ...interface{}) *stdlib.MaybeOp[Expression] {
	var leftExpr *stdlib.MaybeOp[Expression] = be.leftExpr
	var rightExpr *stdlib.MaybeOp[Expression] = be.rightExpr
	var (
		leftDone  bool = false
		rightDone bool = false
	)

	argIndex := 0
	var leftValue, rightValue _LiteralValue

	leftDone = be.evaluateIfLiteralExpression(leftExpr, &leftValue, args...)
	rightDone = be.evaluateIfLiteralExpression(rightExpr, &rightValue, args...)

	for !leftDone || !rightDone {
		if !leftDone {
			leftExpr = leftExpr.Evaluate(args...)

			if args == nil || argIndex >= len(args) {
				return stdlib.ErrorOp[Expression](errors.New("not enough arguments"))
			}

			// TODO Check if this is an error expression
			argIndex++
			leftDone = be.evaluateIfLiteralExpression(leftExpr, &leftValue)
		}

		if !rightDone {
			if args == nil || argIndex >= len(args) {
				return stdlib.ErrorOp[Expression](errors.New("not enough arguments"))
			}

			rightExpr = rightExpr.Evaluate(args[argIndex])
			// TODO Check if this is an error expression
			argIndex++
			rightDone = be.evaluateIfLiteralExpression(rightExpr, &rightValue)
		}
	}

	if leftValue.DataType != rightValue.DataType {
		return stdlib.ErrorOp[Expression](errors.New("incompatible types"))
	}

	switch be.opType {
	case LogicalAnd:
		return stdlib.JustOp[Expression](NewLiteralBoolConstExpression(leftValue.BoolValue && rightValue.BoolValue))
	case LogicalOr:
		// TODO Optimize the OR operator to short-circuit the above for-loop
		return stdlib.JustOp[Expression](NewLiteralBoolConstExpression(leftValue.BoolValue || rightValue.BoolValue))
	default:
		return stdlib.JustOp[Expression](NewLiteralBoolConstExpression(be.evaluateOperation(&leftValue, &rightValue)))
	}
}

// Return the final values produced by the operator, otherwise returns itself
func (be *LogicalExpression) Value() any {
	return be
}

// Return false to indicate that this is not a constant
func (be *LogicalExpression) IsConstant() bool {
	return false
}

type _LiteralValue struct {
	BoolValue  bool
	IntValue   int64
	FloatValue float64
	StrValue   string
	DataType   ValueType
}

// Evalue the Expression based on its underlying literal type and store the
// value in the specfied _LiteralValue struct and return true if the evaluation
// was successful, otherwise returns false.
func (be *LogicalExpression) evaluateIfLiteralExpression(expr *stdlib.MaybeOp[Expression], value *_LiteralValue, args ...interface{}) bool {
	switch expr.Value().(type) {
	case *LiteralBoolExpression:
		value.BoolValue = expr.Evaluate(args...).Value().(*LiteralBoolExpression).Bool()
		value.DataType = ValueTypeBool
	case *LiteralIntExpression:
		value.IntValue = expr.Evaluate(args...).Value().(*LiteralIntExpression).Int()
		value.DataType = ValueTypeInt
	case *LiteralFloatExpression:
		value.FloatValue = expr.Evaluate(args...).Value().(*LiteralFloatExpression).Float()
		value.DataType = ValueTypeFloat
	case *LiteralStringExpression:
		value.StrValue = expr.Evaluate(args...).Value().(*LiteralStringExpression).String()
		value.DataType = ValueTypeString
	default:
		return false
	}
	return true
}

// Evaluate the operation on the two literals based on their optype and return
// the boolean result
func (be *LogicalExpression) evaluateOperation(left *_LiteralValue, right *_LiteralValue) bool {
	switch left.DataType {
	case ValueTypeInt:
		switch be.opType {
		case LogicalGEQ:
			return left.IntValue >= right.IntValue
		case LogicalLEQ:
			return left.IntValue <= right.IntValue
		case LogicalEQ:
			return left.IntValue == right.IntValue
		case LogicalNEQ:
			return left.IntValue != right.IntValue
		case LogicalLT:
			return left.IntValue < right.IntValue
		case LogicalGT:
			return left.IntValue > right.IntValue
		}
	case ValueTypeFloat:
		switch be.opType {
		case LogicalGEQ:
			return left.FloatValue >= right.FloatValue
		case LogicalLEQ:
			return left.FloatValue <= right.FloatValue
		case LogicalEQ:
			return left.FloatValue == right.FloatValue
		case LogicalNEQ:
			return left.FloatValue != right.FloatValue
		case LogicalLT:
			return left.FloatValue < right.FloatValue
		case LogicalGT:
			return left.FloatValue > right.FloatValue
		}
	case ValueTypeString:
		switch be.opType {
		case LogicalGEQ:
			return left.StrValue >= right.StrValue
		case LogicalLEQ:
			return left.StrValue <= right.StrValue
		case LogicalEQ:
			return left.StrValue == right.StrValue
		case LogicalNEQ:
			return left.StrValue != right.StrValue
		case LogicalLT:
			return left.StrValue < right.StrValue
		case LogicalGT:
			return left.StrValue > right.StrValue
		}
	case ValueTypeBool:
		switch be.opType {
		case LogicalGEQ:
			return left.BoolValue == right.BoolValue
		case LogicalLEQ:
			return left.BoolValue == right.BoolValue
		case LogicalEQ:
			return left.BoolValue == right.BoolValue
		case LogicalNEQ:
			return left.BoolValue != right.BoolValue
		case LogicalLT:
			return left.BoolValue == right.BoolValue
		case LogicalGT:
			return left.BoolValue == right.BoolValue
		}
	}

	return false
}
