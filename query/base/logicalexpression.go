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

// Logical Expression State
type LogicalExpressionState struct {

	// State of the expression if the left is a complex expression type
	leftExprState ExpressionState

	// State of the expression if the right is a complex expression type
	rightExprState ExpressionState

	// The const indicator for left expression
	isLeftConst bool

	// The const indicator for right expression
	isRightConst bool
}

// Create a new boolean expression
func NewLogicalExpression(
	sqlOpType SqlOpType,
	opType LogicalOpType,
	leftExpr *stdlib.MaybeOp[Expression],
	rightExpr *stdlib.MaybeOp[Expression]) *stdlib.MaybeOp[Expression] {

	if leftExpr == nil {
		leftExpr = stdlib.JustOp[Expression](NewLiteralBoolConstExpression("", false))
	}

	if rightExpr == nil {
		rightExpr = stdlib.JustOp[Expression](NewLiteralBoolConstExpression("", false))
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
func (be *LogicalExpression) Evaluate(args interface{}) *stdlib.MaybeOp[Expression] {
	var leftExpr *stdlib.MaybeOp[Expression] = be.leftExpr
	var rightExpr *stdlib.MaybeOp[Expression] = be.rightExpr
	var (
		leftDone  bool = false
		rightDone bool = false
	)

	var leftValue, rightValue _LiteralValue

	leftArg := args.([]interface{})[0]
	rightArg := args.([]interface{})[1]

	leftDone = be.evaluateIfLiteralExpression(leftExpr, &leftValue, leftArg)
	rightDone = be.evaluateIfLiteralExpression(rightExpr, &rightValue, rightArg)

	if !leftDone {
		leftExpr = leftExpr.Evaluate(leftArg)
		leftDone = be.evaluateIfLiteralExpression(leftExpr, &leftValue, nil)
		if !leftDone {
			return stdlib.ErrorOp[Expression](errors.New("evaluation failed"))
		}
	}
	if !rightDone {
		rightExpr = rightExpr.Evaluate(rightArg)
		rightDone = be.evaluateIfLiteralExpression(rightExpr, &rightValue, nil)
		if !rightDone {
			return stdlib.ErrorOp[Expression](errors.New("evaluation failed"))
		}
	}

	switch be.opType {
	case LogicalAnd:
		return stdlib.JustOp[Expression](NewLiteralBoolConstExpression("", leftValue.BoolValue && rightValue.BoolValue))
	case LogicalOr:
		// TODO Optimize the OR operator to short-circuit the above for-loop
		return stdlib.JustOp[Expression](NewLiteralBoolConstExpression("", leftValue.BoolValue || rightValue.BoolValue))
	default:
		return stdlib.JustOp[Expression](NewLiteralBoolConstExpression("", be.evaluateOperation(&leftValue, &rightValue)))
	}
}

// Return the final values produced by the operator, otherwise returns itself
func (be *LogicalExpression) Value() any {
	return be
}

// Return false to indicate that this is not a constant
func (be *LogicalExpression) IsConstant() bool {
	return be.leftExpr.Value().(Expression).IsConstant() && be.rightExpr.Value().(Expression).IsConstant()
}

// Prepare an expression
func (be *LogicalExpression) Prepare(
	nameHandler ArgNameHandler) ExpressionState {

	thisState := &LogicalExpressionState{
		leftExprState:  be.leftExpr.Value().(Expression).Prepare(nameHandler),
		rightExprState: be.rightExpr.Value().(Expression).Prepare(nameHandler),
		isLeftConst:    be.leftExpr.Value().(Expression).IsConstant(),
		isRightConst:   be.rightExpr.Value().(Expression).IsConstant(),
	}

	return thisState
}

// Return the Initial State
func (be *LogicalExpressionState) ToArgs() interface{} {
	return []interface{}{be.leftExprState.ToArgs(), be.rightExprState.ToArgs()}
}

// Set the value of the expression
func (les *LogicalExpressionState) SetValue(name string, value any) error {

	if les.isLeftConst && les.isRightConst {
		return errors.New("cannot set the value of a constant logical expression")
	}

	var errLeft error = nil
	var errRight error = nil
	if !les.isLeftConst {
		errLeft = les.leftExprState.SetValue(name, value)
	}

	if !les.isRightConst {
		errRight = les.rightExprState.SetValue(name, value)
	}

	if errLeft != nil {
		return errLeft
	}

	if errRight != nil {
		return errRight
	}

	return nil
}

// Return true if the expression is a constant, otherwise returns false
func (les *LogicalExpressionState) IsConstant() bool {
	return les.leftExprState.IsConstant() && les.rightExprState.IsConstant()
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
func (be *LogicalExpression) evaluateIfLiteralExpression(expr *stdlib.MaybeOp[Expression], value *_LiteralValue, args interface{}) bool {
	switch expr.Value().(type) {
	case *LiteralBoolExpression:
		value.BoolValue = expr.Evaluate(args).Value().(*LiteralBoolExpression).Bool()
		value.DataType = ValueTypeBool
	case *LiteralIntExpression:
		value.IntValue = expr.Evaluate(args).Value().(*LiteralIntExpression).Int()
		value.DataType = ValueTypeInt
	case *LiteralFloatExpression:
		value.FloatValue = expr.Evaluate(args).Value().(*LiteralFloatExpression).Float()
		value.DataType = ValueTypeFloat
	case *LiteralStringExpression:
		value.StrValue = expr.Evaluate(args).Value().(*LiteralStringExpression).String()
		value.DataType = ValueTypeString
	default:
		return false
	}
	return true
}

// Convert the right literal to integer
func (be *LogicalExpression) convertToInt(right *_LiteralValue) int64 {
	switch right.DataType {
	case ValueTypeBool:
		if right.BoolValue {
			return 1
		} else {
			return 0
		}
	case ValueTypeInt:
		return right.IntValue
	case ValueTypeFloat:
		return int64(right.FloatValue)
	case ValueTypeString:
		// TODO
		return 0
	}
	return 0
}

// Convert the right literal to float
func (be *LogicalExpression) convertToFloat(right *_LiteralValue) float64 {
	switch right.DataType {
	case ValueTypeBool:
		if right.BoolValue {
			return 1.0
		} else {
			return 0.0
		}
	case ValueTypeInt:
		return float64(right.IntValue)
	case ValueTypeFloat:
		return right.FloatValue
	case ValueTypeString:
		// TODO
		return 0
	}
	return 0
}

// Convert the right literal to string
func (be *LogicalExpression) convertToString(right *_LiteralValue) string {
	switch right.DataType {
	case ValueTypeBool:
		if right.BoolValue {
			return "true"
		} else {
			return "false"
		}
	case ValueTypeInt:
		// TODO
		return "0"
	case ValueTypeFloat:
		// TODO
		return "0.0"
	case ValueTypeString:
		return right.StrValue
	}
	return ""
}

// Convert the right literal to boolean
func (be *LogicalExpression) convertToBool(right *_LiteralValue) bool {
	switch right.DataType {
	case ValueTypeBool:
		return right.BoolValue
	case ValueTypeInt:
		return right.IntValue != 0
	case ValueTypeFloat:
		return right.FloatValue != 0.0
	case ValueTypeString:
		if right.StrValue == "true" {
			return true
		} else {
			return false
		}
	}
	return false
}

// Evaluate the operation on the two literals based on their optype and return
// the boolean result
func (be *LogicalExpression) evaluateOperation(left *_LiteralValue, right *_LiteralValue) bool {
	switch left.DataType {
	case ValueTypeInt:
		switch be.opType {
		case LogicalGEQ:
			return left.IntValue >= be.convertToInt(right)
		case LogicalLEQ:
			return left.IntValue <= be.convertToInt(right)
		case LogicalEQ:
			return left.IntValue == be.convertToInt(right)
		case LogicalNEQ:
			return left.IntValue != be.convertToInt(right)
		case LogicalLT:
			return left.IntValue < be.convertToInt(right)
		case LogicalGT:
			return left.IntValue > be.convertToInt(right)
		}
	case ValueTypeFloat:
		switch be.opType {
		case LogicalGEQ:
			return left.FloatValue >= be.convertToFloat(right)
		case LogicalLEQ:
			return left.FloatValue <= be.convertToFloat(right)
		case LogicalEQ:
			return left.FloatValue == be.convertToFloat(right)
		case LogicalNEQ:
			return left.FloatValue != be.convertToFloat(right)
		case LogicalLT:
			return left.FloatValue < be.convertToFloat(right)
		case LogicalGT:
			return left.FloatValue > be.convertToFloat(right)
		}
	case ValueTypeString:
		switch be.opType {
		case LogicalGEQ:
			return left.StrValue >= be.convertToString(right)
		case LogicalLEQ:
			return left.StrValue <= be.convertToString(right)
		case LogicalEQ:
			return left.StrValue == be.convertToString(right)
		case LogicalNEQ:
			return left.StrValue != be.convertToString(right)
		case LogicalLT:
			return left.StrValue < be.convertToString(right)
		case LogicalGT:
			return left.StrValue > be.convertToString(right)
		}
	case ValueTypeBool:
		switch be.opType {
		case LogicalGEQ:
			return left.BoolValue == be.convertToBool(right)
		case LogicalLEQ:
			return left.BoolValue == be.convertToBool(right)
		case LogicalEQ:
			return left.BoolValue == be.convertToBool(right)
		case LogicalNEQ:
			return left.BoolValue != be.convertToBool(right)
		case LogicalLT:
			return left.BoolValue == be.convertToBool(right)
		case LogicalGT:
			return left.BoolValue == be.convertToBool(right)
		}
	}

	return false
}
