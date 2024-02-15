package base

import "github.com/rmravindran/boostdb/stdlib"

type LiteralStringExpression struct {
	constValue string
}

// Create a new literal string expression
func NewLiteralStringExpression(value string) *LiteralStringExpression {
	return &LiteralStringExpression{constValue: value}
}

// Return the type of the sql expression that generated this expression
func (le *LiteralStringExpression) SqlOp() SqlOpType {
	return Literal
}

// Evaluate the expression by setting the constant value to the given value
// if a value is provided, otherwise returns the expression
func (le *LiteralStringExpression) Evaluate(args ...interface{}) *stdlib.MaybeOp[Expression] {
	if len(args) > 0 {
		le.constValue = args[0].(string)
	}
	return stdlib.JustOp[Expression](le)
}

// Return the final values produced by the operator, otherwise returns nil
func (le *LiteralStringExpression) Value() any {
	return le
}

// Return the string value
func (le *LiteralStringExpression) String() string {
	return le.constValue
}

// Return true to indicate that this is a constant
func (le *LiteralStringExpression) IsConstant() bool {
	return true
}

// Return the Initial State
func (le *LiteralStringExpression) InitState() []interface{} {
	return nil
}

// Prepare an expression
func (le *LiteralStringExpression) Prepare() ExpressionState {
	return le
}

type LiteralFloatExpression struct {
	constValue float64
}

// Create a new literal float expression
func NewLiteralFloatExpression(value float64) *LiteralFloatExpression {
	return &LiteralFloatExpression{constValue: value}
}

// Return the type of the sql expression that generated this expression
func (le *LiteralFloatExpression) SqlOp() SqlOpType {
	return Literal
}

// Evaluate the expression by setting the constant value to the given value
// if a value is provided, otherwise returns the expression
func (le *LiteralFloatExpression) Evaluate(args ...interface{}) *stdlib.MaybeOp[Expression] {
	if len(args) > 0 {
		le.constValue = args[0].(float64)
	}
	return stdlib.JustOp[Expression](le)
}

// Return the final values produced by the operator, otherwise returns nil
func (le *LiteralFloatExpression) Value() any {
	return le
}

// Return the float value
func (le *LiteralFloatExpression) Float() float64 {
	return le.constValue
}

// Return true to indicate that this is a constant
func (le *LiteralFloatExpression) IsConstant() bool {
	return true
}

type LiteralIntExpression struct {
	constValue int64
}

// Create a new literal int expression
func NewLiteralIntExpression(value int64) *LiteralIntExpression {
	return &LiteralIntExpression{constValue: value}
}

// Return the type of the sql expression that generated this expression
func (le *LiteralIntExpression) SqlOp() SqlOpType {
	return Literal
}

// Evaluate the expression by setting the constant value to the given value
// if a value is provided, otherwise returns the expression
func (le *LiteralIntExpression) Evaluate(args ...interface{}) *stdlib.MaybeOp[Expression] {
	if len(args) > 0 {
		le.constValue = args[0].(int64)
	}
	return stdlib.JustOp[Expression](le)
}

// Return the final values produced by the operator, otherwise returns nil
func (le *LiteralIntExpression) Value() any {
	return le
}

// Return the integer value
func (le *LiteralIntExpression) Int() int64 {
	return le.constValue
}

// Return true to indicate that this is a constant
func (le *LiteralIntExpression) IsConstant() bool {
	return true
}

type LiteralBoolExpression struct {
	constValue bool
}

// Create a new literal bool expression
func NewLiteralBoolExpression(value bool) *LiteralBoolExpression {
	return &LiteralBoolExpression{constValue: value}
}

// Return the type of the sql expression that generated this expression
func (le *LiteralBoolExpression) SqlOp() SqlOpType {
	return Literal
}

// Evaluate the expression by setting the constant value to the given value
// if a value is provided, otherwise returns the expression
func (le *LiteralBoolExpression) Evaluate(args ...interface{}) *stdlib.MaybeOp[Expression] {
	if len(args) > 0 {
		le.constValue = args[0].(bool)
	}
	return stdlib.JustOp[Expression](le)
}

// Return the final values produced by the operator, otherwise returns nil
func (le *LiteralBoolExpression) Value() any {
	return le
}

// Return the boolean value
func (le *LiteralBoolExpression) Bool() bool {
	return le.constValue
}

// Return true to indicate that this is a constant
func (le *LiteralBoolExpression) IsConstant() bool {
	return true
}
