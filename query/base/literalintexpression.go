package base

import (
	"errors"

	"github.com/rmravindran/boostdb/stdlib"
)

type LiteralIntExpression struct {
	name       string
	constValue int64
	isConst    bool
}

type LiteralIntExpressionState struct {
	name    string
	value   int64
	isConst bool
}

// Create a new literal int expression
func NewLiteralIntExpression(name string) *LiteralIntExpression {
	return &LiteralIntExpression{name: name, constValue: 0, isConst: false}
}

// Create a new literal int expression with a constant value
func NewLiteralIntConstExpression(
	name string, value int64) *LiteralIntExpression {
	return &LiteralIntExpression{name: name, constValue: value, isConst: true}
}

// Return the type of the sql expression that generated this expression
func (le *LiteralIntExpression) SqlOp() SqlOpType {
	return Literal
}

// Evaluate the non-const literal expression and produces a const literal
// expression with the specified int64 typed args argument. If the expression
// is already a const, evaluate with a nil parameter to produce a const
// expression containing the same value as this one. Evaluation by any argument
// other than nil on a const expression, results in error
func (le *LiteralIntExpression) Evaluate(args interface{}) *stdlib.MaybeOp[Expression] {
	if args != nil {
		if le.isConst {
			return stdlib.ErrorOp[Expression](errors.New("cannot evaluate a constant literal expression with arguments"))
		}
		// Return a new expression with the constant value set
		return stdlib.JustOp[Expression](
			NewLiteralIntConstExpression(le.name, args.(int64)))
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
	return le.isConst
}

// Prepare an expression
func (le *LiteralIntExpression) Prepare(
	nameHandler ArgNameHandler) ExpressionState {

	if !le.isConst && nameHandler != nil {
		nameHandler(le.name)
	}

	return &LiteralIntExpressionState{
		name: le.name, value: le.constValue, isConst: le.isConst}
}

// Returns the current int64 value of the state if the expression state is
// non-const, otherwise returns nil
func (le *LiteralIntExpressionState) ToArgs() interface{} {
	if le.isConst {
		return nil
	}

	return le.value
}

// Set the value of the expression
func (les *LiteralIntExpressionState) SetValue(name string, value any) error {
	if les.name != name {
		return nil
	}

	if les.isConst {
		return errors.New("cannot set the value of a constant literal expression")
	}

	switch v := value.(type) {
	case int64:
		les.value = v
		return nil
	}

	return errors.New("invalid value type")
}

// Return true if the expression is a constant, otherwise returns false
func (les *LiteralIntExpressionState) IsConstant() bool {
	return les.isConst
}
