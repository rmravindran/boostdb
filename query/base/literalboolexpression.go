package base

import (
	"errors"

	"github.com/rmravindran/boostdb/stdlib"
)

type LiteralBoolExpression struct {
	name       string
	constValue bool
	isConst    bool
}

type LiteralBoolExpressionState struct {
	name    string
	value   bool
	isConst bool
}

// Create a new literal bool expression
func NewLiteralBoolExpression(name string) *LiteralBoolExpression {
	return &LiteralBoolExpression{
		name: name, constValue: false, isConst: false}
}

// Create a new literal bool expression with a constant value
func NewLiteralBoolConstExpression(
	name string, value bool) *LiteralBoolExpression {
	return &LiteralBoolExpression{constValue: value, isConst: true}
}

// Return the type of the sql expression that generated this expression
func (le *LiteralBoolExpression) SqlOp() SqlOpType {
	return Literal
}

// Evaluate the non-const literal expression and produces a const literal
// expression with the specified bool typed args argument. If the expression is
// already a const, evaluate with a nil parameter to produce a const expression
// containing the same value as this one. Evaluation by any argument other than
// nil on a const expression, results in error
func (le *LiteralBoolExpression) Evaluate(args interface{}) *stdlib.MaybeOp[Expression] {
	if args != nil {
		if le.isConst {
			return stdlib.ErrorOp[Expression](errors.New("cannot evaluate a constant literal expression with arguments"))
		}
		// Return a new expression with the constant value set
		return stdlib.JustOp[Expression](
			NewLiteralBoolConstExpression(le.name, args.(bool)))
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
	return le.isConst
}

// Prepare an expression
func (le *LiteralBoolExpression) Prepare(
	nameHandler ArgNameHandler) ExpressionState {

	if !le.isConst && nameHandler != nil {
		nameHandler(le.name)
	}

	return &LiteralBoolExpressionState{
		name: le.name, value: le.constValue, isConst: le.isConst}
}

// Returns the current bool value of the state if the expression state is
// non-const, otherwise returns nil
func (les *LiteralBoolExpressionState) ToArgs() interface{} {
	if les.isConst {
		return nil
	}

	return les.value
}

// Set the value of the expression
func (les *LiteralBoolExpressionState) SetValue(name string, value any) error {
	if name != les.name {
		return nil
	}

	if les.isConst {
		return errors.New("cannot set the value of a constant literal expression")
	}
	switch v := value.(type) {
	case bool:
		les.value = v
		return nil
	}
	return errors.New("invalid value type")
}

// Return true if the expression is a constant, otherwise returns false
func (les *LiteralBoolExpressionState) IsConstant() bool {
	return les.isConst
}
