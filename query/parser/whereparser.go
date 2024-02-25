package parser

import (
	"errors"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/opcode"
	"github.com/pingcap/tidb/pkg/parser/test_driver"
	"github.com/rmravindran/boostdb/query/base"
)

// Where Parser state type
type WhereParseState int

const (
	// Parsing None
	ParsingWhereNone WhereParseState = iota

	// Parsing the root node that holds all the expressions
	ParsingWhereFieldsNode

	// Parsing a binary expression
	ParsingWhereBinaryExp

	// Parsing a column name expression node
	ParsingWhereColNameExprNode

	// Parsing a column name node
	ParsingWhereColNameNode
)

// Where Operator Type
type WhereOperatorType int

const (
	// Unknown
	WhereOpUnknown WhereOperatorType = iota

	// Greater than or equal
	WhereOpGEQ

	// Less than or equal
	WhereOpLEQ

	// Equal
	WhereOpEQ

	// Not Equal
	WhereOpNEQ

	// Less than
	WhereOpLT

	// Greater than
	WhereOpGT

	// Note that SQL WHERE operator combines both logical and boolean
	// expressions into one BinaryExpr type. We have distinctive types to
	// represent both due to the wide difference in optimization steps

	// Conjunction operator
	WhereOpAND

	// Disjunction operator
	WhereOpOR
)

// Expression Type
type ExpressionType int

const (
	// Column Name
	ExpTypeLogicalExpression ExpressionType = iota

	// Column Name
	ExpTypeColumnNameExpression

	// Value Expression (i.e a constant of some sort)
	ExpTypeValueExpression
)

type ConstantValue struct {
	DataType    base.ValueType
	IntValue    int64
	FloatValue  float64
	StringValue string
}

type WhereExpression struct {
	Left              *WhereExpression
	Right             *WhereExpression
	Operator          WhereOperatorType
	Attribute         string
	Series            string
	SeriesFamilyAlias string
	Type              ExpressionType
	ValueData         ConstantValue
}

type WhereExpressionVisitor struct {
	CurrentParseState WhereParseState

	// Is state valid
	IsValid bool

	// Error message
	Error error

	// Parsing text
	ParsingText string

	// Currently parsing expression
	currentExpression *WhereExpression

	// Expression Stack
	RootExpression *WhereExpression
}

// Create a new where expression visitor
func NewWhereExpressionVisitor() *WhereExpressionVisitor {
	return &WhereExpressionVisitor{
		CurrentParseState: ParsingWhereNone,
		IsValid:           true,
		Error:             nil,
		ParsingText:       "",
		currentExpression: nil,
		RootExpression:    nil,
	}
}

func (v *WhereExpressionVisitor) Enter(in ast.Node) (ast.Node, bool) {
	skipChildren := false

	// Switch on the type of the node
	switch n := in.(type) {
	case *ast.BinaryOperationExpr:
		if v.CurrentParseState == ParsingWhereNone {
			v.ParsingText = n.Text()
			// Root expression
			v.currentExpression = &WhereExpression{}
			v.RootExpression = v.currentExpression
		}

		// Only these operations are supported
		switch n.Op {
		case opcode.GE:
			v.currentExpression.Operator = WhereOpGEQ
		case opcode.LE:
			v.currentExpression.Operator = WhereOpLEQ
		case opcode.EQ:
			v.currentExpression.Operator = WhereOpEQ
		case opcode.NE:
			v.currentExpression.Operator = WhereOpNEQ
		case opcode.LT:
			v.currentExpression.Operator = WhereOpLT
		case opcode.GT:
			v.currentExpression.Operator = WhereOpGT
		case opcode.LogicAnd:
			v.currentExpression.Operator = WhereOpAND
		case opcode.LogicOr:
			v.currentExpression.Operator = WhereOpOR
		default:
			v.IsValid = false
			if v.Error == nil {
				v.Error = errors.New("unknown logical operator type at '" + v.ParsingText + "'")
				skipChildren = true
			}
		}

		if !v.IsValid {
			break
		}

		// This is a binary logical expression
		v.currentExpression.Type = ExpTypeLogicalExpression

		// Traverse the left and Right
		v.CurrentParseState = ParsingWhereBinaryExp

		parent := v.currentExpression

		// Parse the Left expression recursively
		if n.L != nil {
			parent.Left = &WhereExpression{}
			v.currentExpression = parent.Left
			n.L.Accept(v)

			// Upon unwiding, we should be back in the binary expression state
			if v.CurrentParseState != ParsingWhereBinaryExp {
				v.IsValid = false
				if v.Error == nil {
					v.Error = errors.New("invalid syntax '" + v.ParsingText + "'")
					skipChildren = true
				}
				break
			}
		}

		if n.R != nil {
			parent.Right = &WhereExpression{}
			v.currentExpression = parent.Right
			n.R.Accept(v)

			// Upon unwiding, we should be back in the binary expression state
			if v.CurrentParseState != ParsingWhereBinaryExp {
				v.IsValid = false
				if v.Error == nil {
					v.Error = errors.New("invalid syntax '" + v.ParsingText + "'")
					skipChildren = true
				}
				break
			}
		}

		skipChildren = true
	case *test_driver.ValueExpr:
		v.currentExpression.Type = ExpTypeValueExpression
		switch n.Datum.Kind() {
		case test_driver.KindInt64:
			v.currentExpression.ValueData.DataType = base.ValueTypeInt
			v.currentExpression.ValueData.IntValue = n.Datum.GetInt64()
		case test_driver.KindUint64:
			v.currentExpression.ValueData.DataType = base.ValueTypeInt
			v.currentExpression.ValueData.IntValue = int64(n.Datum.GetUint64())
		case test_driver.KindFloat32:
			v.currentExpression.ValueData.DataType = base.ValueTypeFloat
			v.currentExpression.ValueData.FloatValue = float64(n.Datum.GetFloat32())
		case test_driver.KindFloat64:
			v.currentExpression.ValueData.DataType = base.ValueTypeFloat
			v.currentExpression.ValueData.FloatValue = n.Datum.GetFloat64()
		case test_driver.KindString:
			v.currentExpression.ValueData.DataType = base.ValueTypeString
			v.currentExpression.ValueData.StringValue = n.Datum.GetString()
		}
		skipChildren = true
	case *ast.ColumnNameExpr:
		if v.CurrentParseState != ParsingWhereBinaryExp {
			// Error
			v.IsValid = false
			if v.Error == nil {
				v.Error = errors.New("invalid syntax '" + v.ParsingText + "'")
				skipChildren = true
			}
		}

		v.CurrentParseState = ParsingWhereColNameExprNode
	case *ast.ColumnName:
		if v.CurrentParseState != ParsingWhereColNameExprNode {
			// Error
			v.IsValid = false
			if v.Error == nil {
				v.Error = errors.New("invalid syntax '" + v.ParsingText + "'")
				skipChildren = true
			}
		}
		v.currentExpression.Type = ExpTypeColumnNameExpression
		v.CurrentParseState = ParsingWhereColNameNode
		attributeName := n.Name.O
		seriesName := n.Table.O
		seriesFamilyAlias := n.Schema.O
		if n.Table.O == "" {
			attributeName = "value"
			seriesName = n.Name.O
		} else if n.Schema.O == "" {
			attributeName = "value"
			seriesName = n.Name.O
			seriesFamilyAlias = n.Table.O
		}

		v.currentExpression.Attribute = attributeName
		v.currentExpression.Series = seriesName
		v.currentExpression.SeriesFamilyAlias = seriesFamilyAlias
		skipChildren = true
	}

	return in, skipChildren
}

func (v *WhereExpressionVisitor) Leave(in ast.Node) (ast.Node, bool) {
	switch in.(type) {
	case *ast.BinaryOperationExpr:
		if v.CurrentParseState != ParsingWhereBinaryExp {
			// Error
			v.IsValid = false
			if v.Error == nil {
				v.Error = errors.New("invalid syntax '" + v.ParsingText + "'")
			}
		}
		// We are done with parsing both left and right. Clear the current
		// expression to indicate that we are done with this expression
		v.currentExpression = nil
	case *ast.ColumnName:
		if v.CurrentParseState != ParsingWhereColNameNode {
			// Error
			v.IsValid = false
			if v.Error == nil {
				v.Error = errors.New("invalid syntax '" + v.ParsingText + "'")
			}
		}
		v.CurrentParseState = ParsingWhereColNameExprNode
	case *ast.ColumnNameExpr:
		if v.CurrentParseState != ParsingWhereColNameExprNode {
			// Error
			v.IsValid = false
			if v.Error == nil {
				v.Error = errors.New("invalid syntax '" + v.ParsingText + "'")
			}
		}
		v.CurrentParseState = ParsingWhereBinaryExp
	case *test_driver.ValueExpr:
		if v.CurrentParseState != ParsingWhereBinaryExp {
			// Error
			v.IsValid = false
			if v.Error == nil {
				v.Error = errors.New("invalid syntax '" + v.ParsingText + "'")
			}
		}
		v.currentExpression = nil
	}

	return in, true
}
