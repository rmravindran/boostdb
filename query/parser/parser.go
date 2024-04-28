package parser

import (
	"errors"
	"fmt"

	sqlparser "github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/rmravindran/boostdb/query/base"
	"github.com/rmravindran/boostdb/stdlib"

	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
)

type Parser struct {
	astGen *sqlparser.Parser
}

func NewParser() *Parser {
	return &Parser{
		astGen: sqlparser.New(),
	}
}

func (p *Parser) Parse(sql string) (*base.QueryOps, error) {
	stmtNodes, _, err := p.astGen.ParseSQL(sql)

	if err != nil {
		return nil, err
	}

	if stmtNodes == nil {
		return nil, errors.New("no sql statements found")
	}

	if len(stmtNodes) != 1 {
		return nil, errors.New("only one sql statement is allowed")
	}

	rootNode := &stmtNodes[0]
	v := &Visitor{
		CurrentParseState: ParsingNone,
		queryOps:          base.NewQueryOps(),
	}
	(*rootNode).Accept(v)

	if v.Error != nil {
		return nil, v.Error
	}

	return v.queryOps, nil
}

// Parse state type
type ParseState int

const (
	// Parsing None
	ParsingNone ParseState = iota

	// Parsing Select
	ParsingSelect

	// Parsing From
	ParsingFrom

	// Parsing Where
	ParsingWhere

	// Parsing  GroupBy
	ParsingGroupBy

	// Parsing Having
	ParsingHaving

	// Parsing OrderBy
	ParsingOrderBy
)

// Visitor Visitor
type Visitor struct {
	CurrentParseState ParseState

	// Error message
	Error error

	queryOps *base.QueryOps
}

func (v *Visitor) Enter(in ast.Node) (ast.Node, bool) {
	skipChildren := false
	switch n := in.(type) {
	case *ast.Join:
		if v.CurrentParseState != ParsingFrom {
			v.Error = fmt.Errorf("Join not allowed here '" + n.Text() + "'")
			skipChildren = true
			break
		}
		joinVisitor := NewJoinVisitor()
		n.Accept(joinVisitor)
		if joinVisitor.IsValid {
			// Add sources
			for _, source := range joinVisitor.Sources {
				v.queryOps.AddSourceFetchOp(source.SourceDomain, source.SourceName, source.Alias)
			}

			// Add joins
			for _, join := range joinVisitor.Joins {
				v.queryOps.AddJoinOp(join.LeftSource, join.LeftColumn, join.RightSource, join.RightColumn)
			}

		} else {
			v.Error = joinVisitor.Error
		}
		skipChildren = true
	case *ast.SelectStmt:

		//- Visit the fields in the SELECT statement as part of the
		//- SELECT operation.
		v.CurrentParseState = ParsingSelect
		selectVisitor := NewSelectFieldsVisitor()
		n.Fields.Accept(selectVisitor)
		v.CurrentParseState = ParsingNone
		if selectVisitor.IsInvalid {
			v.Error = selectVisitor.Error
			skipChildren = true
			break
		} else {
			for _, field := range selectVisitor.Fields {
				v.queryOps.AddSelectFieldOp(field.SeriesFamilyAlias, field.Series, field.Attribute)
			}
		}

		//- FROM
		// Pingcap's parser represents joins as a tree under the FROM cluase.
		// They do this to capture the generalization that a single table is
		// also a join (with no right node).
		v.CurrentParseState = ParsingFrom
		n.From.Accept(v)
		if v.Error != nil {
			skipChildren = true
			break
		}

		//- Parse WHERE
		// The WHERE clause is a filter on the result set. It can also
		// represent an implicit join condition.
		if n.Where != nil {
			v.CurrentParseState = ParsingWhere
			n.Where.Accept(v)
		}

		// We visited everything we need explicitly
		skipChildren = true
	case *ast.BinaryOperationExpr:
		fmt.Println("Where " + n.Text())
		if v.CurrentParseState != ParsingWhere {
			v.Error = fmt.Errorf("Where not allowed here '" + n.Text() + "'")
			skipChildren = true
			break
		}

		whereVisitor := NewWhereExpressionVisitor()
		n.Accept(whereVisitor)
		if whereVisitor.IsValid {
			expr := v.convertWhereExpressionToLogicalExpression(whereVisitor.RootExpression)
			v.queryOps.AddWhereExpression(expr)
		} else {
			v.Error = whereVisitor.Error
		}

		skipChildren = true
	}
	return in, skipChildren
}

func (v *Visitor) Leave(in ast.Node) (ast.Node, bool) {
	v.CurrentParseState = ParsingNone
	return in, true
}

// Create LiteralConstantExpression From WhereExpression
func (v *Visitor) createLiteralConstantExpression(
	whereExpr *WhereExpression) *stdlib.MaybeOp[base.Expression] {

	name := whereExpr.SeriesFamilyAlias + "." + whereExpr.Series + "." + whereExpr.Attribute

	switch whereExpr.ValueData.DataType {
	case base.ValueTypeInt:
		return stdlib.JustOp[base.Expression](base.NewLiteralIntConstExpression(name, whereExpr.ValueData.IntValue))
	case base.ValueTypeFloat:
		return stdlib.JustOp[base.Expression](base.NewLiteralFloatConstExpression(name, whereExpr.ValueData.FloatValue))
	case base.ValueTypeString:
		return stdlib.JustOp[base.Expression](base.NewLiteralStringConstExpression(name, whereExpr.ValueData.StringValue))
	}

	return nil
}

// Convert WhereExpression to LogicalExpression
func (v *Visitor) convertWhereExpressionToLogicalExpression(
	whereExpr *WhereExpression) *stdlib.MaybeOp[base.Expression] {
	// Convert the where expression to a logical expression

	logicalOpType := base.LogicalAnd
	switch whereExpr.Operator {
	case WhereOpGEQ:
		logicalOpType = base.LogicalGEQ
	case WhereOpLEQ:
		logicalOpType = base.LogicalLEQ
	case WhereOpEQ:
		logicalOpType = base.LogicalEQ
	case WhereOpNEQ:
		logicalOpType = base.LogicalNEQ
	case WhereOpLT:
		logicalOpType = base.LogicalLT
	case WhereOpGT:
		logicalOpType = base.LogicalGT
	case WhereOpAND:
		logicalOpType = base.LogicalAnd
	case WhereOpOR:
		logicalOpType = base.LogicalOr
	}

	var leftExpr *stdlib.MaybeOp[base.Expression] = nil
	var rightExpr *stdlib.MaybeOp[base.Expression] = nil

	// Process Left Expression
	if whereExpr.Left.Type == ExpTypeValueExpression {
		leftExpr = v.createLiteralConstantExpression(whereExpr.Left)
	} else if whereExpr.Left.Type == ExpTypeLogicalExpression {
		// Recurse
		leftExpr = v.convertWhereExpressionToLogicalExpression(whereExpr.Left)
	} else if whereExpr.Left.Type == ExpTypeColumnNameExpression {
		colName := whereExpr.Left.Series + "." + whereExpr.Left.Attribute
		if whereExpr.Left.SeriesFamilyAlias != "" {
			colName = whereExpr.Left.SeriesFamilyAlias + "." + colName
		}
		leftExpr = stdlib.JustOp[base.Expression](base.NewColumnNameExpression(colName, base.Where))
	}

	// Process Right Expression
	if whereExpr.Right.Type == ExpTypeValueExpression {
		rightExpr = v.createLiteralConstantExpression(whereExpr.Right)
	} else if whereExpr.Right.Type == ExpTypeLogicalExpression {
		// Recurse
		rightExpr = v.convertWhereExpressionToLogicalExpression(whereExpr.Right)
	} else if whereExpr.Right.Type == ExpTypeColumnNameExpression {
		colName := whereExpr.Right.SeriesFamilyAlias + "." + whereExpr.Right.Series + "." + whereExpr.Right.Attribute
		rightExpr = stdlib.JustOp[base.Expression](base.NewColumnNameExpression(colName, base.Where))
	}

	// Create the logical expression
	return stdlib.JustOp[base.Expression](base.NewLogicalExpression(
		base.Where, logicalOpType, leftExpr, rightExpr))
}
