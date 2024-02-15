package parser

import (
	"errors"
	"fmt"

	sqlparser "github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/rmravindran/boostdb/query/base"

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
		fmt.Println("Join " + n.Text())
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
				v.queryOps.AddSourceFetchOp(source.SourceName, source.Alias)
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
				v.queryOps.AddSelectFieldOp(field.FieldName, field.Source)
			}
		}

		//- FROM
		// Pingcap's parser represents joins as a tree under the FROM cluase.
		// They do this to capture the generalization that a single table is
		// also a join (with no right node).
		v.CurrentParseState = ParsingFrom
		n.From.Accept(v)

		//- Parse WHERE
		// The WHERE clause is a filter on the result set. It can also
		// represent an implicit join condition.
		v.CurrentParseState = ParsingWhere
		n.Where.Accept(v)

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
		skipChildren = true
	}
	return in, skipChildren
}

func (v *Visitor) Leave(in ast.Node) (ast.Node, bool) {
	v.CurrentParseState = ParsingNone
	return in, true
}
