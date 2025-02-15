package columnusage

import (
	"fmt"

	"github.com/antlr/antlr4/runtime/Go/antlr"
	parser "github.com/iimos/clickhouse_query_analyser/internal/clickhouse_parser"
)

type columnExpr struct {
	alias      string
	namepath   namepath
	usedFields []field
	subselect  *scope

	// privateVariables contains variables defined in the columnExpr
	// Example: arrayMap(x -> 1, arr) defines variable x in itself
	// It's an ugly solution but for now it fits our needs
	privateVariables []string
}

func (ce columnExpr) isPrivateVariable(x namepath) bool {
	if len(x) != 1 {
		return false
	}
	for _, v := range ce.privateVariables {
		if v == x[0] {
			return true
		}
	}
	return false
}

type columnExprWalker struct {
	*parser.BaseClickHouseParserListener
	columnExpr

	// vars accumulate all named expressions
	vars            []columnExpr
	stopPropagation bool
}

func (w *columnExprWalker) EnterEveryRule(ctx antlr.ParserRuleContext) {
	if debug {
		fmt.Printf("columnExprWalker: %T \t\t| %s \n", ctx, ctx.GetText())
	}
}

func (w *columnExprWalker) PropagationStopped() bool {
	stop := w.stopPropagation
	w.stopPropagation = false
	return stop
}

func (w *columnExprWalker) EnterColumnIdentifier(ctx *parser.ColumnIdentifierContext) {
	col := unquote(ctx.NestedIdentifier().GetText())
	if ctx.TableIdentifier() == nil {
		f := field{namepath: namepath{col}}
		w.usedFields = append(w.usedFields, f)
	} else {
		name := make([]string, 0)

		for _, ch := range ctx.TableIdentifier().GetChildren() {
			switch x := ch.(type) {
			case *parser.IdentifierContext:
				name = append(name, unquote(x.GetText()))
			case *parser.DatabaseIdentifierContext:
				name = append(name, unquote(x.GetText()))
			case *antlr.TerminalNodeImpl:
				// pass
			default:
				// todo
				panic("unexpected")
			}
		}

		name = append(name, col)
		f := field{namepath: name}
		w.usedFields = append(w.usedFields, f)
	}
}

func (w *columnExprWalker) EnterColumnLambdaExpr(ctx *parser.ColumnLambdaExprContext) {
	params := make([]string, 0)
	children := ctx.GetChildren()

	for _, ch := range children {
		if x, ok := ch.(*parser.IdentifierContext); ok {
			id := unquote(x.GetText())
			params = append(params, id)
		}
		if ch == ctx.ARROW() {
			break
		}
	}

	w.privateVariables = append(w.privateVariables, params...)

	if len(children) == 0 {
		return
	}
	body := children[len(children)-1]
	bodyWalker := &columnExprWalker{}
	walk(bodyWalker, body)
}

func (w *columnExprWalker) EnterSelectStmt(ctx *parser.SelectStmtContext) {
	sw := newSelectWalker()
	walk(sw, ctx)

	w.subselect = sw.scope
	w.stopPropagation = true
}
