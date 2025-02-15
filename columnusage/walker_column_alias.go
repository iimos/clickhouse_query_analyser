package columnusage

import parser "github.com/iimos/clickhouse_query_analyser/internal/clickhouse_parser"

func collectAliasesFromSelect(ctx *parser.SelectStmtContext) []columnExpr {
	w := &columnAliasWalker{rootSelect: ctx}
	walk(w, ctx)
	return w.vars
}

type columnAliasWalker struct {
	*parser.BaseClickHouseParserListener

	// vars accumulate all named expressions
	vars           []columnExpr
	rootSelect     *parser.SelectStmtContext
	skipUntilLeave *parser.SelectStmtContext
}

func (w *columnAliasWalker) EnterSelectStmt(ctx *parser.SelectStmtContext) {
	if ctx != w.rootSelect && w.skipUntilLeave == nil {
		w.skipUntilLeave = ctx
	}
}

func (w *columnAliasWalker) ExitSelectStmt(ctx *parser.SelectStmtContext) {
	if ctx == w.skipUntilLeave {
		w.skipUntilLeave = nil
	}
}

func (w *columnAliasWalker) EnterColumnExprAlias(ctx *parser.ColumnExprAliasContext) {
	if w.skipUntilLeave != nil {
		return
	}

	ew := &columnExprWalker{}
	if ctx.AS() == nil {
		ew.alias = unquote(ctx.Alias().GetText())
	} else {
		ew.alias = unquote(ctx.Identifier().GetText())
	}
	walk(ew, ctx)
	w.vars = append(w.vars, ew.columnExpr)
}

func (w *columnAliasWalker) PropagationStopped() bool {
	return false
}
