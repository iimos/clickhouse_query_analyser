package columnusage

import (
	"fmt"

	"github.com/antlr/antlr4/runtime/Go/antlr"
	parser "github.com/iimos/clickhouse_query_analyser/internal/clickhouse_parser"
)

func collectIdentifiers(ctx antlr.Tree) []string {
	iw := &identifierWalker{}
	walk(iw, ctx)
	return iw.parts
}

type identifierWalker struct {
	*parser.BaseClickHouseParserListener
	parts []string
}

func (w *identifierWalker) EnterEveryRule(ctx antlr.ParserRuleContext) {
	if debug {
		fmt.Printf("identifierWalker: %T \t\t| %s \n", ctx, ctx.GetText())
	}
}

func (w *identifierWalker) EnterIdentifier(ctx *parser.IdentifierContext) {
	text := unquote(ctx.GetText())
	w.parts = append(w.parts, text)
}

func (w *identifierWalker) PropagationStopped() bool {
	return false
}
