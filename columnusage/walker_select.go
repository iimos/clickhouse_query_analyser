package columnusage

import (
	"fmt"
	"strings"

	"github.com/antlr/antlr4/runtime/Go/antlr"
	parser "github.com/iimos/clickhouse_query_analyser/internal/clickhouse_parser"
)

type selectWalker struct {
	*parser.BaseClickHouseParserListener
	*scope
	isRootSelectHandled bool
	stopPropagation     bool
}

func newSelectWalker() *selectWalker {
	return &selectWalker{
		scope: newScope(nil, ""),
	}
}

func (w *selectWalker) PropagationStopped() bool {
	stop := w.stopPropagation
	w.stopPropagation = false
	return stop
}

func (w *selectWalker) EnterEveryRule(ctx antlr.ParserRuleContext) {
	if debug {
		fmt.Printf("selectWalker: %T \t\t| %s \n", ctx, ctx.GetText())
	}
}

func (w *selectWalker) EnterColumnExprList(ctx *parser.ColumnExprListContext) {
	w.stopPropagation = true

	_, inSelect := ctx.GetParent().(*parser.SelectStmtContext)
	fields := w.scope.currentSectionFields()

	for _, ch := range ctx.GetChildren() {
		exprWalker := &columnExprWalker{}
		hasExplicitAlias := false

		switch che := ch.(type) {
		case *antlr.TerminalNodeImpl:
			continue

		case *parser.ColumnsExprColumnContext:
			expr := che.ColumnExpr()

			switch e := expr.(type) {
			case *parser.ColumnExprAliasContext:
				hasExplicitAlias = true
				if e.AS() == nil {
					exprWalker.alias = unquote(e.Alias().GetText())
				} else {
					exprWalker.alias = unquote(e.Identifier().GetText())
				}
			case *parser.ColumnExprIdentifierContext:
				parts := collectIdentifiers(e)
				if len(parts) == 0 {
					panic("ColumnExprIdentifier not contains any Identifier: " + e.GetText())
				}
				exprWalker.namepath = parts
			case *parser.ColumnExprLiteralContext:
				exprWalker.alias = e.GetText()
			default:
				if x, ok := ch.(antlr.ParseTree); ok {
					exprWalker.alias = x.GetText()
				}
			}

			walk(exprWalker, expr)

		case *parser.ColumnsExprAsteriskContext:
			if che.ASTERISK() == nil {
				break
			}

			exprWalker.alias = "*"
			if che.TableIdentifier() == nil {
				// select * from table
				f := field{namepath: namepath{"*"}}
				exprWalker.usedFields = append(exprWalker.usedFields, f)
			} else {
				// select table.* from table
				table := unquote(che.TableIdentifier().GetText())
				f := field{namepath: namepath{table, "*"}}
				exprWalker.usedFields = append(exprWalker.usedFields, f)
			}

		default:
			walk(exprWalker, ch)
		}

		if inSelect {
			w.scope.export = append(w.scope.export, exprWalker.columnExpr)
		}
		if len(exprWalker.usedFields) == 0 && len(exprWalker.namepath) == 0 && exprWalker.subselect == nil && hasExplicitAlias {
			//case: with 1 as a select a
			exprWalker.subselect = newScope(nil, "")
		}
		if exprWalker.subselect != nil {
			exprWalker.subselect.externalName = exprWalker.alias
			w.scope.children[columnSubscopeKey(exprWalker.alias)] = exprWalker.subselect
			exprWalker.subselect.parent = w.scope
		}
		*fields = append(*fields, exprWalker.usedFields...)

		w.vars = append(w.vars, exprWalker.vars...)
	}
}

func columnSubscopeKey(alias string) string {
	return "## column - " + alias
}

// isIdentifierInComparison looks for comparison of column with constant expression.
// Examples: aaa = '2022-01-12', aaa < 123
func isIdentifierInComparison(c antlr.Tree) (comparisonOp, comparisonWith string, ok bool) {
	if c.GetParent() == nil || c.GetParent().GetParent() == nil {
		return "", "", false
	}

	ctx, ok := c.GetParent().GetParent().(*parser.ColumnExprPrecedence3Context)
	if !ok {
		return "", "", false
	}

	var op antlr.TerminalNode
	switch {
	case ctx.EQ_SINGLE() != nil: // =
		op = ctx.EQ_SINGLE()
	case ctx.NOT_EQ() != nil: // !=
		op = ctx.NOT_EQ()
	case ctx.LT() != nil: // <
		op = ctx.LT()
	case ctx.LE() != nil: // <=
		op = ctx.LE()
	case ctx.GT() != nil: // >
		op = ctx.GT()
	case ctx.GE() != nil: // >=
		op = ctx.GE()
	case ctx.LIKE() != nil: // like
		op = ctx.LIKE()
	case ctx.ILIKE() != nil: // ilike
		op = ctx.ILIKE()
	default:
		return "", "", false
	}

	left, right := ctx.ColumnExpr(0), ctx.ColumnExpr(1)
	lids, rids := collectIdentifiers(left), collectIdentifiers(right)

	var idExpr, constExpr parser.IColumnExprContext
	var columnName string

	if len(lids) == 1 && len(rids) == 0 {
		idExpr, constExpr = left, right
		columnName = lids[0]
	} else if len(rids) == 1 && len(lids) == 0 {
		constExpr, idExpr = left, right
		columnName = rids[0]
	} else {
		return "", "", false
	}

	// ensure non constant Expression consists of identifier only
	if columnName == idExpr.GetText() {
		oper := strings.ToLower(op.GetText())
		return oper, constExpr.GetText(), true
	}
	return "", "", false
}

func namepathFromIdentifier(ctx *parser.ColumnIdentifierContext) namepath {
	col := ctx.NestedIdentifier().GetText()
	col = unquote(col)

	if ctx.TableIdentifier() == nil {
		return []string{col}
	}

	table := unquote(ctx.TableIdentifier().GetText())
	return []string{table, col}
}

func (w *selectWalker) EnterColumnIdentifier(ctx *parser.ColumnIdentifierContext) {
	name := namepathFromIdentifier(ctx)
	comparisonOp, comparisonWith, _ := isIdentifierInComparison(ctx)

	fields := w.scope.currentSectionFields()
	*fields = append(*fields, field{
		namepath: name,
		context: fieldContext{
			comparisonOp:   comparisonOp,
			comparisonWith: comparisonWith,
		},
	})
}

func (w *selectWalker) EnterWithClause(ctx *parser.WithClauseContext) {
	w.scope.currentSection = withClause
}
func (w *selectWalker) ExitWithClause(ctx *parser.WithClauseContext) {
	w.scope.currentSection = selectClause
}

func (w *selectWalker) EnterFromClause(ctx *parser.FromClauseContext) {
	w.scope.currentSection = fromClause
}
func (w *selectWalker) EnterWhereClause(ctx *parser.WhereClauseContext) {
	w.scope.currentSection = whereClause
}
func (w *selectWalker) EnterPrewhereClause(ctx *parser.PrewhereClauseContext) {
	w.scope.currentSection = prewhereClause
}
func (w *selectWalker) EnterGroupByClause(ctx *parser.GroupByClauseContext) {
	w.scope.currentSection = groupByClause
}
func (w *selectWalker) EnterHavingClause(ctx *parser.HavingClauseContext) {
	w.scope.currentSection = havingClause
}
func (w *selectWalker) EnterOrderByClause(ctx *parser.OrderByClauseContext) {
	w.scope.currentSection = orderByClause
}
func (w *selectWalker) EnterLimitByClause(ctx *parser.LimitByClauseContext) {
	w.scope.currentSection = limitByClause
}

func (w *selectWalker) EnterTableExprAlias(ctx *parser.TableExprAliasContext) {
	var alias string
	if ctx.AS() != nil {
		alias = unquote(ctx.Identifier().GetText())
	} else {
		alias = unquote(ctx.Alias().GetText())
	}

	table := ctx.TableExpr()
	if w.scope.tableDecls == nil {
		w.scope.tableDecls = make(map[string]parser.ITableExprContext)
	}
	w.scope.tableDecls[alias] = table
}

func (w *selectWalker) EnterTableIdentifier(ctx *parser.TableIdentifierContext) {
	if w.scope.currentSection != fromClause {
		return
	}

	var ref []string

	table := unquote(ctx.Identifier().GetText())
	if ctx.DatabaseIdentifier() == nil {
		ref = []string{table}
	} else {
		db := unquote(ctx.DatabaseIdentifier().GetText())
		ref = []string{db, table}
	}

	w.scope.tables = append(w.scope.tables, ref)

	alias, _ := w.scope.lookupAlias(ctx)
	if alias == "" {
		alias = table
	}

	// `select x from table` стает будто `select x from (select * from table)`
	sc := newScope(w.scope, alias)
	f := field{namepath: namepath{"*"}}
	sc.export = []columnExpr{{alias: "*", usedFields: []field{f}}}
	sc.fieldsSelect = []field{{namepath: namepath{"*"}}}
	sc.tables = []namepath{ref}
}

func (w *selectWalker) EnterTableFunctionExpr(ctx *parser.TableFunctionExprContext) {
	if ctx.TableArgList() == nil {
		return
	}

	w.stopPropagation = true

	tableName := namepath{}
	funcName := ctx.Identifier().GetText()

	switch funcName {
	case "cluster", "clusterAllReplicas":
		for i, el := range ctx.TableArgList().GetChildren() {
			if i == 0 {
				continue
			}
			if _, isTerm := el.(*antlr.TerminalNodeImpl); isTerm {
				continue
			}
			text := el.(antlr.ParseTree).GetText()
			text = strings.Trim(text, "`'")
			parts := strings.Split(text, ".")
			tableName = append(tableName, parts...)
		}
	case "numbers":
		tableName = []string{"system", "numbers"}
	case "null":
		return
	default:
		// todo: error: unsupported table function
		return
	}

	if len(tableName) == 0 {
		// todo: error
		return
	}

	alias, _ := w.scope.lookupAlias(ctx)
	if alias == "" {
		alias = tableName[len(tableName)-1]
	}

	sc := newScope(w.scope, alias)
	f := field{namepath: namepath{"*"}}
	sc.export = []columnExpr{{alias: "*", usedFields: []field{f}}}
	sc.fieldsSelect = []field{{namepath: namepath{"*"}}}
	sc.tables = []namepath{tableName}
}

func (w *selectWalker) EnterSelectStmt(ctx *parser.SelectStmtContext) {
	// внешний алиас этого селекта
	alias, _ := w.scope.lookupAlias(ctx)
	if alias == "" {
		alias = ctx.GetText()
	}

	if !w.isRootSelectHandled {
		w.scope.currentSection = selectClause
		w.isRootSelectHandled = true

		vars := collectAliasesFromSelect(ctx)
		w.scope.vars = vars
	} else {
		// handle subselect
		sub := newSelectWalker()
		walk(sub, ctx)

		sub.scope.parent = w.scope
		w.scope.children[alias] = sub.scope

		w.stopPropagation = true
	}
}
