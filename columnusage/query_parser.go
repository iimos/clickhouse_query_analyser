package columnusage

import (
	"fmt"
	"sync"

	"github.com/antlr/antlr4/runtime/Go/antlr"
	parser "github.com/iimos/clickhouse_query_analyser/internal/clickhouse_parser"
)

var debug = false

type clauseKind = string

const (
	withClause     = clauseKind("with")
	selectClause   = clauseKind("select")
	fromClause     = clauseKind("from")
	prewhereClause = clauseKind("prewhere")
	whereClause    = clauseKind("where")
	groupByClause  = clauseKind("group by")
	havingClause   = clauseKind("having")
	orderByClause  = clauseKind("order by")
	limitByClause  = clauseKind("limit by")
)

type namepath []string

type fieldContext struct {
	comparisonOp   string
	comparisonWith string
}
type field struct {
	namepath namepath
	context  fieldContext
}

type scope struct {
	parent         *scope
	children       map[string]*scope
	externalName   string
	currentSection clauseKind

	export []columnExpr // variable names exported from the scope
	tables []namepath
	vars   []columnExpr // named expressions

	fieldsWith     []field
	fieldsSelect   []field
	fieldsFrom     []field
	fieldsPrewhere []field
	fieldsWhere    []field
	fieldsGroupBy  []field
	fieldsHaving   []field
	fieldsOrderBy  []field
	fieldsLimitBy  []field

	// Мапы для таблиц и колонок разделены поскольку допустимо когда таблица
	// и колонка имеют одинаковый алиас и в кликхаусе это ок.
	// Пример `select xxx.id as xxx, xxx.id from table as xxx`
	tableDecls map[string]parser.ITableExprContext
}

func (s *scope) currentSectionFields() *[]field {
	switch s.currentSection {
	case withClause:
		return &s.fieldsWith
	case selectClause:
		return &s.fieldsSelect
	case fromClause:
		return &s.fieldsFrom
	case whereClause:
		return &s.fieldsWhere
	case prewhereClause:
		return &s.fieldsPrewhere
	case groupByClause:
		return &s.fieldsGroupBy
	case havingClause:
		return &s.fieldsHaving
	case orderByClause:
		return &s.fieldsOrderBy
	case limitByClause:
		return &s.fieldsLimitBy
	}
	panic(fmt.Sprintf("unknown section: %q", s.currentSection))
}

func (s *scope) forEachSection(fn func(section string, fields []field)) {
	fn(withClause, s.fieldsWith)
	fn(selectClause, s.fieldsSelect)
	fn(fromClause, s.fieldsFrom)
	fn(prewhereClause, s.fieldsPrewhere)
	fn(whereClause, s.fieldsWhere)
	fn(groupByClause, s.fieldsGroupBy)
	fn(havingClause, s.fieldsHaving)
	fn(orderByClause, s.fieldsOrderBy)
	fn(limitByClause, s.fieldsLimitBy)
}

func (s *scope) lookupAlias(ctx antlr.Tree) (alias string, aliasCtx antlr.ParserRuleContext) {
	up := ctx
	for up != nil {
		up = up.GetParent()
		for name, tableExpr := range s.tableDecls {
			if up == tableExpr {
				return name, tableExpr
			}
		}
	}
	return "", nil
}

func newScope(parent *scope, externalName string) *scope {
	s := &scope{
		externalName: externalName,
		parent:       parent,
		children:     make(map[string]*scope),
		tableDecls:   make(map[string]parser.ITableExprContext),
	}
	if parent != nil {
		if _, collision := parent.children[externalName]; collision {
			// todo error
			panic("scope alias collision, alias='" + externalName + "'")
		}
		parent.children[externalName] = s
	}
	return s
}

// Analyser is an SQL parser.
type Analyser struct {
	currentDatabase string
	schema          map[string]map[string][]Column
	root            *scope
}

// NewColumnUsageAnalyser creates new parser
func NewColumnUsageAnalyser(schema []Column) *Analyser {
	a := &Analyser{
		root: newScope(nil, ""),
	}
	a.setSchema(schema)
	return a
}

func (a *Analyser) reset() {
	a.root = newScope(nil, "")
}

func (a *Analyser) setSchema(columns []Column) {
	schema := make(map[string]map[string][]Column)
	for _, c := range columns {
		if _, ok := schema[c.Database]; !ok {
			schema[c.Database] = make(map[string][]Column)
		}
		schema[c.Database][c.Table] = append(schema[c.Database][c.Table], c)
	}
	a.schema = schema
}

var lexerPool = sync.Pool{
	New: func() interface{} {
		return parser.NewClickHouseLexer(nil)
	},
}

var parserPool = sync.Pool{
	New: func() interface{} {
		return parser.NewClickHouseParser(nil)
	},
}

// ParseQuery parses sql query and returns information about columns usage, etc.
func (a *Analyser) ParseQuery(currentDatabase, sql string) (Stat, error) {
	defer a.reset()

	a.currentDatabase = currentDatabase

	inputStream := antlr.NewInputStream(sql)

	lexer := lexerPool.Get().(*parser.ClickHouseLexer)
	defer lexerPool.Put(lexer)

	lexer.SetInputStream(inputStream)

	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	errListener := &errorListener{}

	chp := parserPool.Get().(*parser.ClickHouseParser)
	defer parserPool.Put(chp)

	chp.SetInputStream(stream)
	chp.RemoveErrorListeners()
	chp.AddErrorListener(errListener)

	sw := newSelectWalker()
	walk(sw, chp.QueryStmt())

	if len(errListener.errors) > 0 {
		// todo: make error texts
		return Stat{}, errListener.errors[0]
	}

	result, err := a.scanColumnsInScope(sw.scope)
	if err != nil {
		return Stat{}, err
	}

	result = prepareOutput(result)
	return result, nil
}

func prepareOutput(st Stat) Stat {
	colsInExport := make(map[Column]bool)
	for _, c := range st.getColumnsUsedInExport() {
		colsInExport[c] = true
	}

	for key, ex := range st.Export {
		ex.Scope = ""
		st.Export[key] = ex
	}

	usages := make([]ColumnUsage, 0, len(st.ColumnUsages))
	dedupMap := make(map[ColumnUsage]int)
	for _, usage := range st.ColumnUsages {
		if usage.Implicit && !colsInExport[usage.Column] {
			// Если колонка использована неявным образом и при этом отсутствует в экспорте,
			// то отфильтровываем такое использование
			continue
		}

		dedupKey := usage
		dedupKey.Implicit = false

		index, exists := dedupMap[dedupKey]
		if exists {
			// explicit usage beats implicit one
			if !usage.Implicit {
				usages[index] = usage
			}
		} else {
			usages = append(usages, usage)
			dedupMap[dedupKey] = len(usages) - 1
		}
	}
	st.ColumnUsages = usages
	return st
}

func (a *Analyser) resolveTables(scope *scope) (columns []Column) {
	ret := make([]Column, 0)

	for _, parts := range scope.tables {
		if len(parts) == 0 {
			panic("got empty table alias") // todo
		}

		// алиас таблыцы или подселекта
		if len(parts) == 1 {
			name := parts[0]
			_, isAlias := scope.tableDecls[name]
			if isAlias {
				// пока пропускаем
				// todo
				continue
			}
		}

		var db, table string

		switch len(parts) {
		case 1:
			// таблица без указания бд
			db = a.currentDatabase
			table = parts[0]
		case 2:
			// таблица c указанием бд
			db = parts[0]
			table = parts[1]
		default:
			// todo: error
			panic(fmt.Sprintf("found Column alias consisted of more than 2 parts: %v", parts))
		}

		cols, ok := a.schema[db][table]
		if !ok {
			// todo error: unknown table
			continue
		}

		ret = append(ret, cols...)
	}
	return ret
}

type Column struct {
	Database, Table, Name string
}

func (c Column) String() string {
	return "Column(" + c.Database + "." + c.Table + "." + c.Name + ")"
}

type Expression struct {
	Name    string
	Scope   string
	Columns []Column
}
type ColumnUsage struct {
	Column         Column
	Purpose        string
	Implicit       bool
	ComparisonOp   string
	ComparisonWith string
}

// Stat contains statistics about sql query.
type Stat struct {
	// Export is an output expressions of sql query.
	Export []Expression
	// ColumnUsages is a list of columns used in sql query.
	ColumnUsages []ColumnUsage
}

func (st Stat) getColumnsUsedInExport() []Column {
	cols := make([]Column, 0)
	dedupMap := make(map[Column]bool)
	for _, e := range st.Export {
		for _, c := range e.Columns {
			if !dedupMap[c] {
				cols = append(cols, c)
				dedupMap[c] = true
			}
		}
	}
	return cols
}

func (a *Analyser) scanColumnsInScope(scope *scope) (Stat, error) {
	st := Stat{
		Export:       make([]Expression, 0),
		ColumnUsages: make([]ColumnUsage, 0),
	}

	ns := newNamespace()

	for _, c := range a.resolveTables(scope) {
		ns.add(c.Table, Expression{
			Name:    c.Name,
			Scope:   c.Table,
			Columns: []Column{c},
		})
	}

	for scopeName, ch := range scope.children {
		chst, err := a.scanColumnsInScope(ch)
		if err != nil {
			return Stat{}, err
		}
		ns.add(scopeName, chst.Export...)
		st.ColumnUsages = append(st.ColumnUsages, chst.ColumnUsages...)
	}

	// собираем алиасы

	toResolve := scope.vars

	for i := 0; len(toResolve) > 0 && i < 100; i++ {
		unresolved := make([]columnExpr, 0)

		for _, expr := range toResolve {
			if expr.alias == "" || expr.alias == "*" {
				continue
			}

			exprs, ok := a.resolveColumnExpr(ns, scope, expr)
			if ok {
				ns.add("", exprs...)
			} else {
				unresolved = append(unresolved, expr)
			}
		}
		toResolve = unresolved
	}

	if len(toResolve) > 0 {
		return Stat{}, fmt.Errorf("query_parser: can't resolve expressions: %v", toResolve)
	}

	for _, expr := range scope.export {
		exprs, ok := a.resolveColumnExpr(ns, scope, expr)
		if !ok {
			return Stat{}, fmt.Errorf("query_parser: can't resolve expression: %v", expr)
		}
		st.Export = append(st.Export, exprs...)
	}

	scope.forEachSection(func(section string, fields []field) {
		for _, f := range fields {
			// простое односоставное имя
			if len(f.namepath) == 1 {
				col := f.namepath[0]
				if _, isAlias := scope.children[columnSubscopeKey(col)]; isAlias {
					// алиасы просто пропускаем
					continue
				}
			}

			hasAsterisk := len(f.namepath) > 0 && f.namepath[len(f.namepath)-1] == "*"
			results := ns.search(f.namepath)

			for _, ex := range results {
				for _, c := range ex.Columns {
					usg := ColumnUsage{
						Column:   c,
						Purpose:  section,
						Implicit: hasAsterisk,
					}
					if len(ex.Columns) == 1 && len(results) == 1 {
						// todo: it's buggy approach to detect simple variable
						usg.ComparisonOp = f.context.comparisonOp
						usg.ComparisonWith = f.context.comparisonWith
					}
					st.ColumnUsages = append(st.ColumnUsages, usg)
				}
			}

			// todo: error if not found
		}
	})

	return st, nil
}

func (a *Analyser) resolveColumnExpr(ns *namespace, scope *scope, expr columnExpr) (exprs []Expression, ok bool) {
	name := expr.alias
	if name == "" {
		// простое односоставное имя
		if len(expr.namepath) == 1 {
			col := expr.namepath[0]
			if subscope, isAlias := scope.children[columnSubscopeKey(col)]; isAlias {
				name = col
				expr.namepath = nil
				expr.usedFields = nil
				expr.subselect = subscope
			}
		}
		if res, found := ns.lookup(expr.namepath); found {
			name = res.Name
		}
	}

	if name == "*" {
		ret := make([]Expression, 0)
		for _, f := range expr.usedFields {
			list := ns.search(f.namepath)
			ret = append(ret, list...)
		}
		return ret, true
	}

	if name == "" {
		return nil, false
	}

	e := Expression{Name: name}
	for _, f := range expr.usedFields {
		if res, found := ns.lookup(f.namepath); found {
			e.Columns = append(e.Columns, res.Columns...)
		} else {
			if !expr.isPrivateVariable(f.namepath) {
				if r, ok := ns.lookup([]string{name}); ok {
					return []Expression{r}, true
				}
				return nil, false
			}
		}
	}
	if expr.subselect != nil {
		s, err := a.scanColumnsInScope(expr.subselect)
		if err != nil {
			return nil, false
		}
		for _, usage := range s.ColumnUsages {
			if !usage.Implicit {
				e.Columns = append(e.Columns, usage.Column)
			}
		}
	}

	e = dedupColumns(e)
	return []Expression{e}, true
}

func dedupColumns(expr Expression) Expression {
	ret := expr
	ret.Columns = nil

	dedup := make(map[Column]bool)
	for _, c := range expr.Columns {
		if !dedup[c] {
			dedup[c] = true
			ret.Columns = append(ret.Columns, c)
		}
	}
	return ret
}
