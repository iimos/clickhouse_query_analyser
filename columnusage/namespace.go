package columnusage

import (
	"fmt"
	"strings"
)

type namespace struct {
	list  []Expression
	index map[string][]Expression
	dedup map[string]bool
}

func newNamespace() *namespace {
	return &namespace{
		index: make(map[string][]Expression),
		dedup: make(map[string]bool),
	}
}

func (ns *namespace) add(scope string, expressions ...Expression) {
	for _, expr := range expressions {
		e := Expression{
			Name:    expr.Name,
			Scope:   scope,
			Columns: expr.Columns,
		}
		dedupKey := fmt.Sprintf("%v", e)
		if !ns.dedup[dedupKey] {
			ns.dedup[dedupKey] = true
			ns.list = append(ns.list, e)
			ns.index[e.Name] = append(ns.index[e.Name], e)
		}
	}
}

func (ns namespace) lookup(name []string) (Expression, bool) {
	if len(name) == 0 {
		return Expression{}, false
	}

	// простое односоставное имя
	if len(name) == 1 {
		col := name[0]

		if len(ns.index[col]) > 0 {
			return ns.index[col][0], true
		}
		return Expression{}, false
	}

	// select table.col, db.table.col, table.nest.subcol, db.table.nest.subcol, table.`nest.subcol`, ...
	for i := len(name) - 1; i >= 0; i-- {
		var db, tbl, col string

		switch i {
		case 0:
		case 1:
			tbl = name[0]
		case 2:
			db = name[0]
			tbl = name[1]
		default:
			continue
		}

		col = strings.Join(name[i:], ".")

		for _, expr := range ns.index[col] {
			if tbl == expr.Scope || tbl == "" {
				if db != "" {
					if len(expr.Columns) == 1 && expr.Columns[0].Database == db {
						return expr, true
					}
					continue
				}
				return expr, true
			}
		}
	}
	return Expression{}, false
}

func (ns namespace) search(parts []string) []Expression {
	if expr, ok := ns.lookup(parts); ok {
		return []Expression{expr}
	}

	count := len(parts)
	if count > 0 && parts[count-1] == "*" {
		tableParts := parts[:count-1]

		var db, tbl string
		switch len(tableParts) {
		case 0:
		case 1:
			tbl = parts[0]
		case 2:
			db = parts[0]
			tbl = parts[1]
		default:
			// todo: error?
			return nil
		}

		ret := make([]Expression, 0)
		for _, ex := range ns.list {

			dbMatch := false
			if db == "" {
				dbMatch = true
			} else {
				dbMatch = len(ex.Columns) == 1 && ex.Columns[0].Database == db
			}

			tableMatch := false
			if tbl == "" {
				tableMatch = true
			} else {
				tableMatch = ex.Scope == tbl
			}

			if tableMatch && dbMatch {
				ret = append(ret, Expression{
					Name:    ex.Name,
					Scope:   ex.Scope,
					Columns: ex.Columns,
				})
			}
		}
		return ret
	}

	return nil
}
