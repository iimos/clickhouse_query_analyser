package columnusage

import "github.com/antlr/antlr4/runtime/Go/antlr"

// astListener is an AST listener for traversing through a syntax tree.
type astListener interface {
	antlr.ParseTreeListener
	PropagationStopped() bool
}

func walk(listener astListener, t antlr.Tree) {
	switch tt := t.(type) {
	case antlr.ErrorNode:
		listener.VisitErrorNode(tt)
	case antlr.TerminalNode:
		listener.VisitTerminal(tt)
	default:
		enterRule(listener, t.(antlr.RuleNode))
		if !listener.PropagationStopped() {
			for i := 0; i < t.GetChildCount(); i++ {
				child := t.GetChild(i)
				walk(listener, child)
			}
		}
		exitRule(listener, t.(antlr.RuleNode))
	}
}

func enterRule(listener antlr.ParseTreeListener, r antlr.RuleNode) {
	ctx := r.GetRuleContext().(antlr.ParserRuleContext)
	listener.EnterEveryRule(ctx)
	ctx.EnterRule(listener)
}

func exitRule(listener antlr.ParseTreeListener, r antlr.RuleNode) {
	ctx := r.GetRuleContext().(antlr.ParserRuleContext)
	ctx.ExitRule(listener)
	listener.ExitEveryRule(ctx)
}
