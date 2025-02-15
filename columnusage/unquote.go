package columnusage

import "strings"

// Кавычки в Кликхаусе экранируются двойными кавычками
var quoteReplacerBacktick = strings.NewReplacer("``", "`")
var quoteReplacerDoublequote = strings.NewReplacer("\"\"", "\"")

// unquote(`identifier with ""quotes""`) -> `identifier with "quotes"`
// unquote("`identifier in quotes`") -> "identifier in quotes"
func unquote(s string) string {
	if len(s) < 2 {
		return s
	}

	first := s[0]
	if first != '`' && first != '"' {
		return s
	}

	last := s[len(s)-1]
	if first != last {
		// shouldn't happen
		return s
	}

	s = s[1 : len(s)-1]

	switch first {
	case '`':
		s = quoteReplacerBacktick.Replace(s)
	case '"':
		s = quoteReplacerDoublequote.Replace(s)
	}
	return s
}
