package columnusage

import (
	"errors"
	"strconv"

	"github.com/antlr/antlr4/runtime/Go/antlr"
)

type errorListener struct {
	*antlr.DefaultErrorListener
	errors []error
}

func (el *errorListener) SyntaxError(recognizer antlr.Recognizer, offendingSymbol interface{}, line, column int, msg string, e antlr.RecognitionException) {
	err := errors.New("line " + strconv.Itoa(line) + ":" + strconv.Itoa(column) + " " + msg)
	el.errors = append(el.errors, err)
}
