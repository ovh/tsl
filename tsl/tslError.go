package tsl

import (
	"fmt"
)

type (
	// Error structure
	Error struct {
		Message string `json:"error,omitempty"`
	}
)

// NewError create an instance of error using the error type
func NewError(err error) *Error {
	return &Error{
		Message: err.Error(),
	}
}

// NewTslError create an instance of error using the error type
func (p *Parser) NewTslError(message string, pos Pos) *Error {
	errorMessage := fmt.Sprintf("Cannot parsed query: %s at line %d, char %d", message, pos.Line+1-p.lineStart, pos.Char+1)
	return &Error{
		Message: errorMessage,
	}
}

// Error returns the string representation of the error.
func (e *Error) Error() string {
	return e.Message
}

// NewProtoError create an error related to the proto
func (p *ProtoParser) NewProtoError(message string, pos Pos) *Error {
	errorMessage := fmt.Sprintf("Cannot execute query on %s back-end: %s at line %d, char %d", p.name, message, pos.Line+1-p.lineStart, pos.Char+1)
	return &Error{
		Message: errorMessage,
	}
}
