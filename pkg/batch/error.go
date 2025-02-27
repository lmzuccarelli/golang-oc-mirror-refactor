package batch

import (
	"fmt"
)

type SafeError struct {
	message string
}

type UnsafeError struct {
	errSchema mirrorErrorSchema
}

func NewSafeError(format string, a ...any) error {
	return SafeError{fmt.Sprintf(format, a...)}
}

func NewUnsafeError(mes mirrorErrorSchema) error {
	return UnsafeError{mes}
}

func (e SafeError) Error() string { return e.message }

func (e UnsafeError) Error() string { return e.errSchema.err.Error() }
