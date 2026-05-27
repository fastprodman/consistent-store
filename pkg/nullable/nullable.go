// Package nullable provides a simple generic nullable value type.
package nullable

// Nullable represents either a value of type T or null.
type Nullable[T any] struct {
	value T
	valid bool
}

// New creates a valid Nullable containing value.
func New[T any](value T) Nullable[T] {
	return Nullable[T]{
		value: value,
		valid: true,
	}
}

// Null creates a null Nullable.
func Null[T any]() Nullable[T] {
	var zero T

	return Nullable[T]{
		value: zero,
		valid: false,
	}
}

// Valid reports whether the Nullable contains a value.
func (n Nullable[T]) Valid() bool {
	return n.valid
}

// IsNull reports whether the Nullable is null.
func (n Nullable[T]) IsNull() bool {
	return !n.valid
}

// V returns the contained value.
//
// If the Nullable is null, V returns the zero value of T.
func (n Nullable[T]) V() T {
	return n.value
}
