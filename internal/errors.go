package internal

type KeyError interface {
	KeyErrorType() error
}

type KeyNotFoundError struct{}

func (e *KeyNotFoundError) Error() string {
	return "Key not found"
}

func (e *KeyNotFoundError) KeyErrorType() error {
	return e
}

type KeyExpiredError struct{}

func (e *KeyExpiredError) Error() string {
	return "Key expired"
}

func (e *KeyExpiredError) KeyErrorType() error {
	return e
}

type TypeMismatchError struct{}

func (e *TypeMismatchError) Error() string {
	return "Type mismatch"
}

type StreamKeyInvalid struct {
	message string
}

func (e *StreamKeyInvalid) Error() string {
	if e.message == "" {
		return "Stream key invalid"
	}
	return e.message
}

type StreamKeyTooSmall struct{}

func (e *StreamKeyTooSmall) Error() string {
	return "The ID specified in XADD is equal or smaller than the target stream top item"
}
