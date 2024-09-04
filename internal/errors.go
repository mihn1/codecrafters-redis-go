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
