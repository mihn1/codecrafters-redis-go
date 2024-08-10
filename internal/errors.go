package internal

type KeyNotFoundError struct{}

func (e *KeyNotFoundError) Error() string {
	return "Key not found"
}

type KeyExpiredError struct{}

func (e *KeyExpiredError) Error() string {
	return "Key expired"
}
