package internal

type DB struct {
	Mapping map[string]string
}

func NewDB(options *DBOptions) *DB {
	return &DB{
		Mapping: make(map[string]string),
	}
}
