package internal

type DBOptions struct {
	ExpiryTime int64  // default expiry time in milisecond
	Dir        string // default directory to store RDB files
	DbFilename string // default name of the RDB file
}
