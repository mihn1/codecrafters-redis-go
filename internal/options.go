package internal

type DBOptions struct {
	ExpiryTime int    // default expiry time in milisecond
	Dir        string // default directory to store RDB files
	DbFilename string // default name of the RDB file
}
