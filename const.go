package data

import "errors"

const NAME = "DATA"

var (
	errInvalidConnection = errors.New("invalid data connection")
	errInvalidDriver     = errors.New("invalid data driver")
)
