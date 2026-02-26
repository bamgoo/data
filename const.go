package data

import "errors"

const NAME = "DATA"

const (
	OpAnd       = "$and"
	OpOr        = "$or"
	OpNor       = "$nor"
	OpNot       = "$not"
	OpEq        = "$eq"
	OpNe        = "$ne"
	OpGt        = "$gt"
	OpGte       = "$gte"
	OpLt        = "$lt"
	OpLte       = "$lte"
	OpIn        = "$in"
	OpNin       = "$nin"
	OpLike      = "$like"
	OpILike     = "$ilike"
	OpRegex     = "$regex"
	OpContains  = "$contains"
	OpOverlap   = "$overlap"
	OpElemMatch = "$elemMatch"
	OpExists    = "$exists"
	OpNull      = "$null"
)

const (
	OptSelect    = "$select"
	OptSort      = "$sort"
	OptLimit     = "$limit"
	OptOffset    = "$offset"
	OptAfter     = "$after"
	OptWithCount = "$withCount"
	OptGroup     = "$group"
	OptHaving    = "$having"
	OptJoin      = "$join"
	OptAgg       = "$agg"
	OptUnsafe    = "$unsafe"
	OptBatch     = "$batch"
)

const (
	UpdSet       = "$set"
	UpdInc       = "$inc"
	UpdUnset     = "$unset"
	UpdPush      = "$push"
	UpdPull      = "$pull"
	UpdAddToSet  = "$addToSet"
	UpdSetPath   = "$setPath"
	UpdUnsetPath = "$unsetPath"
)

var (
	errInvalidConnection = errors.New("invalid data connection")
	errInvalidDriver     = errors.New("invalid data driver")
)
