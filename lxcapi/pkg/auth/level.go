package auth

type APILevel int

const (
	LevelSystem    APILevel = iota
	LevelAdmin
	LevelContainer
)
