package database

import "strings"

var (
	cmdMap = make(map[string]*command)
)

type command struct {
	executor ExecFunc
	prepare  PreFunc
	undo     UndoFunc
	arity    int
	flag     int
}

const (
	writeFlag    = 0
	readOnlyFlag = 1
)

func RegisterCommand(name string, executor ExecFunc, prepare PreFunc, undo UndoFunc, arity, flag int) {
	name = strings.ToLower(name)
	cmdMap[name] = &command{
		executor: executor,
		prepare:  prepare,
		undo:     undo,
		arity:    arity,
		flag:     flag,
	}
}

func checkReadOnlyCommand(name string) bool {
	name = strings.ToLower(name)
	cmd := cmdMap[name]
	if cmd == nil {
		return false
	}
	return cmd.flag&readOnlyFlag == 1
}
