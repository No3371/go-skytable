package skytable

import "context"

type QueryPacket struct {
	ctx     context.Context
	actions []Action
}

func NewQueryPacket(actions []Action) *QueryPacket {
	return &QueryPacket{
		actions: actions,
	}
}