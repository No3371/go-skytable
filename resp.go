package skytable

import "github.com/No3371/go-skytable/response"

type RawResponsePacket struct {
	resps []response.ResponseEntry
}

type ResponsePacket struct {
	query *QueryPacket
	resps []response.ResponseEntry
}

func (rr ResponsePacket) Resps() []response.ResponseEntry {
	return rr.resps
}