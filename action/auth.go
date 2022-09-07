package action

import (
	"fmt"
	"strings"

	"github.com/No3371/go-skytable/protocol"
)

// https://docs.skytable.io/actions/auth#login
type AuthLogin struct {
	Username string
	Token    string
}

func (q AuthLogin) AppendToPacket(builder *strings.Builder) error {
	err := AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, 4, builder)
	if err != nil {
		return err
	}

	err = AppendElements(builder, false, "AUTH", "LOGIN", q.Username, q.Token)
	if err != nil {
		return err
	}
	return nil
}

func (q AuthLogin) ValidateProtocol(response interface{}) error {
	switch response := response.(type) {
	case protocol.ResponseCode:
		switch response {
		case protocol.RespOkay:
			return nil
		case protocol.RespBadCredentials:
			return nil
		case protocol.RespServerError:
			return nil
		default:
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("AUTH LOGIN: Unexpected response code: %s", response.String()), nil)
		}
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("AUTH LOGIN: Unexpected response element: %v", response), nil)
	}
}

// https://docs.skytable.io/actions/auth#logout
type AuthLogout struct{}

func (q AuthLogout) AppendToPacket(builder *strings.Builder) error {
	err := AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, 2, builder)
	if err != nil {
		return err
	}

	err = AppendElements(builder, false, "AUTH", "LOGOUT")
	if err != nil {
		return err
	}
	return nil
}

func (q AuthLogout) ValidateProtocol(response interface{}) error {
	switch response := response.(type) {
	case protocol.ResponseCode:
		switch response {
		case protocol.RespOkay:
			return nil
		case protocol.RespBadCredentials:
			return nil
		default:
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("AUTH LOGOUT: Unexpected response code: %s", response.String()), nil)
		}
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("AUTH LOGOUT: Unexpected response element: %v", response), nil)
	}
}

// https://docs.skytable.io/actions/auth#claim
type AuthClaim struct {
	OriginKey string
}

func (q AuthClaim) AppendToPacket(builder *strings.Builder) error {
	err := AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, 3, builder)
	if err != nil {
		return err
	}

	err = AppendElements(builder, false, "AUTH", "CLAIM", q.OriginKey)
	if err != nil {
		return err
	}
	return nil
}

func (q AuthClaim) ValidateProtocol(response interface{}) error {
	switch response := response.(type) {
	case string:
		return nil
	case protocol.ResponseCode:
		switch response {
		case protocol.RespBadCredentials:
			return nil
		default:
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("AUTH CLAIM: Unexpected response code: %s", response.String()), nil)
		}
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("AUTH CLAIM: Unexpected response element: %v", response), nil)
	}
}

// https://docs.skytable.io/actions/auth#adduser
type AuthAddUser struct {
	Username string
}

func (q AuthAddUser) AppendToPacket(builder *strings.Builder) error {
	err := AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, 3, builder)
	if err != nil {
		return err
	}

	err = AppendElements(builder, false, "AUTH", "ADDUSER", q.Username)
	if err != nil {
		return err
	}
	return nil
}

func (q AuthAddUser) ValidateProtocol(response interface{}) error {
	switch response := response.(type) {
	case string:
		return nil
	case protocol.ResponseCode:
		switch response {
		case protocol.RespAuthnRealmError:
			return nil
		default:
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("AUTH ADDUSER: Unexpected response code: %s", response.String()), nil)
		}
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("AUTH ADDUSER: Unexpected response element: %v", response), nil)
	}
}

// https://docs.skytable.io/actions/auth#deluser
type AuthDelUser struct {
	Username string
}

func (q AuthDelUser) AppendToPacket(builder *strings.Builder) error {
	err := AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, 3, builder)
	if err != nil {
		return err
	}

	err = AppendElements(builder, false, "AUTH", "DELUSER", q.Username)
	if err != nil {
		return err
	}
	return nil
}

func (q AuthDelUser) ValidateProtocol(response interface{}) error {
	switch response := response.(type) {
	case protocol.ResponseCode:
		switch response {
		case protocol.RespBadCredentials:
			return nil
		case protocol.RespAuthnRealmError:
			return nil
		default:
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("AUTH DELUSER: Unexpected response code: %s", response.String()), nil)
		}
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("AUTH DELUSER: Unexpected response element: %v", response), nil)
	}
}

// https://docs.skytable.io/actions/auth#restore
type AuthRestore struct {
	OriginKey string // If "", omitted in the sent command
	Username  string
}

func (q AuthRestore) AppendToPacket(builder *strings.Builder) error {
	if q.OriginKey == "" {
		err := AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, 3, builder)
		if err != nil {
			return err
		}

		err = AppendElements(builder, false, "AUTH", "RESTORE", q.Username)
		if err != nil {
			return err
		}
	} else {

		err := AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, 4, builder)
		if err != nil {
			return err
		}

		err = AppendElements(builder, false, "AUTH", "RESTORE", q.OriginKey, q.Username)
		if err != nil {
			return err
		}
	}

	return nil
}

func (q AuthRestore) ValidateProtocol(response interface{}) error {
	switch response := response.(type) {
	case string:
		return nil
	case protocol.ResponseCode:
		switch response {
		case protocol.RespBadCredentials:
			return nil
		case protocol.RespAuthnRealmError:
			return nil
		default:
			return protocol.NewUnexpectedProtocolError(fmt.Sprintf("AUTH RESTORE: Unexpected response code: %s", response.String()), nil)
		}
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("AUTH RESTORE: Unexpected response element: %v", response), nil)
	}
}

// https://docs.skytable.io/actions/auth#listuser
type AuthListUser struct{}

func (q AuthListUser) AppendToPacket(builder *strings.Builder) error {
	err := AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, 2, builder)
	if err != nil {
		return err
	}

	err = AppendElements(builder, false, "AUTH", "LISTUSER")
	if err != nil {
		return err
	}
	return nil
}

func (q AuthListUser) ValidateProtocol(response interface{}) error {
	switch response := response.(type) {
	case *protocol.TypedArray:
		return nil
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("AUTH LISTUSER: Unexpected response element: %v", response), nil)
	}
}

// https://docs.skytable.io/actions/auth#whoami
type AuthWhoAmI struct{}

func (q AuthWhoAmI) AppendToPacket(builder *strings.Builder) error {
	err := AppendArrayHeader(protocol.CompoundTypeAnyArray, 0, 2, builder)
	if err != nil {
		return err
	}

	err = AppendElements(builder, false, "AUTH", "LISTUSER")
	if err != nil {
		return err
	}
	return nil
}

func (q AuthWhoAmI) ValidateProtocol(response interface{}) error {
	switch response := response.(type) {
	case string:
		return nil
	default:
		return protocol.NewUnexpectedProtocolError(fmt.Sprintf("AUTH WHOAMI: Unexpected response element: %v", response), nil)
	}
}