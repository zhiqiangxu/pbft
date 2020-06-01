package pbft

import (
	"context"
)

// FSM for business integration
type FSM interface {
	Exec(in []byte)
	StateHash() string
	PreExec(in []byte) (err error)
}

// PBFT defines system
type PBFT interface {
	SetFSM(FSM)
	SetConfig(Config)
	SetSocketProtocol(SocketProtocol)
	Start(context.Context) error
}

// MsgType for pbft
type MsgType uint32

// SocketProtocol for arbitrary socket protocol
type SocketProtocol interface {
	EstablishConnection(addr string, requestHandlers map[MsgType]RequestHandler, pushHandlers map[MsgType]PushHandler) (ClientConnection, error)
	NewListener(addr string, requestHandlers map[MsgType]RequestHandler, pushHandlers map[MsgType]PushHandler) (Listener, error)
}

// ClientConnection for client side
type ClientConnection interface {
	Connection
}

// Connection for qrpc
type Connection interface {
	Request(context.Context, MsgType, []byte) (MsgType, []byte, error)
	Push(context.Context, MsgType, []byte) error
	Close() error
}

// RequestHandler handles request
type RequestHandler func(Connection, []byte) (MsgType, []byte, error)

// PushHandler handles push
type PushHandler func(Connection, []byte) error

// Listener for server side
type Listener interface {
	Serve(context.Context) error
}
