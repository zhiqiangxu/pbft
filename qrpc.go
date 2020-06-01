package pbft

import (
	"context"

	"github.com/zhiqiangxu/qrpc"
	"github.com/zhiqiangxu/util/logger"
	"go.uber.org/zap"
)

type qrpcSocketProtocol struct {
	clientConfig qrpc.ConnectionConfig
	serverConfig qrpc.ServerBinding
}

// NewQRPCSocketProtocol is ctor for SocketProtocol
func NewQRPCSocketProtocol(clientConfig qrpc.ConnectionConfig, serverConfig qrpc.ServerBinding) SocketProtocol {
	return &qrpcSocketProtocol{clientConfig: clientConfig, serverConfig: serverConfig}
}

func (sp *qrpcSocketProtocol) EstablishConnection(addr string, requestHandlers map[MsgType]RequestHandler, pushHandlers map[MsgType]PushHandler) (cc ClientConnection, err error) {

	clientConn := &qrpcClientConnection{}
	clientConn.requestHandlers = requestHandlers
	clientConn.pushHandlers = pushHandlers
	conn, err := qrpc.NewConnection(addr, sp.clientConfig, clientConn.handlePush)
	if err != nil {
		return
	}

	clientConn.connection = conn

	cc = clientConn

	return
}

func (sp *qrpcSocketProtocol) NewListener(addr string, requestHandlers map[MsgType]RequestHandler, pushHandlers map[MsgType]PushHandler) (listener Listener, err error) {
	listener = &qrpcListener{addr: addr, requestHandlers: requestHandlers, pushHandlers: pushHandlers}
	return
}

type qrpcClientConnection struct {
	connection      *qrpc.Connection
	requestHandlers map[MsgType]RequestHandler
	pushHandlers    map[MsgType]PushHandler
}

func (clientConn *qrpcClientConnection) serveQRPC(writer qrpc.FrameWriter, frame *qrpc.RequestFrame) {
	ci := frame.ConnectionInfo()
	h := clientConn.requestHandlers[MsgType(frame.Cmd)]
	if h == nil {
		logger.Instance().Error("qrpcClientConnection invalid cmd", zap.Int32("cmd", int32(frame.Cmd)))
		frame.Close()
		return
	}

	outType, outBytes, err := h((*serverConnection)(ci), frame.Payload)
	if err != nil {
		logger.Instance().Error("qrpcClientConnection serveQRPC", zap.Error(err))
		frame.Close()
		return
	}

	write(writer, frame, outType, outBytes)
}

func (clientConn *qrpcClientConnection) handlePush(conn *qrpc.Connection, frame *qrpc.Frame) {
	h := clientConn.pushHandlers[MsgType(frame.Cmd)]
	if h == nil {
		return
	}
	err := h(clientConn, frame.Payload)
	if err != nil {
		logger.Instance().Error("qrpcClientConnection.handlePush", zap.Error(err))
		err = clientConn.connection.Close()
		if err != nil {
			logger.Instance().Error("qrpcClientConnection.handlePush Close", zap.Error(err))
		}
	}
}

func (clientConn *qrpcClientConnection) Close() (err error) {
	err = clientConn.connection.Close()
	if err != nil {
		logger.Instance().Error("qrpcClientConnection.Close", zap.Error(err))
	}
	return
}

func (clientConn *qrpcClientConnection) Request(ctx context.Context, inType MsgType, inBytes []byte) (outType MsgType, outBytes []byte, err error) {
	_, resp, err := clientConn.connection.Request(qrpc.Cmd(inType), 0, inBytes)
	if err != nil {
		return
	}

	frame, err := resp.GetFrameWithContext(ctx)
	if err != nil {
		return
	}

	outType, outBytes = MsgType(frame.Cmd), frame.Payload

	return
}

func (clientConn *qrpcClientConnection) Push(ctx context.Context, inType MsgType, inBytes []byte) (err error) {
	_, _, err = clientConn.connection.Request(qrpc.Cmd(inType), qrpc.PushFlag, inBytes)
	if err != nil {
		return
	}
	return
}

type qrpcListener struct {
	addr            string
	sp              *qrpcSocketProtocol
	requestHandlers map[MsgType]RequestHandler
	pushHandlers    map[MsgType]PushHandler
}

func (listener *qrpcListener) Serve(context.Context) (err error) {
	config := listener.sp.serverConfig
	config.Addr = listener.addr
	config.Handler = qrpc.HandlerFunc(listener.serveQRPC)
	config.SubFunc = listener.handleSub

	bindings := []qrpc.ServerBinding{config}
	server := qrpc.NewServer(bindings)
	err = server.ListenAndServe()

	return
}

type serverConnection qrpc.ConnectionInfo

func (serverConn *serverConnection) Request(ctx context.Context, inType MsgType, inBytes []byte) (outType MsgType, outBytes []byte, err error) {
	_, resp, err := (*qrpc.ConnectionInfo)(serverConn).Request(qrpc.Cmd(inType), 0, inBytes)
	if err != nil {
		return
	}

	frame, err := resp.GetFrameWithContext(ctx)
	if err != nil {
		return
	}

	outType, outBytes = MsgType(frame.Cmd), frame.Payload

	return
}

func (serverConn *serverConnection) Push(ctx context.Context, inType MsgType, inBytes []byte) (err error) {
	_, _, err = (*qrpc.ConnectionInfo)(serverConn).Request(qrpc.Cmd(inType), qrpc.PushFlag, inBytes)
	if err != nil {
		return
	}

	return
}

func (serverConn *serverConnection) Close() error {
	return (*qrpc.ConnectionInfo)(serverConn).Close()
}

func (listener *qrpcListener) serveQRPC(writer qrpc.FrameWriter, frame *qrpc.RequestFrame) {
	ci := frame.ConnectionInfo()
	h := listener.requestHandlers[MsgType(frame.Cmd)]
	if h == nil {
		logger.Instance().Error("qrpcListener invalid cmd", zap.Int32("cmd", int32(frame.Cmd)))
		frame.Close()
		return
	}

	outType, outBytes, err := h((*serverConnection)(ci), frame.Payload)
	if err != nil {
		logger.Instance().Error("qrpcListener serveQRPC", zap.Error(err))
		frame.Close()
		return
	}

	write(writer, frame, outType, outBytes)
}

func write(w qrpc.FrameWriter, frame *qrpc.RequestFrame, outType MsgType, outBytes []byte) {
	w.StartWrite(frame.RequestID, qrpc.Cmd(outType), 0)
	w.WriteBytes(outBytes)
	err := w.EndWrite()
	if err != nil {
		logger.Instance().Error("write", zap.Error(err))
	}
}

func (listener *qrpcListener) handleSub(ci *qrpc.ConnectionInfo, frame *qrpc.Frame) {
	h := listener.pushHandlers[MsgType(frame.Cmd)]
	if h == nil {
		return
	}

	err := h((*serverConnection)(ci), frame.Payload)
	if err != nil {
		logger.Instance().Error("qrpcListener.handlePush", zap.Error(err))
		err = ci.Close()
		if err != nil {
			logger.Instance().Error("qrpcListener.handlePush Close", zap.Error(err))
		}
	}
}
