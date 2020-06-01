package pbft

import (
	"context"

	"github.com/zhiqiangxu/mondis"

	"github.com/zhiqiangxu/mondis/kv"
	"github.com/zhiqiangxu/mondis/provider"
	"github.com/zhiqiangxu/mondis/structure"
)

type pbft struct {
	fsm    FSM
	config Config
	sp     SocketProtocol
	kvdb   mondis.KVDB
	view   int64
}

// NewPBFT is ctor for PBFT
func NewPBFT() PBFT {
	return &pbft{}
}

func (bft *pbft) SetFSM(fsm FSM) {
	bft.fsm = fsm
}

func (bft *pbft) SetConfig(config Config) {
	bft.config = config
}

func (bft *pbft) SetSocketProtocol(sp SocketProtocol) {
	bft.sp = sp
}

func (bft *pbft) Start(ctx context.Context) (err error) {
	err = bft.bootstrap(ctx)
	if err != nil {
		return
	}

	err = bft.connectPeers(ctx)
	if err != nil {
		return
	}

	err = bft.listenAndServe(ctx)
	return
}

func (bft *pbft) bootstrap(ctx context.Context) (err error) {
	// open database
	kvdb := provider.NewBadger()
	err = kvdb.Open(mondis.KVOption{Dir: bft.config.DBConf.DataDir})
	if err != nil {
		return
	}
	bft.kvdb = kvdb

	// retrieve view from database
	view, err := bft.dbView()
	if err == kv.ErrKeyNotFound {
		// if not exists, retrieve from config
		err = nil
		view = bft.config.NodeConfig.View
	}
	if err != nil {
		return
	}
	bft.view = view

	return
}

func (bft *pbft) connectPeers(ctx context.Context) (err error) {
	nodeConfig := bft.config.NodeConfig
	nodeCount := len(nodeConfig.Nodes)
	for i := 0; i < nodeCount; i++ {
		if i == nodeConfig.Index {
			continue
		}

		// if strings.Compare(nodeConfig.Nodes[nodeConfig.Index].Addr, nodeConfig.Nodes[i].Addr) > 0 {
		// 	t.outConns[i] = qrpc.NewConnectionWithReconnect([]string{peerInfo.Addr}, t.connectionConfig, nil)
		// }
	}
	return
}

func (bft *pbft) listenAndServe(ctx context.Context) (err error) {
	return
}

var (
	structPrefix = []byte{0x1}
	keyForView   = []byte{0x1}
)

func (bft *pbft) dbView() (view int64, err error) {
	txn := bft.kvdb.NewTransaction(false)
	defer txn.Discard()
	txStruct := structure.New(txn, structPrefix)
	view, err = txStruct.GetInt64(keyForView)
	return
}
