package pbft

import (
	"context"
	"fmt"
	"log"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zhiqiangxu/util"
)

type pbft struct {
	// accessed by atomic
	view     uint64
	n        uint64
	inflight int64
	status   Status

	fsm          FSM
	net          Net
	config       *Config
	account      Account
	accountIndex uint32
	msgC         chan Msg
	doneCh       chan struct{}
	wg           sync.WaitGroup
	msgPool      *msgPool
	msgSyncer    *msgSyncer
}

// Status of pbft
type Status uint32

const (
	// StatusInit ...
	StatusInit Status = iota
	// StatusStarting ...
	StatusStarting
	// StatusStarted ...
	StatusStarted
	// StatusStopping ...
	StatusStopping
	// StatusStopped ...
	StatusStopped
)

// New a PBFT
func New() PBFT {
	bft := &pbft{doneCh: make(chan struct{})}
	bft.msgPool = newMsgPool(bft)
	bft.msgSyncer = newMsgSyncer(bft)
	return bft
}

func (bft *pbft) SetFSM(fsm FSM) {
	bft.fsm = fsm
}

func (bft *pbft) GetFSM() FSM {
	return bft.fsm
}

func (bft *pbft) SetNet(net Net) {
	bft.net = net
}

func (bft *pbft) SetAccount(account Account) {
	bft.account = account
}

func (bft *pbft) SetConfig(config Config) {
	// TODO whether should allow call more than once?

	err := config.Validate()
	if err != nil {
		panic(fmt.Sprintf("invalid config:%v", config))
	}

	if config.TuningOptions == nil {
		config.TuningOptions = defaultTuningOptions
	}

	bft.config = &config
}

func (bft *pbft) GetNet() Net {
	return bft.net
}

func (bft *pbft) Start() (err error) {
	if bft.config == nil {
		err = fmt.Errorf("config empty")
		return
	}

	if bft.account == nil {
		err = fmt.Errorf("account empty")
		return
	}

	if bft.fsm == nil {
		err = fmt.Errorf("fsm empty")
		return
	}

	consensusConfig := bft.fsm.GetConsensusConfig()
	if consensusConfig == nil {
		consensusConfig = bft.config.ConsensusConfig
		bft.fsm.InitConsensusConfig(consensusConfig)
		bft.fsm.Commit()
	} else {
		bft.config.ConsensusConfig = consensusConfig
	}

	bft.view = consensusConfig.View
	bft.n = consensusConfig.N
	bft.accountIndex = bft.fsm.GetIndexByPubkey(bft.account.PublicKey())

	err = bft.setStatus(StatusStarting)
	if err != nil {
		return
	}

	if bft.net == nil {
		bft.net = defaultNet()
	}

	bft.msgC = make(chan Msg, bft.config.TuningOptions.MsgCSize)

	bft.net.SetPBFT(bft)

	util.GoFunc(&bft.wg, func() {
		err := bft.handleMsg()
		log.Println("handleMsg quit", err)
	})

	err = bft.setStatus(StatusStarted)
	return
}

func (bft *pbft) onUpdateConsensusPeers(peers []PeerInfo) {
	if bft.accountIndex == uint32(math.MaxUint32) {
		// get index by public key
		pubKey := bft.account.PublicKey()
		for _, peer := range peers {
			if pubKey == peer.Pubkey {
				bft.accountIndex = peer.Index
			}
		}
	}

	bft.net.OnUpdateConsensusPeers(peers)
}

func (bft *pbft) Stop() (err error) {
	err = bft.setStatus(StatusStopping)
	if err != nil {
		return
	}

	close(bft.doneCh)

	bft.wg.Wait()

	err = bft.setStatus(StatusStopped)
	return
}

func (bft *pbft) handleSyncResp(ctx context.Context, msg Msg) (handled bool, err error) {
	mt := msg.Type()
	if mt == MessageTypeSyncClientMessageResp || mt == MessageTypeSyncSealedClientMessageResp {
		handled = true

		err = bft.msgSyncer.onSyncResp(ctx, msg)
		return
	}

	return
}
func (bft *pbft) Send(ctx context.Context, msg Msg) (err error) {

	handled, err := bft.handleSyncResp(ctx, msg)
	if handled {
		return
	}

	select {
	case bft.msgC <- msg:
	case <-ctx.Done():
		err = ctx.Err()
	case <-bft.doneCh:
		err = fmt.Errorf("bft stopped")
	}

	return
}

func (bft *pbft) handleMsg() (err error) {
	for {
		select {
		case msg := <-bft.msgC:
			switch msg.Type() {
			case MessageTypeClient:
				err = bft.handleClientMsg(msg.(ClientMsg))
			case MessageTypePrepare:
				err = bft.handlePrepareMsg(msg.(*PrepareMsg))
			case MessageTypeCommit:
				err = bft.handleCommitMsg(msg.(*CommitMsg))
			case MessageTypeViewChange:
				err = bft.handleViewChangeMsg(msg.(*ViewChangeMsg))
			case MessageTypeNewView:
				err = bft.handleNewViewMsg(msg.(*NewViewMsg))
			case MessageTypeSyncClientMessageReq:
				err = bft.handleSyncClientMessageReq(msg.(*SyncClientMessageReq))
			case MessageTypeSyncSealedClientMessageReq:
				err = bft.handleSyncSealedClientMessageReq(msg.(*SyncSealedClientMessageReq))
			default:
				err = fmt.Errorf("unexpected msg type:%v", msg.Type())
			}
			if err != nil {
				log.Println("handleMsg err, msg", msg)
				err = nil
			}
		case <-bft.doneCh:
			err = fmt.Errorf("bft stopped")
			return
		}
	}
}

func (bft *pbft) isPrimary() bool {
	return bft.accountIndex == bft.config.Peers[atomic.LoadUint64(&bft.view)%uint64(len(bft.config.Peers))].Index
}

func (bft *pbft) primary() uint32 {
	return bft.primaryOfView(atomic.LoadUint64(&bft.view))
}

func (bft *pbft) primaryOfView(v uint64) uint32 {
	return bft.config.Peers[v%uint64(len(bft.config.Peers))].Index
}

func (bft *pbft) handleClientMsg(msg ClientMsg) (err error) {
	if !bft.isPrimary() {
		bft.net.SendTo(bft.primary(), msg)
		return
	}

	inflight := atomic.AddInt64(&bft.inflight, 1)
	if inflight > bft.config.TuningOptions.MaxInflightMsg {
		err = fmt.Errorf("max inflight message exceeded:%v", bft.config.TuningOptions.MaxInflightMsg)
		atomic.AddInt64(&bft.inflight, -1)
		return
	}

	n := bft.n + uint64(inflight) - 1

	ppp, err := bft.constructPrePreparePiggybackedMsg(bft.view, n, msg)
	if err != nil {
		return
	}

	bft.msgPool.AddPrepreparePiggybackedMsg(ppp)
	bft.net.Broadcast(ppp)

	return
}

func (bft *pbft) constructPrePreparePiggybackedMsg(v, n uint64, msg ClientMsg) (ppp *PrePreparePiggybackedMsg, err error) {

	ppp = &PrePreparePiggybackedMsg{ClientMsg: msg, PrePrepareMsg: PrePrepareMsg{Signature: Signature{PeerIndex: bft.accountIndex}, View: v, N: n, ClientMsgDigest: msg.Digest()}}

	digest := ppp.SignatureDigest()
	sig, err := bft.account.Sign(util.Slice(digest))
	ppp.Signature.Sig = sig
	return
}

func (bft *pbft) handlePrePreparePiggybackedMsg(msg *PrePreparePiggybackedMsg) (err error) {

	if msg.View == bft.view && msg.PeerIndex == bft.primary() {
		var p *PrepareMsg
		p, err = bft.constructPrepareMsg(&msg.PrePrepareMsg)
		if err != nil {
			return
		}
		bft.msgPool.AddPrepreparePiggybackedMsg(msg)
		bft.msgPool.AddPrepareMsg(p)
		bft.net.Broadcast(p)
	} else {
		err = fmt.Errorf("invalid PrePreparePiggybackedMsg(msg.View = %v, bft.view = %v, msg.PeerIndex = %v, bft.primary = %v)", msg.View, bft.view, msg.PeerIndex, bft.primary())
	}
	return
}

func (bft *pbft) constructPrepareMsg(msg *PrePrepareMsg) (p *PrepareMsg, err error) {
	p = &PrepareMsg{View: msg.View, N: msg.N, ClientMsgDigest: msg.ClientMsgDigest, Signature: Signature{PeerIndex: bft.accountIndex}}

	digest := p.SignatureDigest()
	sig, err := bft.account.Sign(util.Slice(digest))
	p.Signature.Sig = sig

	return
}

func (bft *pbft) handlePrepareMsg(msg *PrepareMsg) (err error) {
	if msg.View == bft.view {
		if added, prepared := bft.msgPool.AddPrepareMsg(msg); added && prepared {
			var c *CommitMsg
			c, err = bft.constructCommitMsg(msg)
			if err != nil {
				return
			}
			bft.msgPool.AddCommitMsg(c)
			bft.net.Broadcast(c)
		}
	} else {
		err = fmt.Errorf("invalid PrepareMsg(msg.View = %v, bft.view = %v)", msg.View, bft.view)
	}
	return
}

func (bft *pbft) constructCommitMsg(msg *PrepareMsg) (c *CommitMsg, err error) {
	c = &CommitMsg{View: msg.View, N: msg.N, ClientMsgDigest: msg.ClientMsgDigest, Signature: Signature{PeerIndex: bft.accountIndex}}

	digest := c.SignatureDigest()
	sig, err := bft.account.Sign(util.Slice(digest))
	c.Signature.Sig = sig

	return
}

func (bft *pbft) handleCommitMsg(msg *CommitMsg) (err error) {
	if msg.View == bft.view {
		if added, commitLocal := bft.msgPool.AddCommitMsg(msg); added && commitLocal {
			if msg.N > bft.n {
				for n := bft.n; n < msg.N; n++ {
					var resp *SyncSealedClientMessageResp
					for {
						resp, err = bft.msgSyncer.SyncSealedClientMsg(context.Background(), n)
						if err != nil {
							time.Sleep(time.Second)
							log.Println("SyncSealedClientMsg err", err)
							continue
						}

						bft.fsm.Exec(resp.ClientMsg)
						bft.fsm.AddClientMsgAndProof(resp.ClientMsg, resp.CommitMsgs)
						break
					}

				}

			}
			// persist to db
			clientMsg := bft.msgPool.GetClientMsg(msg.N)
			if clientMsg == nil {
				var resp *SyncClientMessageResp
				for {
					resp, err = bft.msgSyncer.SyncClientMsg(context.Background(), msg.N, msg.ClientMsgDigest)
					if err != nil {
						time.Sleep(time.Second)
						log.Println("SyncClientMsg err", err)
						continue
					}
					clientMsg = resp.ClientMsg
					break
				}
			}
			bft.fsm.Exec(clientMsg)
			bft.fsm.AddClientMsgAndProof(clientMsg, bft.msgPool.GetCommitMsgs(msg.N))
			bft.fsm.Commit()

			// update memory
			// TODO handle multiple inflight
			bft.msgPool.Sealed(msg.N)
			atomic.AddUint64(&bft.n, 1)
		}
	} else {
		err = fmt.Errorf("invalid CommitMsg(msg.View = %v, bft.view = %v)", msg.View, bft.view)
	}
	return
}

func (bft *pbft) handleViewChangeMsg(msg *ViewChangeMsg) (err error) {
	if msg.NewView > bft.view && bft.primaryOfView(msg.NewView) == bft.accountIndex {
		if added, enough := bft.msgPool.AddViewChangeMsg(msg); added && enough {
			var nv *NewViewMsg
			v, o := bft.msgPool.GetVO(msg.NewView)
			nv, err = bft.constructNewViewMsg(msg.NewView, v, o)
			if err != nil {
				return
			}
			bft.msgPool.AddNewViewMsg(nv)
			bft.net.Broadcast(nv)

			atomic.StoreUint64(&bft.view, msg.NewView)
		}
	} else {
		log.Printf("ViewChangeMsg dropped for not being primary(%d) of specified view(%d), accountIndex(%d)\n", bft.primaryOfView(msg.NewView), msg.NewView, bft.accountIndex)
	}
	return
}

func (bft *pbft) constructNewViewMsg(newView uint64, v []*ViewChangeMsg, o []*PrePrepareMsg) (msg *NewViewMsg, err error) {
	msg = &NewViewMsg{NewView: newView, V: v, O: o, Signature: Signature{PeerIndex: bft.accountIndex}}
	digest := msg.SignatureDigest()
	sig, err := bft.account.Sign(util.Slice(digest))
	msg.Signature.Sig = sig
	return
}

func (bft *pbft) constructSyncClientMsgReq(n uint64, clientMsgDigest string) (msg *SyncClientMessageReq, err error) {
	msg = &SyncClientMessageReq{N: n, ClientMsgDigest: clientMsgDigest, Signature: Signature{PeerIndex: bft.accountIndex}}

	digest := msg.SignatureDigest()
	sig, err := bft.account.Sign(util.Slice(digest))
	msg.Signature.Sig = sig

	return
}

func (bft *pbft) constructSyncSealedClientMsgReq(n uint64) (msg *SyncSealedClientMessageReq, err error) {
	msg = &SyncSealedClientMessageReq{N: n, Signature: Signature{PeerIndex: bft.accountIndex}}

	digest := msg.SignatureDigest()
	sig, err := bft.account.Sign(util.Slice(digest))
	msg.Signature.Sig = sig

	return
}

func (bft *pbft) handleNewViewMsg(msg *NewViewMsg) (err error) {
	if msg.NewView > bft.view && msg.PeerIndex == bft.primaryOfView(msg.NewView) {

		for _, pp := range msg.O {
			var p *PrepareMsg
			p, err = bft.constructPrepareMsg(pp)
			if err != nil {
				return
			}
			bft.msgPool.AddPrePrepareMsg(pp)
			bft.msgPool.AddPrepareMsg(p)
			bft.net.Broadcast(p)
		}
		bft.fsm.UpdateV(msg.NewView)
		bft.fsm.Commit()

		atomic.StoreUint64(&bft.view, msg.NewView)

	} else {
		err = fmt.Errorf("invalid NewViewMsg(msg.NewView = %v, bft.view = %v, msg.PeerIndex = %v, bft.primaryOfView(msg.NewView) = %v)", msg.NewView, bft.view, msg.PeerIndex, bft.primaryOfView(msg.NewView))
	}

	return
}

func (bft *pbft) handleSyncClientMessageReq(msg *SyncClientMessageReq) (err error) {
	var clientMsg ClientMsg
	if bft.n <= msg.N {
		clientMsg = bft.msgPool.GetClientMsg(msg.N)
	} else {
		clientMsg = bft.fsm.GetClientMsg(msg.N)
	}

	if clientMsg != nil && clientMsg.Digest() == msg.ClientMsgDigest {
		bft.net.SendTo(msg.PeerIndex, &SyncClientMessageResp{ReqID: msg.ReqID, ClientMsg: clientMsg})
	}
	return
}

func (bft *pbft) handleSyncSealedClientMessageReq(msg *SyncSealedClientMessageReq) (err error) {
	clientMsg, commitMsgs := bft.fsm.GetClientMsgAndProof(msg.N)
	if clientMsg == nil {
		return
	}

	bft.net.SendTo(msg.PeerIndex, &SyncSealedClientMessageResp{ReqID: msg.ReqID, ClientMsg: clientMsg, CommitMsgs: commitMsgs})
	return
}

func (bft *pbft) setStatus(status Status) (err error) {
	switch status {
	case StatusStarting:
		swapped := atomic.CompareAndSwapUint32((*uint32)(&bft.status), uint32(StatusInit), uint32(StatusStarting))
		if !swapped {
			err = fmt.Errorf("invalid status change: %v -> %v", atomic.LoadUint32((*uint32)(&bft.status)), uint32(StatusStarting))
			return
		}
	case StatusStarted:
		swapped := atomic.CompareAndSwapUint32((*uint32)(&bft.status), uint32(StatusStarting), uint32(StatusStarted))
		if !swapped {
			err = fmt.Errorf("invalid status change: %v -> %v", atomic.LoadUint32((*uint32)(&bft.status)), StatusStarted)
			return
		}
	case StatusStopping:
		swapped := atomic.CompareAndSwapUint32((*uint32)(&bft.status), uint32(StatusStarted), uint32(StatusStopping))
		if !swapped {
			err = fmt.Errorf("invalid status change: %v -> %v", atomic.LoadUint32((*uint32)(&bft.status)), uint32(StatusStopping))
			return
		}
	case StatusStopped:
		swapped := atomic.CompareAndSwapUint32((*uint32)(&bft.status), uint32(StatusStopping), uint32(StatusStopped))
		if !swapped {
			err = fmt.Errorf("invalid status change: %v -> %v", atomic.LoadUint32((*uint32)(&bft.status)), StatusStopped)
			return
		}
	}

	return
}
