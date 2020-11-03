package pbft

import (
	"context"
	"fmt"
	"log"
	"math"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/petermattis/goid"
	"github.com/zhiqiangxu/util"
)

type pbft struct {
	config       *Config
	fsm          FSM
	net          Net
	account      Account
	accountIndex uint32
	msgC         chan Msg
	doneCh       chan struct{}
	wg           sync.WaitGroup
	msgPool      *msgPool
	msgSyncer    *msgSyncer
}

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

func (bft *pbft) GetNet() Net {
	return bft.net
}

func (bft *pbft) SetAccount(account Account) {
	bft.account = account
}

func (bft *pbft) SetConfig(config *Config) {
	// TODO whether should allow call more than once?

	err := config.Validate()
	if err != nil {
		panic(fmt.Sprintf("invalid config:%v", err))
	}

	if config.TuningOptions == nil {
		config.TuningOptions = defaultTuningOptions
	}

	bft.config = config
}

func (bft *pbft) GetConfig() *Config {
	return bft.config
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

	// InitConsensusConfig for the first time
	initConsensusConfig := bft.fsm.GetInitConsensusConfig()
	if initConsensusConfig == nil {
		initConsensusConfig = bft.config.InitConsensusConfig
		err = initConsensusConfig.Validate()
		if err != nil {
			return
		}
		if initConsensusConfig.CheckpointInterval == 0 {
			initConsensusConfig.CheckpointInterval = defaultCheckpointInterval
		}
		if initConsensusConfig.HighWaterMark == 0 {
			initConsensusConfig.HighWaterMark = defaultHighWaterMark
		}
		bft.fsm.InitConsensusConfig(initConsensusConfig)
		bft.fsm.Commit()
	} else {
		// load from db
		bft.config.InitConsensusConfig = initConsensusConfig
	}

	bft.loadConsensusConfig()

	if bft.net == nil {
		bft.net = defaultNet()
	}

	bft.net.OnUpdateConsensusPeers(bft.fsm.GetHistoryPeers())

	bft.accountIndex = bft.fsm.GetIndexByPubkey(bft.account.PublicKey())

	bft.msgC = make(chan Msg, bft.config.TuningOptions.MsgCSize)

	util.GoFunc(&bft.wg, func() {
		err := bft.handleMsg()
		log.Println("handleMsg quit", err)
	})

	return
}

func (bft *pbft) loadConsensusConfig() {
	config := bft.fsm.GetConsensusConfig()
	if config == nil {
		log.Fatal("GetConsensusConfig returns nil")
	}
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&bft.config.consensusConfig)), unsafe.Pointer(config))
}

func (bft *pbft) getConsensusConfig() *ConsensusConfig {
	return (*ConsensusConfig)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&bft.config.consensusConfig))))
}

func (bft *pbft) Stop() (err error) {

	close(bft.doneCh)

	bft.wg.Wait()

	return
}

const (
	// NonConsensusIndex is index for public keys never ever in consensus
	NonConsensusIndex = uint32(math.MaxUint32)
)

func (bft *pbft) onUpdateConsensusPeers(fromPeers, toPeers []PeerInfo) {
	if bft.accountIndex == NonConsensusIndex {
		// get index by public key
		pubKey := bft.account.PublicKey()
		for _, peer := range toPeers {
			if pubKey == peer.Pubkey {
				bft.accountIndex = peer.Index
			}
		}
	}

	bft.net.OnUpdateConsensusPeers(toPeers)

	bft.config.consensusConfig.Peers = toPeers
}

func (bft *pbft) onUpdateView(view uint64) {
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
			log.Println(goid.Get(), "got msg", msg.Type())
			switch msg.Type() {
			case MessageTypeClient:
				err = bft.handleClientMsg(msg.(ClientMsg))
			case MessageTypePrePreparePiggybacked:
				err = bft.handlePrePreparePiggybackedMsg(msg.(*PrePreparePiggybackedMsg))
			case MessageTypePrepare:
				err = bft.handlePrepareMsg(msg.(*PrepareMsg))
			case MessageTypeCommit:
				err = bft.handleCommitMsg(msg.(*CommitMsg))
			case MessageTypeViewChange:
				err = bft.handleViewChangeMsg(msg.(*ViewChangeMsg))
			case MessageTypeNewView:
				err = bft.handleNewViewMsg(msg.(*NewViewMsg))
			case MessageTypeCheckpoint:
				err = bft.handleCheckpointMsg(msg.(*CheckpointMsg))
			case MessageTypeSyncClientMessageReq:
				err = bft.handleSyncClientMessageReq(msg.(*SyncClientMessageReq))
			case MessageTypeSyncSealedClientMessageReq:
				err = bft.handleSyncSealedClientMessageReq(msg.(*SyncSealedClientMessageReq))
			default:
				err = fmt.Errorf("unexpected msg type:%v", msg.Type())
			}
			if err != nil {
				log.Println("handleMsg err, msg", msg, "err", err)
				err = nil
			}
		case <-bft.doneCh:
			err = fmt.Errorf("bft stopped")
			return
		}
	}
}

func (bft *pbft) validateClientMsg(msg ClientMsg) (err error) {
	digest := msg.Digest()
	cm := bft.msgPool.GetClientMsgByDigest(digest)
	if cm != nil {
		err = fmt.Errorf("duplicate client msg:%v", digest)
		return
	}

	cm = bft.fsm.GetClientMsgByDigest(digest)
	if cm != nil {
		err = fmt.Errorf("duplicate client msg:%v", digest)
		return
	}

	return
}

func (bft *pbft) highestNAllowed() (highestN uint64) {
	consensusConfig := bft.getConsensusConfig()
	highestN = consensusConfig.NextCheckpoint + consensusConfig.HighWaterMark
	return
}

func (bft *pbft) handleClientMsg(msg ClientMsg) (err error) {

	err = bft.validateClientMsg(msg)
	if err != nil {
		return
	}

	consensusConfig := bft.getConsensusConfig()
	primary := consensusConfig.Primary()
	if bft.accountIndex != primary {
		bft.net.SendTo(primary, msg)
		return
	}

	var nextHighest uint64
	{
		highest, exists := bft.msgPool.HighestN()
		if exists {
			nextHighest = highest + 1
		} else {
			nextHighest = consensusConfig.NextN
		}
	}

	highestN := bft.highestNAllowed()
	if nextHighest > highestN {
		err = fmt.Errorf("high water mark exceeded:%v", highestN)
		return
	}

	ppp, err := bft.constructPrePreparePiggybackedMsg(consensusConfig.View, nextHighest, msg)
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
	consensusConfig := bft.getConsensusConfig()

	if msg.View == consensusConfig.View && msg.PeerIndex == consensusConfig.Primary() {
		var p *PrepareMsg
		p, err = bft.constructPrepareMsg(&msg.PrePrepareMsg)
		if err != nil {
			return
		}
		bft.msgPool.AddPrepreparePiggybackedMsg(msg)
		bft.msgPool.AddPrepareMsg(p)
		bft.net.Broadcast(p)
	} else {
		err = fmt.Errorf("invalid PrePreparePiggybackedMsg(msg.View = %v, consensusConfig.View = %v, msg.PeerIndex = %v, consensusConfig.Primary = %v)", msg.View, consensusConfig.View, msg.PeerIndex, consensusConfig.Primary())
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
	log.Println(goid.Get(), "handlePrepareMsg")

	consensusConfig := bft.getConsensusConfig()

	if msg.View == consensusConfig.View {
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
		err = fmt.Errorf("invalid PrepareMsg(msg.View = %v, consensusConfig.View = %v)", msg.View, consensusConfig.View)
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
	consensusConfig := bft.getConsensusConfig()

	if msg.View == consensusConfig.View && msg.N >= consensusConfig.NextN {
		if added, commitLocal := bft.msgPool.AddCommitMsg(msg); added && commitLocal {
			// handle previous ones
			if msg.N > consensusConfig.NextN {
				for n := consensusConfig.NextN; n < msg.N; n++ {
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
						log.Println("SyncClientMsg err", err)
						time.Sleep(time.Second)
						continue
					}
					clientMsg = resp.ClientMsg
					break
				}
			}
			bft.fsm.Exec(clientMsg)
			bft.fsm.AddClientMsgAndProof(clientMsg, bft.msgPool.GetCommitMsgs(msg.N))

			if msg.N == bft.config.consensusConfig.NextCheckpoint {
				var checkPointMsg *CheckpointMsg
				for {
					checkPointMsg, err = bft.constructCheckpointMsg()
					if err != nil {
						log.Println("constructCheckpointMsg err", err)
						time.Sleep(time.Second)
						continue
					}
					break
				}
				if added, checkpointed := bft.msgPool.AddCheckpointMsg(checkPointMsg); added && checkpointed {
					nextCheckpoint := bft.config.consensusConfig.NextCheckpoint + consensusConfig.CheckpointInterval
					bft.fsm.UpdateNextCheckpoint(nextCheckpoint)
					bft.msgPool.Sealed(msg.N)
				}
				bft.fsm.Commit()
				bft.net.Broadcast(checkPointMsg)
			} else {
				bft.fsm.Commit()
			}

			bft.loadConsensusConfig()
		}
	} else {
		err = fmt.Errorf("invalid CommitMsg(msg.View = %v, consensusConfig.View = %v)", msg.View, consensusConfig.View)
	}
	return
}

func (bft *pbft) constructCheckpointMsg() (checkpointMsg *CheckpointMsg, err error) {
	return
}

func (bft *pbft) handleViewChangeMsg(msg *ViewChangeMsg) (err error) {
	consensusConfig := bft.getConsensusConfig()

	if msg.NewView > consensusConfig.View && consensusConfig.PrimaryOfView(msg.NewView) == bft.accountIndex {
		if added, enough := bft.msgPool.AddViewChangeMsg(msg); added && enough {
			var nv *NewViewMsg
			v, o := bft.msgPool.GetVO(msg.NewView)
			nv, err = bft.constructNewViewMsg(msg.NewView, v, o)
			if err != nil {
				return
			}
			bft.msgPool.AddNewViewMsg(nv)
			bft.net.Broadcast(nv)

			bft.fsm.UpdateV(msg.NewView)
			bft.loadConsensusConfig()
		}
	} else {
		log.Printf("ViewChangeMsg dropped for not being primary(%d) of specified view(%d), accountIndex(%d)\n", consensusConfig.PrimaryOfView(msg.NewView), msg.NewView, bft.accountIndex)
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
	consensusConfig := bft.getConsensusConfig()

	if msg.NewView > consensusConfig.View && msg.PeerIndex == consensusConfig.PrimaryOfView(msg.NewView) {

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

		bft.loadConsensusConfig()

	} else {
		err = fmt.Errorf("invalid NewViewMsg(msg.NewView = %v, consensusConfig.View = %v, msg.PeerIndex = %v, consensusConfig.PrimaryOfView(msg.NewView) = %v)", msg.NewView, consensusConfig.View, msg.PeerIndex, consensusConfig.PrimaryOfView(msg.NewView))
	}

	return
}

func (bft *pbft) handleCheckpointMsg(msg *CheckpointMsg) (err error) {
	if added, checkpointed := bft.msgPool.AddCheckpointMsg(msg); added && checkpointed {
		consensusConfig := bft.getConsensusConfig()
		nextCheckpoint := msg.N + consensusConfig.CheckpointInterval
		bft.fsm.UpdateNextCheckpoint(nextCheckpoint)
		bft.fsm.Commit()
		bft.config.consensusConfig.NextCheckpoint = nextCheckpoint
		bft.msgPool.Sealed(msg.N)
	}
	return
}

func (bft *pbft) handleSyncClientMessageReq(msg *SyncClientMessageReq) (err error) {
	consensusConfig := bft.getConsensusConfig()

	var clientMsg ClientMsg
	if consensusConfig.NextN <= msg.N {
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
