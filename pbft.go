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
	state        State
	config       *Config
	accountIndex uint32
	msgC         chan Msg
	doneCh       chan struct{}
	eventC       chan Event
	wg           sync.WaitGroup
	msgPool      *msgPool
	msgSyncer    *msgSyncer
	timer        *timer
}

// State of pbft
type State int32

const (
	// Ready to handle msg
	Ready State = iota
	// DropMsg for test
	DropMsg
)

// New a PBFT
func New() PBFT {
	bft := &pbft{doneCh: make(chan struct{}), eventC: make(chan Event, 100)}
	bft.msgPool = newMsgPool(bft)
	bft.msgSyncer = newMsgSyncer(bft)
	bft.timer = newTimer(bft, bft.eventC)
	return bft
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

	// persist InitConsensusConfig for the first time
	initConsensusConfig := bft.config.FSM.GetInitConsensusConfig()
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
		if initConsensusConfig.ViewChangeTimeout == 0 {
			initConsensusConfig.ViewChangeTimeout = defaultViewChangeTimeout
		}
		if initConsensusConfig.PeerHeartBeatTimeout == 0 {
			initConsensusConfig.PeerHeartBeatTimeout = defaultPeerHeartBeatTimeout
		}

		bft.config.FSM.Start()
		bft.config.FSM.InitConsensusConfig(initConsensusConfig)
		bft.config.FSM.Commit()
	}
	// InitConsensusConfig is never used afterwards
	bft.config.InitConsensusConfig = nil

	// load all states from FSM

	// 1. refresh consensus config
	bft.refreshConsensusConfig()

	// 2. init net with history peers
	bft.config.Net.OnUpdateConsensusPeers(bft.config.FSM.GetHistoryPeers())

	// 3. init self index
	bft.accountIndex = bft.config.FSM.GetIndexByPubkey(bft.config.Account.PublicKey())

	// schedule heartbeat
	bft.timer.ScheduleHeartBeat(bft.config.consensusConfig.PeerHeartBeatTimeout)

	// start to handle msg
	bft.msgC = make(chan Msg, bft.config.TuningOptions.MsgCSize)

	util.GoFunc(&bft.wg, func() {
		err := bft.handleMsg()
		log.Println("handleMsg quit", err)
	})

	return
}

// load from state
func (bft *pbft) refreshConsensusConfig() {
	config := bft.config.FSM.GetConsensusConfig()
	if config == nil {
		log.Fatal("GetConsensusConfig returns nil")
	}
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&bft.config.consensusConfig)), unsafe.Pointer(config))
}

// cached version
func (bft *pbft) getConsensusConfig() *ConsensusConfig {
	return (*ConsensusConfig)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&bft.config.consensusConfig))))
}

func (bft *pbft) setState(state State) {
	atomic.StoreInt32((*int32)(&bft.state), int32(state))
}

func (bft *pbft) getState() State {
	return (State)(atomic.LoadInt32((*int32)(&bft.state)))
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

func (bft *pbft) onStateChanged() {

	bft.refreshConsensusConfig()
	consensusConfig := bft.getConsensusConfig()

	if bft.config.FSM.IsConsensusPeersDirty() {
		if bft.accountIndex == NonConsensusIndex {
			// get index by public key
			pubKey := bft.config.Account.PublicKey()
			for _, peer := range consensusConfig.Peers {
				if pubKey == peer.Pubkey {
					bft.accountIndex = peer.Index
				}
			}
		}

		bft.config.Net.OnUpdateConsensusPeers(consensusConfig.Peers)
	}

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
			log.Println("G", goid.Get(), "accountIndex", bft.accountIndex, "got msg", msg.Type())
			if bft.getState() == DropMsg {
				log.Println("G", goid.Get(), "accountIndex", bft.accountIndex, "dropped msg", msg.Type())
				continue
			}
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
			case MessageTypeHeartBeat:
				err = bft.handleHeartBeatMsg(msg.(*HeartBeatMsg))
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
		case event := <-bft.eventC:
			log.Println("G", goid.Get(), "accountIndex", bft.accountIndex, "got event", event.Type())
			switch event.Type() {
			case TimerEventViewChange:
				err = bft.handleViewChangeEvent(event.(*ViewChangeEvent))
			case TimerEventHeartBeat:
				err = bft.handleHeartBeatEvent(event.(*HeartBeatEvent))
			default:
				err = fmt.Errorf("unexpected event type:%v", event.Type())
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

	cm = bft.config.FSM.GetClientMsgByDigest(digest)
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

func (bft *pbft) handleViewChangeEvent(event *ViewChangeEvent) (err error) {
	consensusConfig := bft.getConsensusConfig()
	if consensusConfig.View >= event.NewView {
		return
	}

	vc, err := bft.constructViewChangeMsg(event.NewView, bft.msgPool.GetPrepared())
	if err != nil {
		return
	}
	err = bft.handleViewChangeMsg(vc)

	bft.config.Net.Broadcast(vc)
	return
}

func (bft *pbft) handleHeartBeatEvent(event *HeartBeatEvent) (err error) {
	hb, err := bft.constructHeartBeatMsg()
	if err != nil {
		return
	}

	bft.config.Net.Broadcast(hb)

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
		log.Println("G", goid.Get(), "accountIndex", bft.accountIndex, "ScheduleViewChange", consensusConfig.ViewChangeTimeout)
		bft.timer.ScheduleViewChange(msg.Digest(), consensusConfig.View+1, consensusConfig.ViewChangeTimeout)

		bft.config.Net.SendTo(primary, msg)
		return
	}

	var nextHighest uint64
	{
		highest, exists := bft.msgPool.HighestN()
		if exists {
			nextHighest = highest + 1
		} else {
			// first client msg after being primary
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
	bft.config.Net.Broadcast(ppp)

	return
}

func (bft *pbft) constructPrePreparePiggybackedMsg(v, n uint64, msg ClientMsg) (ppp *PrePreparePiggybackedMsg, err error) {

	ppp = &PrePreparePiggybackedMsg{ClientMsg: msg, PrePrepareMsg: PrePrepareMsg{Signature: Signature{PeerIndex: bft.accountIndex}, View: v, N: n, ClientMsgDigest: msg.Digest()}}

	digest := ppp.SignatureDigest()
	sig, err := bft.config.Account.Sign(util.Slice(digest))
	ppp.Signature.Sig = sig
	return
}

func (bft *pbft) handlePrePreparePiggybackedMsg(msg *PrePreparePiggybackedMsg) (err error) {
	consensusConfig := bft.getConsensusConfig()

	if msg.View == consensusConfig.View && msg.PeerIndex == consensusConfig.Primary() && msg.N >= consensusConfig.NextN && msg.N <= bft.highestNAllowed() {

		var p *PrepareMsg
		p, err = bft.constructPrepareMsg(&msg.PrePrepareMsg)
		if err != nil {
			return
		}
		bft.msgPool.AddPrepreparePiggybackedMsg(msg)
		bft.msgPool.AddPrepareMsg(p)
		bft.config.Net.Broadcast(p)
	} else {
		err = fmt.Errorf("invalid PrePreparePiggybackedMsg(msg.View = %v, consensusConfig.View = %v, msg.PeerIndex = %v, consensusConfig.Primary = %v)", msg.View, consensusConfig.View, msg.PeerIndex, consensusConfig.Primary())
	}
	return
}

func (bft *pbft) constructPrepareMsg(msg *PrePrepareMsg) (p *PrepareMsg, err error) {
	p = &PrepareMsg{View: msg.View, N: msg.N, ClientMsgDigest: msg.ClientMsgDigest, Signature: Signature{PeerIndex: bft.accountIndex}}

	digest := p.SignatureDigest()
	sig, err := bft.config.Account.Sign(util.Slice(digest))
	p.Signature.Sig = sig

	return
}

func (bft *pbft) handlePrepareMsg(msg *PrepareMsg) (err error) {
	log.Println("G", goid.Get(), "accountIndex", bft.accountIndex, "handlePrepareMsg")

	consensusConfig := bft.getConsensusConfig()

	if msg.View == consensusConfig.View && msg.N >= consensusConfig.NextN && msg.N <= bft.highestNAllowed() {

		if added, prepared := bft.msgPool.AddPrepareMsg(msg); added && prepared {
			var c *CommitMsg
			c, err = bft.constructCommitMsg(msg)
			if err != nil {
				return
			}
			bft.msgPool.AddCommitMsg(c)
			bft.config.Net.Broadcast(c)
		}
	} else {
		err = fmt.Errorf("invalid PrepareMsg(msg.View = %v, consensusConfig.View = %v)", msg.View, consensusConfig.View)
	}
	return
}

func (bft *pbft) constructCommitMsg(msg *PrepareMsg) (c *CommitMsg, err error) {
	c = &CommitMsg{View: msg.View, N: msg.N, ClientMsgDigest: msg.ClientMsgDigest, Signature: Signature{PeerIndex: bft.accountIndex}}

	digest := c.SignatureDigest()
	sig, err := bft.config.Account.Sign(util.Slice(digest))
	c.Signature.Sig = sig

	return
}

// sync sealed msgs in range [from, to)
func (bft *pbft) syncSealedMsgs(from, to uint64) {
	for n := from; n < to; n++ {
		for {
			resp, err := bft.msgSyncer.SyncSealedClientMsg(context.Background(), n)
			if err != nil {
				time.Sleep(time.Second)
				log.Println("SyncSealedClientMsg err", err)
				continue
			}

			bft.timer.CancelViewChange(resp.ClientMsg.Digest())
			bft.config.FSM.Start()
			bft.config.FSM.StoreAndExec(resp.ClientMsg, resp.CommitMsgs, n)
			bft.config.FSM.Commit()
			bft.onStateChanged()
			break
		}
	}
	return
}

func (bft *pbft) handleCommitMsg(msg *CommitMsg) (err error) {
	consensusConfig := bft.getConsensusConfig()

	if msg.View == consensusConfig.View && msg.N >= consensusConfig.NextN && msg.N <= bft.highestNAllowed() {
		if added, commitLocal := bft.msgPool.AddCommitMsg(msg); added && commitLocal {
			// handle previous ones
			if msg.N > consensusConfig.NextN {
				bft.syncSealedMsgs(consensusConfig.NextN, msg.N)
				consensusConfig = bft.getConsensusConfig()
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
			bft.timer.CancelViewChange(clientMsg.Digest())
			bft.config.FSM.Start()
			bft.config.FSM.StoreAndExec(clientMsg, bft.msgPool.GetCommitMsgs(msg.N), msg.N)

			if msg.N == consensusConfig.NextCheckpoint {
				var checkPointMsg *CheckpointMsg
				for {
					checkPointMsg, err = bft.constructCheckpointMsg(bft.config.FSM.GetStateRoot(), msg.N)
					if err != nil {
						log.Println("constructCheckpointMsg err", err)
						time.Sleep(time.Second)
						continue
					}
					break
				}
				if added, checkpointed, checkpointMsgs := bft.msgPool.AddCheckpointMsg(checkPointMsg); added && checkpointed {
					bft.msgPool.Sealed(msg.N)
					bft.config.FSM.Sealed(msg.N, checkpointMsgs)
				}

				nextCheckpoint := bft.config.consensusConfig.NextCheckpoint + consensusConfig.CheckpointInterval
				bft.config.FSM.UpdateNextCheckpoint(nextCheckpoint)
				bft.config.FSM.Commit()
				bft.onStateChanged()
				bft.config.Net.Broadcast(checkPointMsg)
			} else {
				bft.config.FSM.Commit()
				bft.onStateChanged()
			}
		}
	} else {
		err = fmt.Errorf("invalid CommitMsg(msg.View = %v, msg.N = %v, consensusConfig.View = %v, consensusConfig.NextN = %v)", msg.View, msg.N, consensusConfig.View, consensusConfig.NextN)
	}
	return
}

func (bft *pbft) constructCheckpointMsg(stateRoot string, n uint64) (checkpointMsg *CheckpointMsg, err error) {
	checkpointMsg = &CheckpointMsg{N: n, StateRoot: stateRoot, Signature: Signature{PeerIndex: bft.accountIndex}}

	digest := checkpointMsg.SignatureDigest()
	sig, err := bft.config.Account.Sign(util.Slice(digest))
	checkpointMsg.Signature.Sig = sig
	return
}

func (bft *pbft) constructViewChangeMsg(newView uint64, prepared []Prepared) (vc *ViewChangeMsg, err error) {
	vc = &ViewChangeMsg{NewView: newView, P: prepared, Signature: Signature{PeerIndex: bft.accountIndex}}

	digest := vc.SignatureDigest()
	sig, err := bft.config.Account.Sign(util.Slice(digest))
	vc.Signature.Sig = sig
	return
}

func (bft *pbft) constructHeartBeatMsg() (hb *HeartBeatMsg, err error) {
	hb = &HeartBeatMsg{NextN: bft.getConsensusConfig().NextN, Signature: Signature{PeerIndex: bft.accountIndex}}

	digest := hb.SignatureDigest()
	sig, err := bft.config.Account.Sign(util.Slice(digest))
	hb.Signature.Sig = sig
	return
}

func (bft *pbft) handleViewChangeMsg(msg *ViewChangeMsg) (err error) {
	consensusConfig := bft.getConsensusConfig()

	if msg.NewView > consensusConfig.View && consensusConfig.PrimaryOfView(msg.NewView) == bft.accountIndex {
		if added, enough := bft.msgPool.AddViewChangeMsg(msg); added && enough {
			v, o := bft.msgPool.GetVO(msg.NewView)
			if v[bft.accountIndex] == nil {
				var selfVC *ViewChangeMsg
				for {
					selfVC, err = bft.constructViewChangeMsg(msg.NewView, bft.msgPool.GetPrepared())
					if err != nil {
						log.Println("constructViewChangeMsg err", err)
						time.Sleep(time.Second)
						continue
					}

					v[bft.accountIndex] = selfVC
					break
				}
			}

			var nv *NewViewMsg
			nv, err = bft.constructNewViewMsg(msg.NewView, v, o)
			if err != nil {
				return
			}
			bft.msgPool.AddNewViewMsg(nv)
			bft.config.Net.Broadcast(nv)

			bft.config.FSM.Start()
			bft.config.FSM.UpdateV(msg.NewView)
			bft.config.FSM.Commit()
			bft.onStateChanged()
		}
	} else {
		log.Printf("ViewChangeMsg dropped for not being primary(%d) of specified view(%d), accountIndex(%d) consensusConfig.View(%d)\n", consensusConfig.PrimaryOfView(msg.NewView), msg.NewView, bft.accountIndex, consensusConfig.View)
	}
	return
}

func (bft *pbft) constructNewViewMsg(newView uint64, v map[uint32] /*index*/ *ViewChangeMsg, o []*PrePrepareMsg) (msg *NewViewMsg, err error) {

	msg = &NewViewMsg{NewView: newView, V: v, O: o, Signature: Signature{PeerIndex: bft.accountIndex}}
	digest := msg.SignatureDigest()
	sig, err := bft.config.Account.Sign(util.Slice(digest))
	msg.Signature.Sig = sig
	return
}

func (bft *pbft) constructSyncClientMsgReq(n uint64, clientMsgDigest string) (msg *SyncClientMessageReq, err error) {
	msg = &SyncClientMessageReq{N: n, ClientMsgDigest: clientMsgDigest, Signature: Signature{PeerIndex: bft.accountIndex}}

	digest := msg.SignatureDigest()
	sig, err := bft.config.Account.Sign(util.Slice(digest))
	msg.Signature.Sig = sig

	return
}

func (bft *pbft) constructSyncSealedClientMsgReq(n uint64) (msg *SyncSealedClientMessageReq, err error) {
	msg = &SyncSealedClientMessageReq{N: n, Signature: Signature{PeerIndex: bft.accountIndex}}

	digest := msg.SignatureDigest()
	sig, err := bft.config.Account.Sign(util.Slice(digest))
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
			bft.config.Net.Broadcast(p)
		}
		bft.config.FSM.Start()
		bft.config.FSM.UpdateV(msg.NewView)
		bft.config.FSM.Commit()
		bft.onStateChanged()

	} else {
		err = fmt.Errorf("invalid NewViewMsg(msg.NewView = %v, consensusConfig.View = %v, msg.PeerIndex = %v, bft.accountIndex = %v, consensusConfig.PrimaryOfView(msg.NewView) = %v)", msg.NewView, consensusConfig.View, msg.PeerIndex, bft.accountIndex, consensusConfig.PrimaryOfView(msg.NewView))
	}

	return
}

func (bft *pbft) handleCheckpointMsg(msg *CheckpointMsg) (err error) {
	if added, checkpointed, checkpointMsgs := bft.msgPool.AddCheckpointMsg(msg); added && checkpointed {
		consensusConfig := bft.getConsensusConfig()
		nextCheckpoint := msg.N + consensusConfig.CheckpointInterval

		bft.config.FSM.Start()
		bft.config.FSM.UpdateNextCheckpoint(nextCheckpoint)
		bft.config.FSM.Sealed(msg.N, checkpointMsgs)
		bft.msgPool.Sealed(msg.N)
		bft.config.FSM.Commit()
		bft.onStateChanged()
	}
	return
}

func (bft *pbft) handleHeartBeatMsg(msg *HeartBeatMsg) (err error) {
	// TODO handle evil heartbeat msg
	consensusConfig := bft.getConsensusConfig()
	if msg.NextN > consensusConfig.NextN {
		bft.syncSealedMsgs(consensusConfig.NextN, msg.NextN)
	}
	return
}

func (bft *pbft) handleSyncClientMessageReq(msg *SyncClientMessageReq) (err error) {
	consensusConfig := bft.getConsensusConfig()

	var clientMsg ClientMsg
	if consensusConfig.NextN <= msg.N {
		clientMsg = bft.msgPool.GetClientMsg(msg.N)
	} else {
		clientMsg = bft.config.FSM.GetClientMsg(msg.N)
	}

	if clientMsg != nil && clientMsg.Digest() == msg.ClientMsgDigest {
		bft.config.Net.SendTo(msg.PeerIndex, &SyncClientMessageResp{ReqID: msg.ReqID, ClientMsg: clientMsg})
	}
	return
}

func (bft *pbft) handleSyncSealedClientMessageReq(msg *SyncSealedClientMessageReq) (err error) {
	clientMsg, commitMsgs := bft.config.FSM.GetClientMsgAndProof(msg.N)
	if clientMsg == nil {
		return
	}

	bft.config.Net.SendTo(msg.PeerIndex, &SyncSealedClientMessageResp{ReqID: msg.ReqID, ClientMsg: clientMsg, CommitMsgs: commitMsgs})
	return
}

func (bft *pbft) quorum() int {
	consensusConfig := bft.getConsensusConfig()
	n := len(consensusConfig.Peers)
	return n - (n-1)/3
}
