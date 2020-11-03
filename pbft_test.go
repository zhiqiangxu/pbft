package pbft

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/ontio/ontology/common"
)

func TestPBft(t *testing.T) {

	// test with a simple fsm that only does ADD op

	account1 := RandAccount("")
	account2 := RandAccount("")
	account3 := RandAccount("")
	account4 := RandAccount("")
	accounts := []Account{account1, account2, account3, account4}
	index1 := uint32(1)
	index2 := uint32(2)
	index3 := uint32(3)
	index4 := uint32(4)
	indices := []uint32{index1, index2, index3, index4}

	bft1 := New()
	bft2 := New()
	bft3 := New()
	bft4 := New()
	bfts := []PBFT{bft1, bft2, bft3, bft4}

	fsm1 := newTestFSM()
	fsm2 := newTestFSM()
	fsm3 := newTestFSM()
	fsm4 := newTestFSM()
	fsms := []*fsm{fsm1, fsm2, fsm3, fsm4}

	net1 := newTestNet()
	net2 := newTestNet()
	net3 := newTestNet()
	net4 := newTestNet()
	nets := []*testNet{net1, net2, net3, net4}
	index2netidx := map[uint32]int{}
	for i := range nets {
		index2netidx[indices[i]] = i
	}
	for i, net := range nets {
		net.bft = bfts[i]
		net.selfIndex = indices[i]
		net.nets = nets
		net.index2netidx = index2netidx
	}

	NewClientMsgFunc = newClientMsg
	for i, account := range accounts {
		bfts[i].SetConfig(&Config{
			FSM:     fsms[i],
			Net:     nets[i],
			Account: account,
			InitConsensusConfig: &InitConsensusConfig{
				Peers: []PeerInfo{
					{Index: index1, Pubkey: account1.PublicKey()},
					{Index: index2, Pubkey: account2.PublicKey()},
					{Index: index3, Pubkey: account3.PublicKey()},
					{Index: index4, Pubkey: account4.PublicKey()},
				},
			}})
	}

	for _, bft := range bfts {
		err := bft.Start()
		if err != nil {
			t.Fatalf("Start failed:%v", err)
		}

		defer bft.Stop()
	}

	{
		// test happy path
		err := bft1.Send(context.Background(), &testClientMsg{n: 10})
		if err != nil {
			t.Fatalf("Send failed:%v", err)
		}

		time.Sleep(time.Second)

		for _, fsm := range fsms {
			if fsm.v != 10 {
				t.Fatalf("fsm.v wrong:%v", fsm.v)
			}
		}
	}

}

type testClientMsg struct {
	n int64
}

func (msg *testClientMsg) Type() MessageType {
	return MessageTypeClient
}

func (msg *testClientMsg) Digest() string {
	return fmt.Sprintf("%d", msg.n)
}

func (msg *testClientMsg) Serialization(sink *common.ZeroCopySink) {
	sink.WriteInt64(msg.n)
}

func (msg *testClientMsg) Deserialization(source *common.ZeroCopySource) (err error) {
	n, _ := source.NextInt64()

	msg.n = n
	return
}

type testNet struct {
	bft          PBFT
	nets         []*testNet
	selfIndex    uint32
	index2netidx map[uint32]int
}

func (net *testNet) SetPBFT(bft PBFT) {
	net.bft = bft
}

func (net *testNet) Broadcast(msg Msg) {

	for index, netIndex := range net.index2netidx {
		if index != net.selfIndex {
			sink := common.NewZeroCopySink(nil)
			msg.Serialization(sink)
			payloadMsg := &PayloadMsg{Type: msg.Type(), Payload: sink.Bytes()}
			net.nets[netIndex].onPayload(payloadMsg)
		}
	}
}

func (net *testNet) onPayload(payloadMsg *PayloadMsg) {
	msg, err := payloadMsg.DeserializePayload()
	if err != nil {
		log.Fatalf("DeserializePayload fail:%v", err)
	}
	err = net.bft.Send(context.Background(), msg)
	if err != nil {
		log.Fatalf("Send fail:%v", err)
	}
}

func (net *testNet) SendTo(peerIndex uint32, msg Msg) {

}

func (net *testNet) OnUpdateConsensusPeers([]PeerInfo) {

}

func newTestNet() *testNet {
	return &testNet{}
}

func newClientMsg() ClientMsg {
	return &testClientMsg{}
}

type clientMsgAndProof struct {
	clientMsg ClientMsg
	commits   []*CommitMsg
}

type fsm struct {
	v                      int64
	clientMsgAndProof      map[uint64]clientMsgAndProof
	digest2N               map[string]uint64
	initConsensusConfig    *InitConsensusConfig
	currentConsensusConfig *ConsensusConfig
	pubkey2peer            map[Pubkey]PeerInfo
}

func newTestFSM() *fsm {
	return &fsm{
		clientMsgAndProof: make(map[uint64]clientMsgAndProof),
		digest2N:          make(map[string]uint64),
		pubkey2peer:       make(map[Pubkey]PeerInfo),
	}
}

func (f *fsm) Start() {

}

func (f *fsm) GetCheckpointInterval() uint64 {
	return f.currentConsensusConfig.CheckpointInterval
}

func (f *fsm) GetHighWaterMark() uint64 {
	return f.currentConsensusConfig.HighWaterMark
}

func (f *fsm) UpdateHighWaterMark(uint64) {

}

func (f *fsm) UpdateCheckpointInterval(v uint64) {

}

func (f *fsm) IsVDirty() bool {
	panic("todo")
}
func (f *fsm) IsNextCheckpointDirty() bool {
	panic("todo")
}
func (f *fsm) IsCheckpointIntervalDirty() bool {
	panic("todo")
}
func (f *fsm) IsHighWaterMarkDirty() bool {
	panic("todo")
}
func (f *fsm) IsConsensusPeersDirty() bool {
	panic("todo")
}

func (f *fsm) StoreAndExec(msg ClientMsg, commits []*CommitMsg, n uint64) {
	if len(commits) > 0 {
		n = commits[0].N
	}
	f.clientMsgAndProof[n] = clientMsgAndProof{clientMsg: msg, commits: commits}
	f.digest2N[msg.Digest()] = n

	cm := msg.(*testClientMsg)
	f.v += cm.n
	return
}

func (f *fsm) GetClientMsgAndProof(n uint64) (ClientMsg, []*CommitMsg) {
	clientMsgAndProof, ok := f.clientMsgAndProof[n]
	if !ok {
		return nil, nil
	}
	return clientMsgAndProof.clientMsg, clientMsgAndProof.commits
}

func (f *fsm) GetClientMsg(n uint64) ClientMsg {
	clientMsgAndProof, ok := f.clientMsgAndProof[n]
	if !ok {
		return nil
	}
	return clientMsgAndProof.clientMsg
}

func (f *fsm) GetClientMsgByDigest(digest string) ClientMsg {
	n, ok := f.digest2N[digest]
	if !ok {
		return nil
	}

	return f.GetClientMsg(n)
}

func (f *fsm) InitConsensusConfig(initConsensusConfig *InitConsensusConfig) {
	if f.initConsensusConfig != nil {
		panic("InitConsensusConfig called twice")
	}
	for _, peer := range initConsensusConfig.Peers {
		f.pubkey2peer[peer.Pubkey] = peer
	}
	f.initConsensusConfig = initConsensusConfig
	if initConsensusConfig.GenesisMsg != nil {
		f.StoreAndExec(initConsensusConfig.GenesisMsg, nil, initConsensusConfig.N)
		n := initConsensusConfig.N + 1
		f.currentConsensusConfig = &ConsensusConfig{
			Peers:              initConsensusConfig.Peers,
			View:               initConsensusConfig.View,
			NextN:              n,
			NextCheckpoint:     n + initConsensusConfig.CheckpointInterval - 1,
			CheckpointInterval: initConsensusConfig.CheckpointInterval,
			HighWaterMark:      initConsensusConfig.HighWaterMark,
		}
	} else {
		f.currentConsensusConfig = &ConsensusConfig{
			Peers:              initConsensusConfig.Peers,
			View:               initConsensusConfig.View,
			NextN:              initConsensusConfig.N,
			NextCheckpoint:     initConsensusConfig.N + initConsensusConfig.CheckpointInterval - 1,
			CheckpointInterval: initConsensusConfig.CheckpointInterval,
			HighWaterMark:      initConsensusConfig.HighWaterMark,
		}
	}

	return
}

func (f *fsm) UpdteConsensusPeers(peers []PeerInfo) {
	for _, peer := range peers {
		f.pubkey2peer[peer.Pubkey] = peer
	}
	f.currentConsensusConfig.Peers = peers
	return
}

func (f *fsm) GetIndexByPubkey(pk Pubkey) (idx uint32) {
	peer, exists := f.pubkey2peer[pk]
	if !exists {
		idx = NonConsensusIndex
	} else {
		idx = peer.Index
	}
	return
}

func (f *fsm) GetConsensusConfig() *ConsensusConfig {
	return f.currentConsensusConfig
}

func (f *fsm) GetInitConsensusConfig() *InitConsensusConfig {
	return f.initConsensusConfig
}

func (f *fsm) GetHistoryPeers() (peers []PeerInfo) {
	for _, peer := range f.pubkey2peer {
		peers = append(peers, peer)
	}
	return
}

func (f *fsm) GetV() (v uint64, err error) {
	if f.currentConsensusConfig == nil {
		err = fmt.Errorf("init not called")
		return
	}
	v = f.currentConsensusConfig.View
	return
}

func (f *fsm) GetNextN() (n uint64, err error) {
	if f.currentConsensusConfig == nil {
		err = fmt.Errorf("init not called")
		return
	}
	n = f.currentConsensusConfig.NextN
	return
}

func (f *fsm) UpdateV(v uint64) {
	f.currentConsensusConfig.View = v
}

func (f *fsm) UpdateNextCheckpoint(checkPoint uint64) {
	f.currentConsensusConfig.NextCheckpoint = checkPoint
}

func (f *fsm) Commit() {

}
