package pbft

import (
	"fmt"
	"testing"

	"github.com/ontio/ontology/common"
)

func TestPBft(t *testing.T) {
	// test with a simple fsm that only does ADD op

	account1 := RandAccount("")
	account2 := RandAccount("")
	account3 := RandAccount("")
	account4 := RandAccount("")

	bft1 := New()
	bft1.SetAccount(account1)
	bft1.SetConfig(Config{NewClientMsgFunc: newClientMsg, InitConsensusConfig: &InitConsensusConfig{
		Peers: []PeerInfo{
			{Index: 1, Pubkey: account1.PublicKey()},
			{Index: 2, Pubkey: account2.PublicKey()},
			{Index: 3, Pubkey: account3.PublicKey()},
			{Index: 4, Pubkey: account4.PublicKey()},
		},
	}})
	bft1.SetNet(nil)
	bft1.SetFSM(newFSM())

	err := bft1.Start()
	if err != nil {
		t.Fatalf("Start failed:%v", err)
	}

	defer bft1.Stop()

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
	n, eof := source.NextInt64()
	if !eof {
		err = fmt.Errorf("size wrong")
		return
	}

	msg.n = n
	return
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

func newFSM() FSM {
	return &fsm{
		clientMsgAndProof: make(map[uint64]clientMsgAndProof),
		digest2N:          make(map[string]uint64),
		pubkey2peer:       make(map[Pubkey]PeerInfo),
	}
}

func (f *fsm) Exec(msg ClientMsg) (changed bool, fromPeers, toPeers []PeerInfo) {
	cm := msg.(*testClientMsg)
	f.v += cm.n
	return
}

func (f *fsm) AddClientMsgAndProof(clientMsg ClientMsg, commits []*CommitMsg) {
	f.clientMsgAndProof[commits[0].N] = clientMsgAndProof{clientMsg: clientMsg, commits: commits}
	f.digest2N[clientMsg.Digest()] = commits[0].N
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
		f.Exec(initConsensusConfig.GenesisMsg)
		n := initConsensusConfig.N
		f.currentConsensusConfig = &ConsensusConfig{
			Peers:              initConsensusConfig.Peers,
			View:               initConsensusConfig.View,
			N:                  &n,
			CheckpointInterval: initConsensusConfig.CheckpointInterval,
			HighWaterMark:      initConsensusConfig.HighWaterMark,
		}
	} else {
		f.currentConsensusConfig = &ConsensusConfig{
			Peers:              initConsensusConfig.Peers,
			View:               initConsensusConfig.View,
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

func (f *fsm) GetN() (n uint64, err error) {
	if f.currentConsensusConfig == nil {
		err = fmt.Errorf("init not called")
		return
	}
	if f.currentConsensusConfig.N == nil {
		err = fmt.Errorf("no msg saved")
		return
	}
	n = *f.currentConsensusConfig.N
	return
}

func (f *fsm) UpdateV(v uint64) {
	f.currentConsensusConfig.View = v
}

func (f *fsm) UpdateLastCheckpoint(checkPoint uint64) {
	f.currentConsensusConfig.LastCheckpoint = &checkPoint
}

func (f *fsm) Commit() {

}
