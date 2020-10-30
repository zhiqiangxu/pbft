package pbft

import "sync"

// default Net implementation
type net struct {
	sync.RWMutex
	bft     PBFT
	peerMap map[uint32]PeerInfo
}

func defaultNet() Net {
	return &net{peerMap: make(map[uint32]PeerInfo)}
}

func (n *net) SetPBFT(bft PBFT) {
	n.Lock()
	defer n.Unlock()

	n.bft = bft
	peers := bft.GetFSM().GetHistoryPeers()
	for _, peer := range peers {
		n.peerMap[peer.Index] = peer
	}
}

func (n *net) SendTo(peerIndex uint32, msg Msg) {

}

func (n *net) Broadcast(msg Msg) {

}

func (n *net) OnUpdateConsensusPeers(peers []PeerInfo) {
	n.Lock()
	defer n.Unlock()

	for _, peer := range peers {
		n.peerMap[peer.Index] = peer
	}
}
