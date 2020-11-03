package pbft

import "sync"

// default Net implementation
type net struct {
	sync.RWMutex
	peerMap map[uint32]PeerInfo
}

// NewNet ...
func NewNet() Net {
	return &net{peerMap: make(map[uint32]PeerInfo)}
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
