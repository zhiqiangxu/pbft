package pbft

import (
	"context"
	"fmt"
	"time"

	"github.com/ontio/ontology/common"
)

// FSM for deterministic state machine
type FSM interface {
	Start() // reset all IsXXXDirty to false

	// genesis config
	InitConsensusConfig(*InitConsensusConfig) // also updates history peers and execs optional genesis msg
	StoreAndExec(ClientMsg, map[uint32]*CommitMsg, uint64 /*N*/)
	Sealed(uint64, map[uint32]*CheckpointMsg)
	UpdateV(uint64)
	UpdateNextCheckpoint(uint64)
	UpdateCheckpointInterval(uint64)
	UpdateHighWaterMark(uint64)
	UpdteConsensusPeers([]PeerInfo) // also updates history peers

	GetInitConsensusConfig() *InitConsensusConfig
	GetClientMsgAndProof(n uint64) (ClientMsg, map[uint32]*CommitMsg)
	GetClientMsg(n uint64) ClientMsg
	GetClientMsgByDigest(digest string) ClientMsg
	GetStateRoot() string

	GetIndexByPubkey(pk Pubkey) uint32    // returns NonConsensusIndex if not found
	GetConsensusConfig() *ConsensusConfig // each call should return a new copy
	GetHistoryPeers() []PeerInfo

	IsVDirty() bool
	IsNextCheckpointDirty() bool
	IsCheckpointIntervalDirty() bool
	IsHighWaterMarkDirty() bool
	IsConsensusPeersDirty() bool

	GetV() (uint64, error)
	GetNextN() (uint64, error)
	GetHighWaterMark() uint64
	GetCheckpointInterval() uint64

	Commit()
}

// Msg for all msg
type Msg interface {
	Type() MessageType
	Serialization(sink *common.ZeroCopySink)
	Deserialization(source *common.ZeroCopySource) error
}

// ClientMsg ...
type ClientMsg interface {
	Msg
	Digest() string
}

// ConsensusMsg ...
type ConsensusMsg interface {
	Msg
	SignatureDigest() string
}

// Pubkey for public key
type Pubkey string

// PeerInfo for peer info
type PeerInfo struct {
	Index uint32
	Pubkey
}

// Config for pbft
type Config struct {
	// current consensus config, accessed by atomic
	consensusConfig *ConsensusConfig
	TuningOptions   *TuningOptions
	FSM             FSM
	Net             Net
	Account         Account
	// it only takes effects at the first time when bootstrap
	// and it's set to nil after usage
	InitConsensusConfig *InitConsensusConfig
}

// NewClientMsgFunc is shared by all other parts
var NewClientMsgFunc func() ClientMsg

// InitConsensusConfig should never change since day 1
type InitConsensusConfig struct {
	GenesisMsg         ClientMsg // optional
	Peers              []PeerInfo
	View               uint64
	N                  uint64
	CheckpointInterval uint64
	HighWaterMark      uint64
	ViewChangeTimeout  time.Duration
}

// Validate InitConsensusConfig
func (iconfig *InitConsensusConfig) Validate() (err error) {
	if iconfig == nil {
		err = fmt.Errorf("InitConsensusConfig empty")
		return
	}
	if len(iconfig.Peers) == 0 {
		err = fmt.Errorf("InitConsensusConfig.Peers empty")
		return
	}

	peerMap := make(map[uint32]bool)
	for _, peer := range iconfig.Peers {
		if _, ok := peerMap[peer.Index]; ok {
			err = fmt.Errorf("duplicate peer index in InitConsensusConfig:%v", peer.Index)
			return
		}
		peerMap[peer.Index] = true
	}
	return
}

// ConsensusConfig is persisted to db
type ConsensusConfig struct {
	Peers              []PeerInfo
	View               uint64
	NextN              uint64
	CheckpointInterval uint64
	HighWaterMark      uint64
	NextCheckpoint     uint64
	ViewChangeTimeout  time.Duration
}

// Primary index
func (cconfig *ConsensusConfig) Primary() uint32 {
	return cconfig.PrimaryOfView(cconfig.View)
}

// PrimaryOfView for primary of specified view
func (cconfig *ConsensusConfig) PrimaryOfView(v uint64) uint32 {
	return cconfig.Peers[v%uint64(len(cconfig.Peers))].Index
}

const (
	defaultCheckpointInterval = uint64(1)
	defaultHighWaterMark      = uint64(100)
	defaultViewChangeTimeout  = time.Second * 10
)

// Validate a Config
func (config *Config) Validate() (err error) {
	switch {
	case config == nil:
		err = fmt.Errorf("config is nil")
		return
	case config.Account == nil:
		err = fmt.Errorf("Account is nil")
		return
	case config.Net == nil:
		err = fmt.Errorf("Net is nil")
		return
	case config.FSM == nil:
		err = fmt.Errorf("FSM is nil")
		return
	}

	return
}

// TuningOptions for tuning
type TuningOptions struct {
	MsgCSize int
}

var defaultTuningOptions = &TuningOptions{MsgCSize: 100}

// Net for network related stuff
type Net interface {
	Broadcast(msg Msg)
	SendTo(peerIndex uint32, msg Msg)
	OnUpdateConsensusPeers([]PeerInfo)
}

// Account ...
type Account interface {
	PublicKey() Pubkey
	Sign([]byte) ([]byte, error)
}

// PBFT defines system
type PBFT interface {
	SetConfig(*Config)
	GetConfig() *Config

	Start() error
	Stop() error

	Send(context.Context, Msg) error
}

// MessageType for pbft
type MessageType uint32

func (mt MessageType) String() string {
	switch mt {
	case MessageTypeClient:
		return "MessageTypeClient"
	case MessageTypePrePrepare:
		return "MessageTypePrePrepare"
	case MessageTypePrePreparePiggybacked:
		return "MessageTypePrePreparePiggybacked"
	case MessageTypePrepare:
		return "MessageTypePrepare"
	case MessageTypeCommit:
		return "MessageTypeCommit"
	case MessageTypeViewChange:
		return "MessageTypeViewChange"
	case MessageTypeNewView:
		return "MessageTypeNewView"
	case MessageTypeCheckpoint:
		return "MessageTypeCheckpoint"
	case MessageTypeSyncClientMessageReq:
		return "MessageTypeSyncClientMessageReq"
	case MessageTypeSyncSealedClientMessageReq:
		return "MessageTypeSyncSealedClientMessageReq"
	case MessageTypeSyncClientMessageResp:
		return "MessageTypeSyncClientMessageResp"
	case MessageTypeSyncSealedClientMessageResp:
		return "MessageTypeSyncSealedClientMessageResp"
	default:
		panic(fmt.Sprintf("unkown message type:%d", mt))
	}
}

const (
	// MessageTypeClient for client msg
	MessageTypeClient MessageType = iota
	// MessageTypePrePrepare for pre-prepare msg
	MessageTypePrePrepare
	// MessageTypePrePreparePiggybacked for piggypacked pre-prepare msg
	MessageTypePrePreparePiggybacked
	// MessageTypePrepare for prepare msg
	MessageTypePrepare
	// MessageTypeCommit for commit msg
	MessageTypeCommit
	// MessageTypeViewChange for view-change msg
	MessageTypeViewChange
	// MessageTypeNewView for new-view msg
	MessageTypeNewView
	// MessageTypeCheckpoint for checkpoint msg
	MessageTypeCheckpoint

	// MessageTypeSyncClientMessageReq for sync client msg
	MessageTypeSyncClientMessageReq
	// MessageTypeSyncSealedClientMessageReq for sync client msg with proof
	MessageTypeSyncSealedClientMessageReq
	// MessageTypeSyncClientMessageResp is resp to MessageTypeSyncClientMessageReq
	MessageTypeSyncClientMessageResp
	// MessageTypeSyncSealedClientMessageResp is resp to MessageTypeSyncSealedClientMessageReq
	MessageTypeSyncSealedClientMessageResp
)

// TimerEvent ...
type TimerEvent int

const (
	// TimerEventViewChange ...
	TimerEventViewChange TimerEvent = iota
)

func (ev TimerEvent) String() string {
	switch ev {
	case TimerEventViewChange:
		return "TimerEventViewChange"
	default:
		panic(fmt.Sprintf("unkown event type:%d", ev))
	}
}

// Event ...
type Event interface {
	Type() TimerEvent
}
