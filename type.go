package pbft

import (
	"context"
	"fmt"

	"github.com/ontio/ontology/common"
)

// FSM for deterministic state machine
type FSM interface {
	Exec(ClientMsg) (changed bool, fromPeers, toPeers []PeerInfo)

	AddClientMsgAndProof(ClientMsg, []*CommitMsg) // N is implicitly saved
	GetClientMsgAndProof(n uint64) (ClientMsg, []*CommitMsg)
	GetClientMsg(n uint64) ClientMsg
	GetClientMsgByDigest(digest string) ClientMsg

	InitConsensusConfig(*InitConsensusConfig) // also updates history peers
	UpdteConsensusPeers([]PeerInfo)           // also updates history peers
	GetIndexByPubkey(pk Pubkey) uint32        // returns NonConsensusIndex if not found
	GetConsensusConfig() *ConsensusConfig
	GetInitConsensusConfig() *InitConsensusConfig
	GetHistoryPeers() []PeerInfo
	GetV() (uint64, error)
	GetN() (uint64, error)
	UpdateV(v uint64)
	UpdateLastCheckpoint(checkPoint uint64)

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
	NewClientMsgFunc func() ClientMsg
	TuningOptions    *TuningOptions
	// it only takes effects at the first time when bootstrap
	InitConsensusConfig *InitConsensusConfig
	// current consensus config
	consensusConfig *ConsensusConfig
}

// InitConsensusConfig should never change since day 1
type InitConsensusConfig struct {
	GenesisMsg         ClientMsg // optional
	Peers              []PeerInfo
	View               uint64
	N                  uint64
	CheckpointInterval uint64
	HighWaterMark      uint64
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
	N                  *uint64
	CheckpointInterval uint64
	HighWaterMark      uint64
	LastCheckpoint     *uint64
}

const (
	defaultCheckpointInterval = uint64(1)
	defaultHighWaterMark      = uint64(100)
)

// Validate a Config
func (config *Config) Validate() (err error) {
	switch {
	case config.NewClientMsgFunc == nil:
		err = fmt.Errorf("NewClientMsgFunc empty")
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
	SetPBFT(PBFT)
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
	SetFSM(FSM)
	GetFSM() FSM
	SetNet(Net)
	GetNet() Net
	SetAccount(Account)
	SetConfig(Config)

	Start() error
	Stop() error

	Send(context.Context, Msg) error
}

// MessageType for pbft
type MessageType uint32

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

// PayloadMsg for transport
type PayloadMsg struct {
	Type    MessageType
	Payload []byte
	msg     Msg
}

// Deserialization a PayloadMsg
func (pm *PayloadMsg) Deserialization(source *common.ZeroCopySource) error {
	return nil
}

// Serialization a PayloadMsg
func (pm *PayloadMsg) Serialization(sink *common.ZeroCopySink) {

}

// Signature ...
type Signature struct {
	PeerIndex uint32
	Sig       []byte
}

// PrePrepareMsg ...
type PrePrepareMsg struct {
	Signature
	View            uint64
	N               uint64
	ClientMsgDigest string
}

// Type of msg
func (pp *PrePrepareMsg) Type() MessageType {
	return MessageTypePrePrepare
}

// SignatureDigest of msg
func (pp *PrePrepareMsg) SignatureDigest() string {
	return ""
}

// Deserialization a PrePrepareMsg
func (pp *PrePrepareMsg) Deserialization(source *common.ZeroCopySource) error {
	return nil
}

// Serialization a PrePrepareMsg
func (pp *PrePrepareMsg) Serialization(sink *common.ZeroCopySink) {

}

// PrePreparePiggybackedMsg ...
type PrePreparePiggybackedMsg struct {
	PrePrepareMsg
	ClientMsg
}

// Type of msg
func (ppp *PrePreparePiggybackedMsg) Type() MessageType {
	return MessageTypePrePreparePiggybacked
}

// SignatureDigest of msg
func (ppp *PrePreparePiggybackedMsg) SignatureDigest() string {
	return ""
}

// Deserialization a PrePreparePiggybackedMsg
func (ppp *PrePreparePiggybackedMsg) Deserialization(source *common.ZeroCopySource) error {
	return nil
}

// Serialization a PrePreparePiggybackedMsg
func (ppp *PrePreparePiggybackedMsg) Serialization(sink *common.ZeroCopySink) {

}

// PrepareMsg ...
type PrepareMsg struct {
	Signature
	View            uint64
	N               uint64
	ClientMsgDigest string
}

// Type of msg
func (p *PrepareMsg) Type() MessageType {
	return MessageTypePrepare
}

// SignatureDigest of msg
func (p *PrepareMsg) SignatureDigest() string {
	return ""
}

// Deserialization a PrepareMsg
func (p *PrepareMsg) Deserialization(source *common.ZeroCopySource) error {
	return nil
}

// Serialization a PrepareMsg
func (p *PrepareMsg) Serialization(sink *common.ZeroCopySink) {

}

// CommitMsg ...
type CommitMsg struct {
	Signature
	View            uint64
	N               uint64
	ClientMsgDigest string
}

// Type of msg
func (c *CommitMsg) Type() MessageType {
	return MessageTypeCommit
}

// SignatureDigest of msg
func (c *CommitMsg) SignatureDigest() string {
	return ""
}

// Deserialization a CommitMsg
func (c *CommitMsg) Deserialization(source *common.ZeroCopySource) error {
	return nil
}

// Serialization a CommitMsg
func (c *CommitMsg) Serialization(sink *common.ZeroCopySink) {

}

// Prepared ...
type Prepared struct {
	PrepareMsgs   []PrepareMsg
	PrePrepareMsg PrePrepareMsg
}

// ViewChangeMsg ...
type ViewChangeMsg struct {
	Signature
	NewView uint64
	N       uint64
	P       []Prepared
}

// Type of msg
func (vc *ViewChangeMsg) Type() MessageType {
	return MessageTypeViewChange
}

// SignatureDigest of msg
func (vc *ViewChangeMsg) SignatureDigest() string {
	return ""
}

// Deserialization a ViewChangeMsg
func (vc *ViewChangeMsg) Deserialization(source *common.ZeroCopySource) error {
	return nil
}

// Serialization a ViewChangeMsg
func (vc *ViewChangeMsg) Serialization(sink *common.ZeroCopySink) {

}

// NewViewMsg ...
type NewViewMsg struct {
	Signature
	NewView uint64
	V       []*ViewChangeMsg
	O       []*PrePrepareMsg
}

// Type of msg
func (nv *NewViewMsg) Type() MessageType {
	return MessageTypeNewView
}

// SignatureDigest of msg
func (nv *NewViewMsg) SignatureDigest() string {
	return ""
}

// Deserialization a NewViewMsg
func (nv *NewViewMsg) Deserialization(source *common.ZeroCopySource) error {
	return nil
}

// Serialization a NewViewMsg
func (nv *NewViewMsg) Serialization(sink *common.ZeroCopySink) {

}

// CheckpointMsg ...
type CheckpointMsg struct {
	Signature
	N         uint64
	StateRoot string
}

// Type of msg
func (ckpt *CheckpointMsg) Type() MessageType {
	return MessageTypeCheckpoint
}

// SignatureDigest of msg
func (ckpt *CheckpointMsg) SignatureDigest() string {
	return ""
}

// Deserialization a NewViewMsg
func (ckpt *CheckpointMsg) Deserialization(source *common.ZeroCopySource) error {
	return nil
}

// Serialization a NewViewMsg
func (ckpt *CheckpointMsg) Serialization(sink *common.ZeroCopySink) {

}

// SyncClientMessageReq ...
type SyncClientMessageReq struct {
	ReqID uint64 // updated by msg syncer
	Signature
	N               uint64
	ClientMsgDigest string
}

// Type of msg
func (sync *SyncClientMessageReq) Type() MessageType {
	return MessageTypeSyncClientMessageReq
}

// SignatureDigest of msg
func (sync *SyncClientMessageReq) SignatureDigest() string {
	return ""
}

// Deserialization a SyncClientMessageReq
func (sync *SyncClientMessageReq) Deserialization(source *common.ZeroCopySource) error {
	return nil
}

// Serialization a SyncClientMessageReq
func (sync *SyncClientMessageReq) Serialization(sink *common.ZeroCopySink) {

}

// SyncClientMessageResp ...
type SyncClientMessageResp struct {
	ReqID uint64
	ClientMsg
}

// Type of msg
func (sync *SyncClientMessageResp) Type() MessageType {
	return MessageTypeSyncClientMessageResp
}

// SignatureDigest of msg
func (sync *SyncClientMessageResp) SignatureDigest() string {
	return ""
}

// Deserialization a SyncClientMessageResp
func (sync *SyncClientMessageResp) Deserialization(source *common.ZeroCopySource) error {
	return nil
}

// Serialization a SyncClientMessageResp
func (sync *SyncClientMessageResp) Serialization(sink *common.ZeroCopySink) {

}

// SyncSealedClientMessageReq ...
type SyncSealedClientMessageReq struct {
	ReqID uint64
	Signature
	N uint64
}

// Type of msg
func (sync *SyncSealedClientMessageReq) Type() MessageType {
	return MessageTypeSyncSealedClientMessageReq
}

// SignatureDigest of msg
func (sync *SyncSealedClientMessageReq) SignatureDigest() string {
	return ""
}

// Deserialization a SyncSealedClientMessageReq
func (sync *SyncSealedClientMessageReq) Deserialization(source *common.ZeroCopySource) error {
	return nil
}

// Serialization a SyncSealedClientMessageReq
func (sync *SyncSealedClientMessageReq) Serialization(sink *common.ZeroCopySink) {

}

// SyncSealedClientMessageResp ...
type SyncSealedClientMessageResp struct {
	ReqID uint64
	ClientMsg
	CommitMsgs []*CommitMsg
}

// Type of msg
func (sync *SyncSealedClientMessageResp) Type() MessageType {
	return MessageTypeSyncSealedClientMessageResp
}

// SignatureDigest of msg
func (sync *SyncSealedClientMessageResp) SignatureDigest() string {
	return ""
}

// Deserialization a SyncSealedClientMessageResp
func (sync *SyncSealedClientMessageResp) Deserialization(source *common.ZeroCopySource) error {
	return nil
}

// Serialization a SyncSealedClientMessageResp
func (sync *SyncSealedClientMessageResp) Serialization(sink *common.ZeroCopySink) {

}
