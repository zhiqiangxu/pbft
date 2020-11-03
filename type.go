package pbft

import (
	"context"
	"fmt"

	"github.com/ontio/ontology/common"
)

// FSM for deterministic state machine
type FSM interface {
	Start() // reset all IsXXXDirty to false

	// genesis config
	InitConsensusConfig(*InitConsensusConfig) // also updates history peers and execs optional genesis msg
	StoreAndExec(ClientMsg, []*CommitMsg, uint64 /*N*/)
	UpdateV(uint64)
	UpdateNextCheckpoint(uint64)
	UpdateCheckpointInterval(uint64)
	UpdateHighWaterMark(uint64)
	UpdteConsensusPeers([]PeerInfo) // also updates history peers

	GetInitConsensusConfig() *InitConsensusConfig
	GetClientMsgAndProof(n uint64) (ClientMsg, []*CommitMsg)
	GetClientMsg(n uint64) ClientMsg
	GetClientMsgByDigest(digest string) ClientMsg

	GetIndexByPubkey(pk Pubkey) uint32 // returns NonConsensusIndex if not found
	GetConsensusConfig() *ConsensusConfig
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

// PayloadMsg for transport
type PayloadMsg struct {
	Type    MessageType
	Payload []byte
}

// DeserializePayload ...
func (pm *PayloadMsg) DeserializePayload() (msg Msg, err error) {

	switch pm.Type {
	case MessageTypeClient:
		msg = NewClientMsgFunc()
	case MessageTypePrePrepare:
		msg = &PrePrepareMsg{}
	case MessageTypePrePreparePiggybacked:
		msg = &PrePreparePiggybackedMsg{}
	case MessageTypePrepare:
		msg = &PrepareMsg{}
	case MessageTypeCommit:
		msg = &CommitMsg{}
	case MessageTypeViewChange:
		msg = &ViewChangeMsg{}
	case MessageTypeNewView:
		msg = &NewViewMsg{}
	case MessageTypeCheckpoint:
		msg = &CheckpointMsg{}
	case MessageTypeSyncClientMessageReq:
		msg = &SyncClientMessageReq{}
	case MessageTypeSyncSealedClientMessageReq:
		msg = &SyncSealedClientMessageReq{}
	case MessageTypeSyncClientMessageResp:
		msg = &SyncClientMessageResp{}
	case MessageTypeSyncSealedClientMessageResp:
		msg = &SyncSealedClientMessageResp{}
	default:
		err = fmt.Errorf("invalid message type:%v", pm.Type)
		return
	}

	source := common.NewZeroCopySource(pm.Payload)
	err = msg.Deserialization(source)
	if err != nil {
		return
	}

	if source.Len() != 0 {
		err = fmt.Errorf("bytes left in PayloadMsg")
	}
	return
}

// Deserialization a PayloadMsg
func (pm *PayloadMsg) Deserialization(source *common.ZeroCopySource) (err error) {
	typeU32, eof := source.NextUint32()
	if eof {
		err = fmt.Errorf("PayloadMsg Deserialization NextUint32 unexpected eof")
		return
	}
	payload, _, irregular, eof := source.NextVarBytes()
	if irregular {
		err = fmt.Errorf("PayloadMsg Deserialization NextVarBytes irregular data")
		return
	}
	if eof {
		err = fmt.Errorf("PayloadMsg Deserialization NextVarBytes unexpected eof")
		return
	}

	pm.Type = MessageType(typeU32)
	pm.Payload = payload
	return
}

// Serialization a PayloadMsg
func (pm *PayloadMsg) Serialization(sink *common.ZeroCopySink) {
	sink.WriteUint32(uint32(pm.Type))
	sink.WriteVarBytes(pm.Payload)
}

// Signature ...
type Signature struct {
	PeerIndex uint32
	Sig       []byte
}

// Deserialization a Signature
func (sig *Signature) Deserialization(source *common.ZeroCopySource) (err error) {
	peerIndex, eof := source.NextUint32()
	if eof {
		err = fmt.Errorf("Signature Deserialization NextUint32 unexpected eof")
		return
	}

	sigBytes, _, irregular, eof := source.NextVarBytes()
	if irregular {
		err = fmt.Errorf("Signature Deserialization NextVarBytes irregular data")
		return
	}
	if eof {
		err = fmt.Errorf("Signature Deserialization NextVarBytes unexpected eof")
		return
	}
	sig.PeerIndex = peerIndex
	sig.Sig = sigBytes
	return
}

// Serialization a Signature
func (sig *Signature) Serialization(sink *common.ZeroCopySink) {
	sink.WriteUint32(sig.PeerIndex)
	sink.WriteVarBytes(sig.Sig)
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
func (pp *PrePrepareMsg) Deserialization(source *common.ZeroCopySource) (err error) {
	err = pp.Signature.Deserialization(source)
	if err != nil {
		return
	}
	view, eof := source.NextUint64()
	if eof {
		err = fmt.Errorf("PrePrepareMsg Deserialization unexpected eof reading view")
		return
	}
	n, eof := source.NextUint64()
	if eof {
		err = fmt.Errorf("PrePrepareMsg Deserialization unexpected eof reading n")
		return
	}
	clientMsgDigest, _, irregular, eof := source.NextString()
	if irregular {
		err = fmt.Errorf("PrePrepareMsg Deserialization irregular data reading clientMsgDigest")
		return
	}
	if eof {
		err = fmt.Errorf("PrePrepareMsg Deserialization unexpected eof reading clientMsgDigest")
		return
	}

	pp.View = view
	pp.N = n
	pp.ClientMsgDigest = clientMsgDigest
	return
}

// Serialization a PrePrepareMsg
func (pp *PrePrepareMsg) Serialization(sink *common.ZeroCopySink) {
	pp.Signature.Serialization(sink)
	sink.WriteUint64(pp.View)
	sink.WriteUint64(pp.N)
	sink.WriteString(pp.ClientMsgDigest)
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
func (ppp *PrePreparePiggybackedMsg) Deserialization(source *common.ZeroCopySource) (err error) {
	err = ppp.PrePrepareMsg.Deserialization(source)
	if err != nil {
		return
	}
	ppp.ClientMsg = NewClientMsgFunc()
	err = ppp.ClientMsg.Deserialization(source)
	return
}

// Serialization a PrePreparePiggybackedMsg
func (ppp *PrePreparePiggybackedMsg) Serialization(sink *common.ZeroCopySink) {
	ppp.PrePrepareMsg.Serialization(sink)
	ppp.ClientMsg.Serialization(sink)
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
func (p *PrepareMsg) Deserialization(source *common.ZeroCopySource) (err error) {
	err = p.Signature.Deserialization(source)
	if err != nil {
		return
	}
	view, eof := source.NextUint64()
	if eof {
		err = fmt.Errorf("PrepareMsg Deserialization unexpected eof reading view")
		return
	}
	n, eof := source.NextUint64()
	if eof {
		err = fmt.Errorf("PrepareMsg Deserialization unexpected eof reading n")
		return
	}
	clientMsgDigest, _, irregular, eof := source.NextString()
	if irregular {
		err = fmt.Errorf("PrepareMsg Deserialization irregular data reading clientMsgDigest")
		return
	}

	p.View = view
	p.N = n
	p.ClientMsgDigest = clientMsgDigest
	return
}

// Serialization a PrepareMsg
func (p *PrepareMsg) Serialization(sink *common.ZeroCopySink) {
	p.Signature.Serialization(sink)
	sink.WriteUint64(p.View)
	sink.WriteUint64(p.N)
	sink.WriteString(p.ClientMsgDigest)
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
func (c *CommitMsg) Deserialization(source *common.ZeroCopySource) (err error) {
	err = c.Signature.Deserialization(source)
	if err != nil {
		return
	}
	view, eof := source.NextUint64()
	if eof {
		err = fmt.Errorf("CommitMsg Deserialization unexpected eof reading view")
		return
	}
	n, eof := source.NextUint64()
	if eof {
		err = fmt.Errorf("CommitMsg Deserialization unexpected eof reading n")
		return
	}
	clientMsgDigest, _, irregular, eof := source.NextString()
	if irregular {
		err = fmt.Errorf("CommitMsg Deserialization irregular data reading clientMsgDigest")
		return
	}

	c.View = view
	c.N = n
	c.ClientMsgDigest = clientMsgDigest
	return
}

// Serialization a CommitMsg
func (c *CommitMsg) Serialization(sink *common.ZeroCopySink) {
	c.Signature.Serialization(sink)
	sink.WriteUint64(c.View)
	sink.WriteUint64(c.N)
	sink.WriteString(c.ClientMsgDigest)
}

// Prepared ...
type Prepared struct {
	PrepareMsgs   []PrepareMsg
	PrePrepareMsg PrePrepareMsg
}

// Deserialization a Prepared
func (pd *Prepared) Deserialization(source *common.ZeroCopySource) (err error) {
	size, eof := source.NextUint16()
	if eof {
		err = fmt.Errorf("Prepared Deserialization unexpected eof reading size")
		return
	}

	pd.PrepareMsgs = make([]PrepareMsg, size)
	for i := uint16(0); i < size; i++ {
		err = pd.PrepareMsgs[i].Deserialization(source)
		if err != nil {
			return
		}
	}

	err = pd.PrePrepareMsg.Deserialization(source)
	return
}

// Serialization a Prepared
func (pd *Prepared) Serialization(sink *common.ZeroCopySink) {
	sink.WriteUint16(uint16(len(pd.PrepareMsgs)))
	for _, pm := range pd.PrepareMsgs {
		pm.Serialization(sink)
	}
	pd.PrePrepareMsg.Serialization(sink)
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
func (vc *ViewChangeMsg) Deserialization(source *common.ZeroCopySource) (err error) {
	err = vc.Signature.Deserialization(source)
	if err != nil {
		return
	}
	view, eof := source.NextUint64()
	if eof {
		err = fmt.Errorf("ViewChangeMsg Deserialization unexpected eof reading view")
		return
	}
	n, eof := source.NextUint64()
	if eof {
		err = fmt.Errorf("ViewChangeMsg Deserialization unexpected eof reading n")
		return
	}
	size, eof := source.NextUint16()
	if eof {
		err = fmt.Errorf("ViewChangeMsg Deserialization unexpected eof reading size")
		return
	}
	p := make([]Prepared, size)
	for i := uint16(0); i < size; i++ {
		err = p[i].Deserialization(source)
		if err != nil {
			return
		}
	}

	vc.NewView = view
	vc.N = n
	vc.P = p
	return
}

// Serialization a ViewChangeMsg
func (vc *ViewChangeMsg) Serialization(sink *common.ZeroCopySink) {
	vc.Signature.Serialization(sink)
	sink.WriteUint64(vc.NewView)
	sink.WriteUint64(vc.N)
	sink.WriteUint16(uint16(len(vc.P)))
	for i := 0; i < len(vc.P); i++ {
		vc.P[i].Serialization(sink)
	}
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
