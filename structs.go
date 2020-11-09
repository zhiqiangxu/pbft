package pbft

import (
	"fmt"

	"github.com/ontio/ontology/common"
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
	V       map[uint32] /*index*/ *ViewChangeMsg
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
func (nv *NewViewMsg) Deserialization(source *common.ZeroCopySource) (err error) {
	err = nv.Signature.Deserialization(source)
	if err != nil {
		return
	}

	newView, eof := source.NextUint64()
	if eof {
		err = fmt.Errorf("NewViewMsg Deserialization unexpected eof reading newView")
		return
	}
	nv.NewView = newView

	vsize, eof := source.NextUint16()
	if eof {
		err = fmt.Errorf("NewViewMsg Deserialization unexpected eof reading vsize")
		return
	}

	nv.V = make(map[uint32]*ViewChangeMsg)
	for i := uint16(0); i < vsize; i++ {
		index, eof := source.NextUint32()
		if eof {
			err = fmt.Errorf("NewViewMsg Deserialization unexpected eof reading index")
			return
		}
		vc := &ViewChangeMsg{}
		err = vc.Deserialization(source)
		if err != nil {
			return
		}
		nv.V[index] = vc
	}

	osize, eof := source.NextUint16()
	if eof {
		err = fmt.Errorf("NewViewMsg Deserialization unexpected eof reading osize")
		return
	}
	for i := uint16(0); i < osize; i++ {
		var ppm PrePrepareMsg
		err = ppm.Deserialization(source)
		if err != nil {
			return
		}

		nv.O = append(nv.O, &ppm)
	}
	return
}

// Serialization a NewViewMsg
func (nv *NewViewMsg) Serialization(sink *common.ZeroCopySink) {
	nv.Signature.Serialization(sink)
	sink.WriteUint64(nv.NewView)
	sink.WriteUint16(uint16(len(nv.V)))
	for index, vc := range nv.V {
		sink.WriteUint32(index)
		vc.Serialization(sink)
	}
	sink.WriteUint16(uint16(len(nv.O)))
	for _, ppm := range nv.O {
		ppm.Serialization(sink)
	}
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

// HeartBeatMsg ...
type HeartBeatMsg struct {
	Signature
	NextN      uint64
	Commits    []*CommitMsg
	Checkpoint *CheckpointMsg
}

// Type of msg
func (sync *HeartBeatMsg) Type() MessageType {
	return MessageTypeHeartBeat
}

// SignatureDigest of msg
func (sync *HeartBeatMsg) SignatureDigest() string {
	return ""
}

// Deserialization a HeartBeatMsg
func (sync *HeartBeatMsg) Deserialization(source *common.ZeroCopySource) error {
	return nil
}

// Serialization a HeartBeatMsg
func (sync *HeartBeatMsg) Serialization(sink *common.ZeroCopySink) {

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
	CommitMsgs map[uint32]*CommitMsg
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

// ViewChangeEvent ...
type ViewChangeEvent struct {
	NewView uint64
}

// Type of Event
func (event *ViewChangeEvent) Type() EventType {
	return EventTypeViewChange
}

// HeartBeatEvent ...
type HeartBeatEvent struct {
}

// Type of Event
func (event *HeartBeatEvent) Type() EventType {
	return EventTypeHeartBeat
}
