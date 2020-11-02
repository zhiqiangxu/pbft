package pbft

type msgPool struct {
	rounds map[uint64]*consensusRound // indexed by N
	pbft   *pbft
}

type roundMsg struct {
	clientMsg     ClientMsg
	prePrepareMsg *PrePrepareMsg
	prepareMsgs   []*PrePrepareMsg
	commitMsgs    []*CommitMsg
}

type consensusRound struct {
	v uint64
	n uint64
	roundMsg
}

func newMsgPool(pbft *pbft) *msgPool {
	return &msgPool{pbft: pbft, rounds: make(map[uint64]*consensusRound)}
}

func (pool *msgPool) GetClientMsgByDigest(digest string) (msg ClientMsg) {
	return
}

func (pool *msgPool) GetClientMsg(n uint64) (msg ClientMsg) {
	round := pool.rounds[n]
	if round == nil {
		return
	}
	msg = round.clientMsg
	return
}

func (pool *msgPool) HighestN() (uint64, bool) {
	return 0, false
}

func (pool *msgPool) GetCommitMsgs(n uint64) []*CommitMsg {
	return pool.rounds[n].commitMsgs
}

func (pool *msgPool) Sealed(n uint64) {
	delete(pool.rounds, n)
}

func (pool *msgPool) AddPrePrepareMsg(msg *PrePrepareMsg) {
}

func (pool *msgPool) AddPrepreparePiggybackedMsg(msg *PrePreparePiggybackedMsg) {
	pool.rounds[msg.N] = &consensusRound{v: msg.View, n: msg.N, roundMsg: roundMsg{clientMsg: msg.ClientMsg, prePrepareMsg: &msg.PrePrepareMsg}}
}

// AddPrepareMsg will add just enough msg to be prepared
func (pool *msgPool) AddPrepareMsg(msg *PrepareMsg) (added bool, prepared bool) {
	return
}

// AddCommitMsg will add just enough msg to be commit-local
func (pool *msgPool) AddCommitMsg(msg *CommitMsg) (added bool, commitLocal bool) {
	return
}

func (pool *msgPool) AddCheckpointMsg(msg *CheckpointMsg) (added bool, checkpointed bool) {
	return
}

// AddViewChangeMsg will add just enough msg to start new-view
func (pool *msgPool) AddViewChangeMsg(msg *ViewChangeMsg) (added bool, enough bool) {
	return
}

func (pool *msgPool) GetVO(newView uint64) (v []*ViewChangeMsg, o []*PrePrepareMsg) {
	return
}

func (pool *msgPool) AddNewViewMsg(msg *NewViewMsg) {

}
