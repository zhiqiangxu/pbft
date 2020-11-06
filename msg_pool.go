package pbft

type msgPool struct {
	rounds         map[uint64] /*N*/ *consensusRound
	viewChangeMsgs map[uint64] /*view*/ map[uint32] /*index*/ *ViewChangeMsg
	digest2N       map[string]uint64
	highestN       uint64
	pbft           *pbft
}

type roundMsg struct {
	clientMsg      ClientMsg
	prePrepareMsg  *PrePrepareMsg
	prepareMsgs    map[uint32]*PrepareMsg
	commitMsgs     map[uint32]*CommitMsg
	checkpointMsgs map[uint32]*CheckpointMsg
}

func emptyRoundMsg() roundMsg {
	return roundMsg{
		prepareMsgs:    make(map[uint32]*PrepareMsg),
		commitMsgs:     make(map[uint32]*CommitMsg),
		checkpointMsgs: make(map[uint32]*CheckpointMsg),
	}
}

type consensusRound struct {
	v uint64
	n uint64
	roundMsg
}

func newMsgPool(pbft *pbft) *msgPool {
	return &msgPool{
		pbft:           pbft,
		rounds:         make(map[uint64]*consensusRound),
		viewChangeMsgs: make(map[uint64]map[uint32]*ViewChangeMsg),
		digest2N:       make(map[string]uint64),
	}
}

func (pool *msgPool) GetClientMsgByDigest(digest string) (msg ClientMsg) {
	n, exists := pool.digest2N[digest]
	if !exists {
		return
	}

	round, exists := pool.rounds[n]
	if !exists {
		return
	}
	msg = round.clientMsg
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
	return pool.highestN, len(pool.rounds) > 0
}

func (pool *msgPool) GetCommitMsgs(n uint64) map[uint32]*CommitMsg {
	round := pool.rounds[n]
	if round == nil {
		return nil
	}
	return round.commitMsgs
}

func (pool *msgPool) Sealed(n uint64) {
	delete(pool.rounds, n)
}

func (pool *msgPool) AddPrePrepareMsg(msg *PrePrepareMsg) {
	if pool.highestN < msg.N {
		pool.highestN = msg.N
	}

	pool.digest2N[msg.ClientMsgDigest] = msg.N
	round := pool.rounds[msg.N]
	if round == nil {
		round = &consensusRound{v: msg.View, n: msg.N, roundMsg: emptyRoundMsg()}
		round.roundMsg.prePrepareMsg = msg
		pool.rounds[msg.N] = round
	} else {
		round.prePrepareMsg = msg
	}
}

func (pool *msgPool) AddPrepreparePiggybackedMsg(msg *PrePreparePiggybackedMsg) {
	if pool.highestN < msg.N {
		pool.highestN = msg.N
	}

	pool.digest2N[msg.ClientMsgDigest] = msg.N

	round := pool.rounds[msg.N]
	if round == nil {
		round = &consensusRound{v: msg.View, n: msg.N, roundMsg: emptyRoundMsg()}
		pool.rounds[msg.N] = round
		round.roundMsg.clientMsg = msg.ClientMsg
		round.roundMsg.prePrepareMsg = &msg.PrePrepareMsg
	} else {
		round.clientMsg = msg.ClientMsg
		round.prePrepareMsg = &msg.PrePrepareMsg
	}
}

// AddPrepareMsg will add just enough msg to be prepared
func (pool *msgPool) AddPrepareMsg(msg *PrepareMsg) (added bool, prepared bool) {
	round := pool.rounds[msg.N]
	if round == nil {
		round = &consensusRound{v: msg.View, n: msg.N, roundMsg: emptyRoundMsg()}
		pool.rounds[msg.N] = round
	}

	added = len(round.prepareMsgs)+1 < pool.pbft.quorum()
	if !added {
		return
	}
	round.prepareMsgs[msg.PeerIndex] = msg

	prepared = round.prePrepareMsg != nil && len(round.prepareMsgs)+1 == pool.pbft.quorum()
	return
}

// AddCommitMsg will add just enough msg to be commit-local
func (pool *msgPool) AddCommitMsg(msg *CommitMsg) (added bool, commitLocal bool) {
	round := pool.rounds[msg.N]
	if round == nil {
		round = &consensusRound{v: msg.View, n: msg.N, roundMsg: emptyRoundMsg()}
		pool.rounds[msg.N] = round
	}

	quorum := pool.pbft.quorum()
	added = len(round.commitMsgs) < quorum
	if !added {
		return
	}
	round.commitMsgs[msg.PeerIndex] = msg

	commitLocal = round.prePrepareMsg != nil && len(round.prepareMsgs)+1 == quorum && len(round.commitMsgs) == quorum
	return
}

func (pool *msgPool) IsCommitLocal(n uint64, quorum int) bool {
	round := pool.rounds[n]
	if round == nil {
		return false
	}

	if quorum <= 0 {
		quorum = pool.pbft.quorum()
	}

	return round.prePrepareMsg != nil && len(round.prepareMsgs)+1 == quorum && len(round.commitMsgs) == quorum
}

func (pool *msgPool) IsPrepared(n uint64, quorum int) bool {
	round := pool.rounds[n]
	if round == nil {
		return false
	}

	if quorum <= 0 {
		quorum = pool.pbft.quorum()
	}

	return round.prePrepareMsg != nil && len(round.prepareMsgs)+1 == quorum
}

func (pool *msgPool) AddCheckpointMsg(msg *CheckpointMsg) (added bool, checkpointed bool, checkpointMsgs map[uint32]*CheckpointMsg) {
	round := pool.rounds[msg.N]
	if round == nil {
		return
	}

	quorum := pool.pbft.quorum()
	added = len(round.checkpointMsgs) < quorum && round.checkpointMsgs[msg.PeerIndex] == nil
	if !added {
		return
	}
	round.checkpointMsgs[msg.PeerIndex] = msg

	checkpointed = len(round.checkpointMsgs) == quorum

	if checkpointed {
		checkpointMsgs = round.checkpointMsgs
	}
	return
}

// AddViewChangeMsg will add just enough msg to start new-view
func (pool *msgPool) AddViewChangeMsg(msg *ViewChangeMsg) (added bool, enough bool) {
	msgsOfView := pool.viewChangeMsgs[msg.NewView]
	if msgsOfView == nil {
		msgsOfView = make(map[uint32]*ViewChangeMsg)
		pool.viewChangeMsgs[msg.NewView] = msgsOfView
	}

	quorum := pool.pbft.quorum()
	added = len(msgsOfView)+1 < quorum && msgsOfView[msg.PeerIndex] == nil
	if !added {
		return
	}

	msgsOfView[msg.PeerIndex] = msg

	enough = len(msgsOfView)+1 == quorum
	return
}

func (pool *msgPool) GetVO(newView uint64) (v map[uint32] /*index*/ *ViewChangeMsg, o []*PrePrepareMsg) {
	v = pool.viewChangeMsgs[newView]
	return
}

func (pool *msgPool) GetPrepared() []Prepared {
	return nil
}

func (pool *msgPool) AddNewViewMsg(msg *NewViewMsg) {

}
