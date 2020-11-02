package pbft

import (
	"context"
	"fmt"
	"sync"
)

type msgSyncer struct {
	sync.RWMutex
	nextID uint64
	respC  map[uint64]chan Msg
	bft    *pbft
}

func newMsgSyncer(bft *pbft) *msgSyncer {
	return &msgSyncer{bft: bft, respC: make(map[uint64]chan Msg)}
}

func (syncer *msgSyncer) onSyncResp(ctx context.Context, msg Msg) (err error) {
	switch msg.Type() {
	case MessageTypeSyncClientMessageResp:
		reqID := msg.(*SyncClientMessageResp).ReqID
		syncer.RLock()
		ch := syncer.respC[reqID]
		syncer.RUnlock()

		if ch == nil {
			err = fmt.Errorf("dangling resp founded, reqID=%v type=%v", reqID, msg.Type())
			return
		}
		select {
		case ch <- msg:
		case <-ctx.Done():
			err = ctx.Err()
		}
	case MessageTypeSyncSealedClientMessageResp:
		reqID := msg.(*SyncSealedClientMessageResp).ReqID
		syncer.RLock()
		ch := syncer.respC[reqID]
		syncer.RUnlock()

		if ch == nil {
			err = fmt.Errorf("dangling resp founded, reqID=%v type=%v", reqID, msg.Type())
			return
		}
		select {
		case ch <- msg:
		case <-ctx.Done():
			err = ctx.Err()
		}
	}
	return
}

func (syncer *msgSyncer) SyncClientMsg(ctx context.Context, n uint64, clientMsgDigest string) (resp *SyncClientMessageResp, err error) {

	msg, err := syncer.bft.constructSyncClientMsgReq(n, clientMsgDigest)
	if err != nil {
		return
	}
	ch := make(chan Msg, 1)

	syncer.Lock()

	msg.ReqID = syncer.nextID
	syncer.nextID++
	syncer.respC[msg.ReqID] = ch

	syncer.Unlock()

	syncer.bft.net.Broadcast(msg)

	select {
	case <-ctx.Done():
		err = ctx.Err()
	case respMsg := <-ch:
		resp = respMsg.(*SyncClientMessageResp)
	}

	syncer.Lock()
	delete(syncer.respC, msg.ReqID)
	syncer.Unlock()
	return
}

func (syncer *msgSyncer) SyncSealedClientMsg(ctx context.Context, n uint64) (resp *SyncSealedClientMessageResp, err error) {
	msg, err := syncer.bft.constructSyncSealedClientMsgReq(n)
	if err != nil {
		return
	}
	ch := make(chan Msg, 1)

	syncer.Lock()

	msg.ReqID = syncer.nextID
	syncer.nextID++

	syncer.respC[msg.ReqID] = ch

	syncer.Unlock()

	syncer.bft.net.Broadcast(msg)

	select {
	case <-ctx.Done():
		err = ctx.Err()
	case respMsg := <-ch:
		resp = respMsg.(*SyncSealedClientMessageResp)
	}

	syncer.Lock()
	delete(syncer.respC, msg.ReqID)
	syncer.Unlock()
	return
}
