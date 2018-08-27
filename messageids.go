package magina

import (
	"sync"
)

// MessageIds is 16 bit message id as specified by the MQTT spec.
// In general, these values should not be depended upon by
// the client application.
type MessageIds struct {
	sync.RWMutex
	index map[uint16]Token
}

const (
	midMin uint16 = 1
	midMax uint16 = 65535
)

func (mids *MessageIds) freeID(id uint16) {
	mids.Lock()
	defer mids.Unlock()
	delete(mids.index, id)
}

func (mids *MessageIds) getID(t Token) uint16 {
	mids.Lock()
	defer mids.Unlock()
	for i := midMin; i < midMax; i++ {
		if _, ok := mids.index[i]; !ok {
			mids.index[i] = t
			return i
		}
	}
	return 0
}

func (mids *MessageIds) getToken(id uint16) Token {
	mids.RLock()
	defer mids.RUnlock()
	if token, ok := mids.index[id]; ok {
		return token
	}
	return nil
}
