package pbft

import (
	"github.com/ontio/ontology-crypto/keypair"
	oa "github.com/ontio/ontology/account"
	cmdcommon "github.com/ontio/ontology/cmd/common"
	"github.com/ontio/ontology/core/signature"
	"github.com/zhiqiangxu/util"
)

type account struct {
	oa           *oa.Account
	publicKeyStr []byte
}

// LoadAccount from wallet
func LoadAccount(path, address string, passwd []byte) (acc Account, err error) {
	wallet, err := oa.Open(path)
	if err != nil {
		return
	}

	defer clearPasswd(passwd)

	oaAccount, err := cmdcommon.GetAccountMulti(wallet, passwd, address)
	if err != nil {
		return
	}

	acc = toAccount(oaAccount)

	return
}

// RandAccount creates a new account randomly
func RandAccount(encrypt string) (acc Account) {
	oaAccount := oa.NewAccount(encrypt)
	acc = toAccount(oaAccount)
	return
}

func toAccount(oa *oa.Account) Account {

	acc := &account{oa: oa}
	acc.publicKeyStr = keypair.SerializePublicKey(oa.PublicKey)
	return acc
}

func clearPasswd(passwd []byte) {
	size := len(passwd)
	for i := 0; i < size; i++ {
		passwd[i] = 0
	}
}

func (acc *account) Sign(data []byte) (sig []byte, err error) {
	sig, err = signature.Sign(acc.oa, data)
	return
}

func (acc *account) PublicKey() Pubkey {
	return Pubkey(util.String(acc.publicKeyStr))
}
