package pbft

import (
	"crypto"
	"time"
)

type (

	// DBConfig for DB
	DBConfig struct {
		DataDir string
	}

	// IOConfig for I/O
	IOConfig struct {
		Timeout time.Duration
	}

	// NodeConfig for pbft nodes
	NodeConfig struct {
		Nodes       []NodePublicInfo
		Index       int
		View        int64
		PrivateInfo NodePrivateInfo
	}

	// NodePublicInfo for node public info
	NodePublicInfo struct {
		Addr   string
		Pubkey crypto.PublicKey
	}

	// NodePrivateInfo for node private info
	NodePrivateInfo struct {
		PriKey     crypto.PrivateKey
		ListenAddr string
	}

	// Config for pbft
	Config struct {
		DBConf     DBConfig
		IOConf     IOConfig
		NodeConfig NodeConfig
	}
)
