package mongods

import "time"

var (
	defaultConfig = config{
		opTimeout:  30 * time.Second,
		txnTimeout: 30 * time.Second,
		collName:   "kvstore",
	}
)

type config struct {
	opTimeout  time.Duration
	txnTimeout time.Duration
	collName   string
}

type Option func(*config)

func WithOpTimeout(d time.Duration) Option {
	return func(c *config) {
		c.opTimeout = d
	}
}

func WithTxnTimeout(d time.Duration) Option {
	return func(c *config) {
		c.txnTimeout = d
	}
}

func WithCollName(collName string) Option {
	return func(c *config) {
		c.collName = collName
	}
}
