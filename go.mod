module github.com/textileio/go-threads

go 1.14

require (
	// agl/ed25519 only used in tests for backward compatibility, *do not* use in production code.
	github.com/agl/ed25519 v0.0.0-20170116200512-5312a6153412
	github.com/alecthomas/jsonschema v0.0.0-20191017121752-4bb6e3fae4f2
	github.com/btcsuite/btcd v0.21.0-beta // indirect
	github.com/davidlazar/go-crypto v0.0.0-20200604182044-b73af7476f6c // indirect
	github.com/desertbit/timer v0.0.0-20180107155436-c41aec40b27f // indirect
	github.com/dgraph-io/badger v1.6.2
	github.com/dgraph-io/ristretto v0.0.3 // indirect
	github.com/dgrijalva/jwt-go v3.2.0+incompatible
	github.com/dlclark/regexp2 v1.2.0 // indirect
	github.com/dop251/goja v0.0.0-20200721192441-a695b0cdd498
	github.com/evanphx/json-patch v4.5.0+incompatible
	github.com/fd/go-nat v1.0.0 // indirect
	github.com/fsnotify/fsnotify v1.4.7
	github.com/go-sourcemap/sourcemap v2.1.3+incompatible // indirect
	github.com/gogo/googleapis v1.3.1 // indirect
	github.com/gogo/protobuf v1.3.1
	github.com/gogo/status v1.1.0
	github.com/golang/protobuf v1.4.3
	github.com/google/gopacket v1.1.19 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.2.1
	github.com/gxed/pubsub v0.0.0-20180201040156-26ebdf44f824 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-multierror v1.1.0
	github.com/hashicorp/golang-lru v0.5.4
	github.com/hsanjuan/ipfs-lite v1.1.17
	github.com/improbable-eng/grpc-web v0.13.0
	github.com/ipfs/go-bitswap v0.3.3 // indirect
	github.com/ipfs/go-block-format v0.0.2
	github.com/ipfs/go-blockservice v0.1.4
	github.com/ipfs/go-cid v0.0.7
	github.com/ipfs/go-datastore v0.4.5
	github.com/ipfs/go-ds-badger v0.2.6
	github.com/ipfs/go-ipfs-blockstore v1.0.3
	github.com/ipfs/go-ipfs-exchange-offline v0.0.1
	github.com/ipfs/go-ipfs-flags v0.0.1 // indirect
	github.com/ipfs/go-ipfs-util v0.0.2
	github.com/ipfs/go-ipld-cbor v0.0.5
	github.com/ipfs/go-ipld-format v0.2.0
	github.com/ipfs/go-log v1.0.4
	github.com/ipfs/go-log/v2 v2.1.1
	github.com/ipfs/go-merkledag v0.3.2
	github.com/koron/go-ssdp v0.0.2 // indirect
	github.com/libp2p/go-libp2p v0.12.0
	github.com/libp2p/go-libp2p-asn-util v0.0.0-20201026210036-4f868c957324 // indirect
	github.com/libp2p/go-libp2p-connmgr v0.2.4
	github.com/libp2p/go-libp2p-core v0.7.0
	github.com/libp2p/go-libp2p-crypto v0.1.0 // indirect
	github.com/libp2p/go-libp2p-gostream v0.3.0
	github.com/libp2p/go-libp2p-kad-dht v0.11.0 // indirect
	github.com/libp2p/go-libp2p-noise v0.1.2 // indirect
	github.com/libp2p/go-libp2p-peer v0.2.0 // indirect
	github.com/libp2p/go-libp2p-peerstore v0.2.6
	github.com/libp2p/go-libp2p-pubsub v0.4.0
	github.com/libp2p/go-libp2p-quic-transport v0.5.0 // indirect
	github.com/libp2p/go-libp2p-swarm v0.3.1
	github.com/libp2p/go-libp2p-yamux v0.4.1 // indirect
	github.com/libp2p/go-netroute v0.1.4 // indirect
	github.com/libp2p/go-sockaddr v0.1.0 // indirect
	github.com/multiformats/go-multiaddr v0.3.1
	github.com/multiformats/go-multibase v0.0.3
	github.com/multiformats/go-multihash v0.0.14
	github.com/multiformats/go-varint v0.0.6
	github.com/mwitkow/go-conntrack v0.0.0-20190716064945-2f068394615f // indirect
	github.com/namsral/flag v1.7.4-pre
	github.com/oklog/ulid/v2 v2.0.2
	github.com/phayes/freeport v0.0.0-20180830031419-95f893ade6f2
	github.com/polydawn/refmt v0.0.0-20190807091052-3d65705ee9f1 // indirect
	github.com/rs/cors v1.7.0 // indirect
	github.com/smartystreets/goconvey v0.0.0-20190710185942-9d28bd7c0945 // indirect
	github.com/textileio/go-datastore v0.4.5-0.20200819232101-baa577bf9422
	github.com/textileio/go-ds-badger v0.2.5-0.20200819232634-de89720b5d6a
	github.com/tidwall/gjson v1.3.5
	github.com/tidwall/sjson v1.0.4
	github.com/wangjia184/sortedset v0.0.0-20160527075905-f5d03557ba30 // indirect
	github.com/whyrusleeping/base32 v0.0.0-20170828182744-c30ac30633cc
	github.com/whyrusleeping/cbor-gen v0.0.0-20200826160007-0b9f6c5fb163 // indirect
	github.com/whyrusleeping/go-smux-multiplex v3.0.16+incompatible // indirect
	github.com/whyrusleeping/go-smux-multistream v2.0.2+incompatible // indirect
	github.com/whyrusleeping/go-smux-yamux v2.0.9+incompatible // indirect
	github.com/whyrusleeping/yamux v1.1.5 // indirect
	github.com/xeipuuv/gojsonpointer v0.0.0-20190905194746-02993c407bfb // indirect
	github.com/xeipuuv/gojsonschema v1.2.0
	go.opencensus.io v0.22.5 // indirect
	go.uber.org/multierr v1.6.0 // indirect
	go.uber.org/zap v1.16.0
	golang.org/x/crypto v0.0.0-20201203163018-be400aefbc4c
	golang.org/x/exp v0.0.0-20200331195152-e8c3332aa8e5
	golang.org/x/net v0.0.0-20201202161906-c7110b5ffcbb // indirect
	golang.org/x/sync v0.0.0-20200317015054-43a5402ce75a
	golang.org/x/sys v0.0.0-20201204225414-ed752295db88 // indirect
	golang.org/x/text v0.3.4 // indirect
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
	google.golang.org/grpc v1.31.1
	google.golang.org/protobuf v1.25.0
)
