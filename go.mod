module github.com/textileio/go-threads

go 1.15

require (
	github.com/alecthomas/jsonschema v0.0.0-20191017121752-4bb6e3fae4f2
	github.com/anytypeio/go-ds-badger3 v0.3.1-0.20221103102622-3233d4e13cb8
	github.com/dgraph-io/badger/v3 v3.2103.3
	github.com/dgtony/collections v0.1.6
	github.com/dlclark/regexp2 v1.2.0 // indirect
	github.com/dop251/goja v0.0.0-20200721192441-a695b0cdd498
	github.com/evanphx/json-patch v4.5.0+incompatible
	github.com/fsnotify/fsnotify v1.5.4
	github.com/go-sourcemap/sourcemap v2.1.3+incompatible // indirect
	github.com/gogo/googleapis v1.3.1 // indirect
	github.com/gogo/protobuf v1.3.2
	github.com/gogo/status v1.1.0
	github.com/golang-jwt/jwt v3.2.2+incompatible
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0
	github.com/hashicorp/go-multierror v1.1.1
	github.com/hashicorp/golang-lru v0.5.5-0.20210104140557-80c98217689d
	github.com/hsanjuan/ipfs-lite v1.4.2
	github.com/improbable-eng/grpc-web v0.14.1
	github.com/ipfs/go-block-format v0.0.3
	github.com/ipfs/go-blockservice v0.4.0
	github.com/ipfs/go-cid v0.3.2
	github.com/ipfs/go-datastore v0.6.0
	github.com/ipfs/go-ipfs-blockstore v1.2.0
	github.com/ipfs/go-ipfs-exchange-offline v0.3.0
	github.com/ipfs/go-ipfs-util v0.0.2
	github.com/ipfs/go-ipld-cbor v0.0.6
	github.com/ipfs/go-ipld-format v0.4.0
	github.com/ipfs/go-log/v2 v2.5.1
	github.com/ipfs/go-merkledag v0.7.0
	github.com/jbenet/goprocess v0.1.4
	github.com/libp2p/go-libp2p v0.22.0
	github.com/libp2p/go-libp2p-core v0.20.1 // indirect
	github.com/libp2p/go-libp2p-gostream v0.5.0
	github.com/libp2p/go-libp2p-pubsub v0.8.0
	github.com/multiformats/go-multiaddr v0.6.0
	github.com/multiformats/go-multiaddr-fmt v0.1.0
	github.com/multiformats/go-multibase v0.1.1
	github.com/multiformats/go-multihash v0.2.1
	github.com/multiformats/go-varint v0.0.6
	github.com/namsral/flag v1.7.4-pre
	github.com/oklog/ulid/v2 v2.0.2
	github.com/phayes/freeport v0.0.0-20180830031419-95f893ade6f2
	github.com/prometheus/client_golang v1.12.1
	github.com/textileio/crypto v0.0.0-20210928200545-9b5a55171e1b
	github.com/textileio/go-datastore-extensions v1.1.0
	github.com/textileio/go-libp2p-pubsub-rpc v0.0.9
	github.com/tidwall/gjson v1.10.2
	github.com/tidwall/sjson v1.2.3
	github.com/whyrusleeping/base32 v0.0.0-20170828182744-c30ac30633cc
	github.com/xeipuuv/gojsonpointer v0.0.0-20190905194746-02993c407bfb // indirect
	github.com/xeipuuv/gojsonschema v1.2.0
	go.mongodb.org/mongo-driver v1.10.3
	go.uber.org/zap v1.22.0
	golang.org/x/exp v0.0.0-20200331195152-e8c3332aa8e5
	golang.org/x/sync v0.0.0-20220722155255-886fb9371eb4
	google.golang.org/grpc v1.40.0
	google.golang.org/protobuf v1.28.1
	nhooyr.io/websocket v1.8.7 // indirect
)

replace github.com/dgraph-io/badger/v3 => github.com/anytypeio/badger/v3 v3.0.0-20220504124523-ca79ca5ff94d
