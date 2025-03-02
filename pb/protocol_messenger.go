package dht_pb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/routing"

	logging "github.com/ipfs/go-log"
	record "github.com/libp2p/go-libp2p-record"
	recpb "github.com/libp2p/go-libp2p-record/pb"
	"github.com/multiformats/go-multihash"

	"github.com/peter-tmc/go-libp2p-kad-dht/internal"
)

var logger = logging.Logger("dht")

// ProtocolMessenger can be used for sending DHT messages to peers and processing their responses.
// This decouples the wire protocol format from both the DHT protocol implementation and from the implementation of the
// routing.Routing interface.
//
// Note: the ProtocolMessenger's MessageSender still needs to deal with some wire protocol details such as using
// varint-delineated protobufs
type ProtocolMessenger struct {
	m         MessageSender
	validator record.Validator
}

type ProtocolMessengerOption func(*ProtocolMessenger) error

func WithValidator(validator record.Validator) ProtocolMessengerOption {
	return func(messenger *ProtocolMessenger) error {
		messenger.validator = validator
		return nil
	}
}

// NewProtocolMessenger creates a new ProtocolMessenger that is used for sending DHT messages to peers and processing
// their responses.
func NewProtocolMessenger(msgSender MessageSender, opts ...ProtocolMessengerOption) (*ProtocolMessenger, error) {
	pm := &ProtocolMessenger{
		m: msgSender,
	}

	for _, o := range opts {
		if err := o(pm); err != nil {
			return nil, err
		}
	}

	return pm, nil
}

// MessageSender handles sending wire protocol messages to a given peer
type MessageSender interface {
	// SendRequest sends a peer a message and waits for its response
	SendRequest(ctx context.Context, p peer.ID, pmes *Message) (*Message, error)
	// SendMessage sends a peer a message without waiting on a response
	SendMessage(ctx context.Context, p peer.ID, pmes *Message) error
}

// PutValue asks a peer to store the given key/value pair.
func (pm *ProtocolMessenger) PutValue(ctx context.Context, p peer.ID, rec *recpb.Record) error {
	pmes := NewMessage(Message_PUT_VALUE, rec.Key, 0)
	pmes.Record = rec
	rpmes, err := pm.m.SendRequest(ctx, p, pmes)
	if err != nil {
		logger.Debugw("failed to put value to peer", "to", p, "key", internal.LoggableRecordKeyBytes(rec.Key), "error", err)
		return err
	}

	if !bytes.Equal(rpmes.GetRecord().Value, pmes.GetRecord().Value) {
		const errStr = "value not put correctly"
		logger.Infow(errStr, "put-message", pmes, "get-message", rpmes)
		return errors.New(errStr)
	}

	return nil
}

// GetValue asks a peer for the value corresponding to the given key. Also returns the K closest peers to the key
// as described in GetClosestPeers.
func (pm *ProtocolMessenger) GetValue(ctx context.Context, p peer.ID, key string) (*recpb.Record, []*peer.AddrInfo, error) {
	pmes := NewMessage(Message_GET_VALUE, []byte(key), 0)
	respMsg, err := pm.m.SendRequest(ctx, p, pmes)
	if err != nil {
		return nil, nil, err
	}

	// Perhaps we were given closer peers
	peers := PBPeersToPeerInfos(respMsg.GetCloserPeers())

	if rec := respMsg.GetRecord(); rec != nil {
		// Success! We were given the value
		logger.Debug("got value")

		// make sure record is valid.
		err = pm.validator.Validate(string(rec.GetKey()), rec.GetValue())
		if err != nil {
			logger.Debug("received invalid record (discarded)")
			// return a sentinel to signify an invalid record was received
			return nil, peers, internal.ErrInvalidRecord
		}
		return rec, peers, err
	}

	if len(peers) > 0 {
		return nil, peers, nil
	}

	return nil, nil, routing.ErrNotFound
}

// GetClosestPeers asks a peer to return the K (a DHT-wide parameter) DHT server peers closest in XOR space to the id
// Note: If the peer happens to know another peer whose peerID exactly matches the given id it will return that peer
// even if that peer is not a DHT server node.
func (pm *ProtocolMessenger) GetClosestPeers(ctx context.Context, p peer.ID, id peer.ID) ([]*peer.AddrInfo, error) {
	pmes := NewMessage(Message_FIND_NODE, []byte(id), 0)
	respMsg, err := pm.m.SendRequest(ctx, p, pmes)
	if err != nil {
		return nil, err
	}
	peers := PBPeersToPeerInfos(respMsg.GetCloserPeers())
	return peers, nil
}

func (pm *ProtocolMessenger) GetClosestPeersMod(ctx context.Context, p peer.ID, id peer.ID, uid uuid.UUID) ([]*peer.AddrInfo, error) {
	pmes := NewMessage(Message_FIND_NODE, []byte(id), 0)
	uidmes := uuid.New()
	t1 := time.Now()
	logger.Info("Sent getClosestPeers message to " + p.Pretty() + " at time " + t1.UTC().String() + " for peer " + id.Pretty() + " request ID is " + uid.String() + " message uid is " + uidmes.String())
	respMsg, err := pm.m.SendRequest(ctx, p, pmes)
	t2 := time.Now()
	logger.Info("Received response to getClosestPeers from peer " + p.Pretty() + " took " + t2.Sub(t1).String() + " at time " + t2.UTC().String() + " request ID is " + uid.String() + " message uid is " + uidmes.String())
	if err != nil {
		return nil, err
	}
	peers := PBPeersToPeerInfos(respMsg.GetCloserPeers())
	return peers, nil
}

// PutProvider asks a peer to store that we are a provider for the given key.
func (pm *ProtocolMessenger) PutProvider(ctx context.Context, p peer.ID, key multihash.Multihash, host host.Host) error {
	pi := peer.AddrInfo{
		ID:    host.ID(),
		Addrs: host.Addrs(),
	}

	// TODO: We may want to limit the type of addresses in our provider records
	// For example, in a WAN-only DHT prohibit sharing non-WAN addresses (e.g. 192.168.0.100)
	if len(pi.Addrs) < 1 {
		return fmt.Errorf("no known addresses for self, cannot put provider")
	}

	pmes := NewMessage(Message_ADD_PROVIDER, key, 0)
	pmes.ProviderPeers = RawPeerInfosToPBPeers([]peer.AddrInfo{pi})
	logger.Info("Asked peer " + p.Pretty() + " to store that we are provider for the key " + key.B58String())
	return pm.m.SendMessage(ctx, p, pmes)
}

// GetProviders asks a peer for the providers it knows of for a given key. Also returns the K closest peers to the key
// as described in GetClosestPeers.
func (pm *ProtocolMessenger) GetProviders(ctx context.Context, p peer.ID, key multihash.Multihash) ([]*peer.AddrInfo, []*peer.AddrInfo, error) {
	pmes := NewMessage(Message_GET_PROVIDERS, key, 0)
	respMsg, err := pm.m.SendRequest(ctx, p, pmes)
	if err != nil {
		return nil, nil, err
	}
	provs := PBPeersToPeerInfos(respMsg.GetProviderPeers())
	closerPeers := PBPeersToPeerInfos(respMsg.GetCloserPeers())
	msg := " "
	if provs != nil {
		msg = msg + "providers:"
		for _, v := range provs {
			msg = msg + " " + v.String()
		}
		msg = msg + " "
	} else {
		msg = msg + " providers returned null"
	}
	//logger.Info("Sent GetProviders to " + p.Pretty() + msg + " got " + strconv.Itoa(len(closerPeers)) + " closer peers")
	return provs, closerPeers, nil
}

func (pm *ProtocolMessenger) GetProvidersMod(ctx context.Context, p peer.ID, key multihash.Multihash, uid uuid.UUID) ([]*peer.AddrInfo, []*peer.AddrInfo, error) {
	pmes := NewMessage(Message_GET_PROVIDERS, key, 0)
	uidmes := uuid.New()
	t1 := time.Now()
	logger.Info("Sent getProviders message to " + p.Pretty() + " at time " + t1.UTC().String() + " for key " + key.B58String() + " request ID is " + uid.String() + " message uid is " + uidmes.String())
	respMsg, err := pm.m.SendRequest(ctx, p, pmes) //TODO posso meter um log com mandei msg ao peer a certas horas e depois quando se recebe a resposta dar log disso e assim ja da para calcular a latencia
	t2 := time.Now()
	logger.Info("Received response to getProviders from peer " + p.Pretty() + " took " + t2.Sub(t1).String() + " at time " + t2.UTC().String() + " request ID is " + uid.String() + " message uid is " + uidmes.String())
	if err != nil {
		return nil, nil, err
	}
	provs := PBPeersToPeerInfos(respMsg.GetProviderPeers())
	closerPeers := PBPeersToPeerInfos(respMsg.GetCloserPeers())
	msg := " "
	if provs != nil {
		msg = msg + "providers:"
		for _, v := range provs {
			msg = msg + " " + v.String()
		}
		msg = msg + " "
	} else {
		msg = msg + " providers returned null"
	}
	//logger.Info("Sent GetProviders to " + p.Pretty() + msg + " got " + strconv.Itoa(len(closerPeers)) + " closer peers")
	return provs, closerPeers, nil
}

// Ping sends a ping message to the passed peer and waits for a response.
func (pm *ProtocolMessenger) Ping(ctx context.Context, p peer.ID) error {
	req := NewMessage(Message_PING, nil, 0)
	resp, err := pm.m.SendRequest(ctx, p, req)
	if err != nil {
		return fmt.Errorf("sending request: %w", err)
	}
	if resp.Type != Message_PING {
		return fmt.Errorf("got unexpected response type: %v", resp.Type)
	}
	return nil
}
