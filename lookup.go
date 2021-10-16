package dht

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/routing"

	kb "github.com/libp2p/go-libp2p-kbucket"
)

// GetClosestPeers is a Kademlia 'node lookup' operation. Returns a channel of
// the K closest peers to the given key.
//
// If the context is canceled, this function will return the context error along
// with the closest K peers it has found so far.
func (dht *IpfsDHT) GetClosestPeers(ctx context.Context, key string) ([]peer.ID, error) {
	if key == "" {
		return nil, fmt.Errorf("can't lookup empty key")
	}
	//TODO: I can break the interface! return []peer.ID
	lookupRes, err := dht.runLookupWithFollowup(ctx, key,
		func(ctx context.Context, p peer.ID) ([]*peer.AddrInfo, error) {
			// For DHT query command
			routing.PublishQueryEvent(ctx, &routing.QueryEvent{
				Type: routing.SendingQuery,
				ID:   p,
			})

			peers, err := dht.protoMessenger.GetClosestPeers(ctx, p, peer.ID(key))
			if err != nil {
				logger.Debugf("error getting closer peers: %s", err)
				return nil, err
			}
			logger.Info("Contacted peer " + p.Pretty() + " to get closest peers to key " + key + " peer ID from key is " + peer.ID(key).Pretty() + " received " + strconv.Itoa(len(peers)))

			// For DHT query command
			routing.PublishQueryEvent(ctx, &routing.QueryEvent{
				Type:      routing.PeerResponse,
				ID:        p,
				Responses: peers,
			})

			return peers, err
		},
		func() bool { return false },
	)

	if err != nil {
		return nil, err
	}

	if ctx.Err() == nil && lookupRes.completed {
		// refresh the cpl for this key as the query was successful
		dht.routingTable.ResetCplRefreshedAtForID(kb.ConvertKey(key), time.Now())
	}

	return lookupRes.peers, ctx.Err()
}

func (dht *IpfsDHT) GetClosestPeersMod(ctx context.Context, key string, uid uuid.UUID) ([]peer.ID, error) {
	if key == "" {
		return nil, fmt.Errorf("can't lookup empty key")
	}
	//TODO: I can break the interface! return []peer.ID
	lookupRes, err := dht.runLookupWithFollowup(ctx, key,
		func(ctx context.Context, p peer.ID) ([]*peer.AddrInfo, error) {
			// For DHT query command
			routing.PublishQueryEvent(ctx, &routing.QueryEvent{
				Type: routing.SendingQuery,
				ID:   p,
			})

			peers, err := dht.protoMessenger.GetClosestPeersMod(ctx, p, peer.ID(key), uid)
			if err != nil {
				logger.Debugf("error getting closer peers: %s", err)
				return nil, err
			}
			logger.Info("ID: " + uid.String() + " Contacted peer " + p.Pretty() + " to get key " + key + " peer ID from key is " + peer.ID(key).Pretty() + " received " + strconv.Itoa(len(peers)))

			// For DHT query command
			routing.PublishQueryEvent(ctx, &routing.QueryEvent{
				Type:      routing.PeerResponse,
				ID:        p,
				Responses: peers,
			})

			return peers, err
		},
		func() bool { return false },
	)

	if err != nil {
		return nil, err
	}

	if ctx.Err() == nil && lookupRes.completed {
		// refresh the cpl for this key as the query was successful
		dht.routingTable.ResetCplRefreshedAtForID(kb.ConvertKey(key), time.Now())
	}

	return lookupRes.peers, ctx.Err()
}
