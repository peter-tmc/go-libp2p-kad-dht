package fullrt

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/multiformats/go-base32"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"

	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"
	"github.com/libp2p/go-libp2p-core/protocol"
	"github.com/libp2p/go-libp2p-core/routing"

	"github.com/gogo/protobuf/proto"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	u "github.com/ipfs/go-ipfs-util"
	logging "github.com/ipfs/go-log"

	kaddht "github.com/peter-tmc/go-libp2p-kad-dht"
	"github.com/peter-tmc/go-libp2p-kad-dht/crawler"
	"github.com/peter-tmc/go-libp2p-kad-dht/internal"
	internalConfig "github.com/peter-tmc/go-libp2p-kad-dht/internal/config"
	"github.com/peter-tmc/go-libp2p-kad-dht/internal/net"
	dht_pb "github.com/peter-tmc/go-libp2p-kad-dht/pb"
	"github.com/peter-tmc/go-libp2p-kad-dht/providers"
	kb "github.com/libp2p/go-libp2p-kbucket"

	record "github.com/libp2p/go-libp2p-record"
	recpb "github.com/libp2p/go-libp2p-record/pb"

	"github.com/libp2p/go-libp2p-xor/kademlia"
	kadkey "github.com/libp2p/go-libp2p-xor/key"
	"github.com/libp2p/go-libp2p-xor/trie"
)

var logger = logging.Logger("fullrtdht")

// FullRT is an experimental DHT client that is under development. Expect breaking changes to occur in this client
// until it stabilizes.
type FullRT struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	enableValues, enableProviders bool
	Validator                     record.Validator
	ProviderManager               *providers.ProviderManager
	datastore                     ds.Datastore
	h                             host.Host

	crawlerInterval time.Duration
	lastCrawlTime   time.Time

	crawler        *crawler.Crawler
	protoMessenger *dht_pb.ProtocolMessenger
	messageSender  dht_pb.MessageSender

	filterFromTable kaddht.QueryFilterFunc
	rtLk            sync.RWMutex
	rt              *trie.Trie

	kMapLk       sync.RWMutex
	keyToPeerMap map[string]peer.ID

	peerAddrsLk sync.RWMutex
	peerAddrs   map[peer.ID][]multiaddr.Multiaddr

	bootstrapPeers []*peer.AddrInfo

	bucketSize int

	triggerRefresh chan struct{}

	waitFrac     float64
	timeoutPerOp time.Duration

	bulkSendParallelism int
}

// NewFullRT creates a DHT client that tracks the full network. It takes a protocol prefix for the given network,
// For example, the protocol /ipfs/kad/1.0.0 has the prefix /ipfs.
//
// FullRT is an experimental DHT client that is under development. Expect breaking changes to occur in this client
// until it stabilizes.
//
// Not all of the standard DHT options are supported in this DHT.
func NewFullRT(h host.Host, protocolPrefix protocol.ID, options ...Option) (*FullRT, error) {
	var fullrtcfg config
	if err := fullrtcfg.apply(options...); err != nil {
		return nil, err
	}

	dhtcfg := &internalConfig.Config{
		Datastore:        dssync.MutexWrap(ds.NewMapDatastore()),
		Validator:        record.NamespacedValidator{},
		ValidatorChanged: false,
		EnableProviders:  true,
		EnableValues:     true,
		ProtocolPrefix:   protocolPrefix,
	}

	if err := dhtcfg.Apply(fullrtcfg.dhtOpts...); err != nil {
		return nil, err
	}
	if err := dhtcfg.ApplyFallbacks(h); err != nil {
		return nil, err
	}

	if err := dhtcfg.Validate(); err != nil {
		return nil, err
	}

	ms := net.NewMessageSenderImpl(h, []protocol.ID{dhtcfg.ProtocolPrefix + "/kad/1.0.0"})
	protoMessenger, err := dht_pb.NewProtocolMessenger(ms, dht_pb.WithValidator(dhtcfg.Validator))
	if err != nil {
		return nil, err
	}

	c, err := crawler.New(h, crawler.WithParallelism(200))
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	pm, err := providers.NewProviderManager(ctx, h.ID(), dhtcfg.Datastore)
	if err != nil {
		cancel()
		return nil, err
	}

	var bsPeers []*peer.AddrInfo

	for _, ai := range dhtcfg.BootstrapPeers {
		tmpai := ai
		bsPeers = append(bsPeers, &tmpai)
	}

	rt := &FullRT{
		ctx:    ctx,
		cancel: cancel,

		enableValues:    dhtcfg.EnableValues,
		enableProviders: dhtcfg.EnableProviders,
		Validator:       dhtcfg.Validator,
		ProviderManager: pm,
		datastore:       dhtcfg.Datastore,
		h:               h,
		crawler:         c,
		messageSender:   ms,
		protoMessenger:  protoMessenger,
		filterFromTable: kaddht.PublicQueryFilter,
		rt:              trie.New(),
		keyToPeerMap:    make(map[string]peer.ID),
		bucketSize:      dhtcfg.BucketSize,

		peerAddrs:      make(map[peer.ID][]multiaddr.Multiaddr),
		bootstrapPeers: bsPeers,

		triggerRefresh: make(chan struct{}),

		waitFrac:     0.3,
		timeoutPerOp: 5 * time.Second,

		crawlerInterval: time.Minute * 60,

		bulkSendParallelism: 10,
	}

	rt.wg.Add(1)
	go rt.runCrawler(ctx)

	return rt, nil
}

type crawlVal struct {
	addrs []multiaddr.Multiaddr
	key   kadkey.Key
}

func (dht *FullRT) TriggerRefresh(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case dht.triggerRefresh <- struct{}{}:
		return nil
	case <-dht.ctx.Done():
		return fmt.Errorf("dht is closed")
	}
}

func (dht *FullRT) Stat() map[string]peer.ID {
	newMap := make(map[string]peer.ID)

	dht.kMapLk.RLock()
	for k, v := range dht.keyToPeerMap {
		newMap[k] = v
	}
	dht.kMapLk.RUnlock()
	return newMap
}

func (dht *FullRT) Ready() bool {
	dht.rtLk.RLock()
	lastCrawlTime := dht.lastCrawlTime
	dht.rtLk.RUnlock()

	if time.Since(lastCrawlTime) > dht.crawlerInterval {
		return false
	}

	// TODO: This function needs to be better defined. Perhaps based on going through the peer map and seeing when the
	// last time we were connected to any of them was.
	dht.peerAddrsLk.RLock()
	rtSize := len(dht.keyToPeerMap)
	dht.peerAddrsLk.RUnlock()

	return rtSize > len(dht.bootstrapPeers)+1
}

func (dht *FullRT) Host() host.Host {
	return dht.h
}

func (dht *FullRT) runCrawler(ctx context.Context) {
	defer dht.wg.Done()
	t := time.NewTicker(dht.crawlerInterval)

	m := make(map[peer.ID]*crawlVal)
	mxLk := sync.Mutex{}

	initialTrigger := make(chan struct{}, 1)
	initialTrigger <- struct{}{}

	for {
		select {
		case <-t.C:
		case <-initialTrigger:
		case <-dht.triggerRefresh:
		case <-ctx.Done():
			return
		}

		var addrs []*peer.AddrInfo
		dht.peerAddrsLk.Lock()
		for k := range m {
			addrs = append(addrs, &peer.AddrInfo{ID: k}) // Addrs: v.addrs
		}

		addrs = append(addrs, dht.bootstrapPeers...)
		dht.peerAddrsLk.Unlock()

		for k := range m {
			delete(m, k)
		}

		start := time.Now()
		dht.crawler.Run(ctx, addrs,
			func(p peer.ID, rtPeers []*peer.AddrInfo) {
				conns := dht.h.Network().ConnsToPeer(p)
				var addrs []multiaddr.Multiaddr
				for _, conn := range conns {
					addr := conn.RemoteMultiaddr()
					addrs = append(addrs, addr)
				}

				if len(addrs) == 0 {
					logger.Debugf("no connections to %v after successful query. keeping addresses from the peerstore", p)
					addrs = dht.h.Peerstore().Addrs(p)
				}

				keep := kaddht.PublicRoutingTableFilter(dht, p)
				if !keep {
					return
				}

				mxLk.Lock()
				defer mxLk.Unlock()
				m[p] = &crawlVal{
					addrs: addrs,
				}
			},
			func(p peer.ID, err error) {})
		dur := time.Since(start)
		logger.Infof("crawl took %v", dur)

		peerAddrs := make(map[peer.ID][]multiaddr.Multiaddr)
		kPeerMap := make(map[string]peer.ID)
		newRt := trie.New()
		for k, v := range m {
			v.key = kadkey.KbucketIDToKey(kb.ConvertPeerID(k))
			peerAddrs[k] = v.addrs
			kPeerMap[string(v.key)] = k
			newRt.Add(v.key)
		}

		dht.peerAddrsLk.Lock()
		dht.peerAddrs = peerAddrs
		dht.peerAddrsLk.Unlock()

		dht.kMapLk.Lock()
		dht.keyToPeerMap = kPeerMap
		dht.kMapLk.Unlock()

		dht.rtLk.Lock()
		dht.rt = newRt
		dht.lastCrawlTime = time.Now()
		dht.rtLk.Unlock()
	}
}

func (dht *FullRT) Close() error {
	dht.cancel()
	err := dht.ProviderManager.Process().Close()
	dht.wg.Wait()
	return err
}

func (dht *FullRT) Bootstrap(ctx context.Context) error {
	return nil
}

// CheckPeers return (success, total)
func (dht *FullRT) CheckPeers(ctx context.Context, peers ...peer.ID) (int, int) {
	var peerAddrs chan interface{}
	var total int
	if len(peers) == 0 {
		dht.peerAddrsLk.RLock()
		total = len(dht.peerAddrs)
		peerAddrs = make(chan interface{}, total)
		for k, v := range dht.peerAddrs {
			peerAddrs <- peer.AddrInfo{
				ID:    k,
				Addrs: v,
			}
		}
		close(peerAddrs)
		dht.peerAddrsLk.RUnlock()
	} else {
		total = len(peers)
		peerAddrs = make(chan interface{}, total)
		dht.peerAddrsLk.RLock()
		for _, p := range peers {
			peerAddrs <- peer.AddrInfo{
				ID:    p,
				Addrs: dht.peerAddrs[p],
			}
		}
		close(peerAddrs)
		dht.peerAddrsLk.RUnlock()
	}

	var success uint64

	workers(100, func(i interface{}) {
		a := i.(peer.AddrInfo)
		dialctx, dialcancel := context.WithTimeout(ctx, time.Second*3)
		if err := dht.h.Connect(dialctx, a); err == nil {
			atomic.AddUint64(&success, 1)
		}
		dialcancel()
	}, peerAddrs)
	return int(success), total
}

func workers(numWorkers int, fn func(interface{}), inputs <-chan interface{}) {
	jobs := make(chan interface{})
	defer close(jobs)
	for i := 0; i < numWorkers; i++ {
		go func() {
			for j := range jobs {
				fn(j)
			}
		}()
	}
	for i := range inputs {
		jobs <- i
	}
}

func (dht *FullRT) GetClosestPeers(ctx context.Context, key string) ([]peer.ID, error) {
	kbID := kb.ConvertKey(key)
	kadKey := kadkey.KbucketIDToKey(kbID)
	dht.rtLk.RLock()
	closestKeys := kademlia.ClosestN(kadKey, dht.rt, dht.bucketSize)
	dht.rtLk.RUnlock()

	peers := make([]peer.ID, 0, len(closestKeys))
	for _, k := range closestKeys {
		dht.kMapLk.RLock()
		p, ok := dht.keyToPeerMap[string(k)]
		if !ok {
			logger.Errorf("key not found in map")
		}
		dht.kMapLk.RUnlock()
		dht.peerAddrsLk.RLock()
		peerAddrs := dht.peerAddrs[p]
		dht.peerAddrsLk.RUnlock()

		dht.h.Peerstore().AddAddrs(p, peerAddrs, peerstore.TempAddrTTL)
		peers = append(peers, p)
	}
	return peers, nil
}

// PutValue adds value corresponding to given Key.
// This is the top level "Store" operation of the DHT
func (dht *FullRT) PutValue(ctx context.Context, key string, value []byte, opts ...routing.Option) (err error) {
	if !dht.enableValues {
		return routing.ErrNotSupported
	}

	logger.Debugw("putting value", "key", internal.LoggableRecordKeyString(key))

	// don't even allow local users to put bad values.
	if err := dht.Validator.Validate(key, value); err != nil {
		return err
	}

	old, err := dht.getLocal(key)
	if err != nil {
		// Means something is wrong with the datastore.
		return err
	}

	// Check if we have an old value that's not the same as the new one.
	if old != nil && !bytes.Equal(old.GetValue(), value) {
		// Check to see if the new one is better.
		i, err := dht.Validator.Select(key, [][]byte{value, old.GetValue()})
		if err != nil {
			return err
		}
		if i != 0 {
			return fmt.Errorf("can't replace a newer value with an older value")
		}
	}

	rec := record.MakePutRecord(key, value)
	rec.TimeReceived = u.FormatRFC3339(time.Now())
	err = dht.putLocal(key, rec)
	if err != nil {
		return err
	}

	peers, err := dht.GetClosestPeers(ctx, key)
	if err != nil {
		return err
	}

	successes := dht.execOnMany(ctx, func(ctx context.Context, p peer.ID) error {
		routing.PublishQueryEvent(ctx, &routing.QueryEvent{
			Type: routing.Value,
			ID:   p,
		})
		err := dht.protoMessenger.PutValue(ctx, p, rec)
		return err
	}, peers, true)

	if successes == 0 {
		return fmt.Errorf("failed to complete put")
	}

	return nil
}

// RecvdVal stores a value and the peer from which we got the value.
type RecvdVal struct {
	Val  []byte
	From peer.ID
}

// GetValue searches for the value corresponding to given Key.
func (dht *FullRT) GetValue(ctx context.Context, key string, opts ...routing.Option) (_ []byte, err error) {
	if !dht.enableValues {
		return nil, routing.ErrNotSupported
	}

	// apply defaultQuorum if relevant
	var cfg routing.Options
	if err := cfg.Apply(opts...); err != nil {
		return nil, err
	}
	opts = append(opts, kaddht.Quorum(internalConfig.GetQuorum(&cfg)))

	responses, err := dht.SearchValue(ctx, key, opts...)
	if err != nil {
		return nil, err
	}
	var best []byte

	for r := range responses {
		best = r
	}

	if ctx.Err() != nil {
		return best, ctx.Err()
	}

	if best == nil {
		return nil, routing.ErrNotFound
	}
	logger.Debugf("GetValue %v %x", internal.LoggableRecordKeyString(key), best)
	return best, nil
}

// SearchValue searches for the value corresponding to given Key and streams the results.
func (dht *FullRT) SearchValue(ctx context.Context, key string, opts ...routing.Option) (<-chan []byte, error) {
	if !dht.enableValues {
		return nil, routing.ErrNotSupported
	}

	var cfg routing.Options
	if err := cfg.Apply(opts...); err != nil {
		return nil, err
	}

	responsesNeeded := 0
	if !cfg.Offline {
		responsesNeeded = internalConfig.GetQuorum(&cfg)
	}

	stopCh := make(chan struct{})
	valCh, lookupRes := dht.getValues(ctx, key, stopCh)

	out := make(chan []byte)
	go func() {
		defer close(out)
		best, peersWithBest, aborted := dht.searchValueQuorum(ctx, key, valCh, stopCh, out, responsesNeeded)
		if best == nil || aborted {
			return
		}

		updatePeers := make([]peer.ID, 0, dht.bucketSize)
		select {
		case l := <-lookupRes:
			if l == nil {
				return
			}

			for _, p := range l.peers {
				if _, ok := peersWithBest[p]; !ok {
					updatePeers = append(updatePeers, p)
				}
			}
		case <-ctx.Done():
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		dht.updatePeerValues(ctx, key, best, updatePeers)
		cancel()
	}()

	return out, nil
}

func (dht *FullRT) searchValueQuorum(ctx context.Context, key string, valCh <-chan RecvdVal, stopCh chan struct{},
	out chan<- []byte, nvals int) ([]byte, map[peer.ID]struct{}, bool) {
	numResponses := 0
	return dht.processValues(ctx, key, valCh,
		func(ctx context.Context, v RecvdVal, better bool) bool {
			numResponses++
			if better {
				select {
				case out <- v.Val:
				case <-ctx.Done():
					return false
				}
			}

			if nvals > 0 && numResponses > nvals {
				close(stopCh)
				return true
			}
			return false
		})
}

// GetValues gets nvals values corresponding to the given key.
func (dht *FullRT) GetValues(ctx context.Context, key string, nvals int) (_ []RecvdVal, err error) {
	if !dht.enableValues {
		return nil, routing.ErrNotSupported
	}

	queryCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	valCh, _ := dht.getValues(queryCtx, key, nil)

	out := make([]RecvdVal, 0, nvals)
	for val := range valCh {
		out = append(out, val)
		if len(out) == nvals {
			cancel()
		}
	}

	return out, ctx.Err()
}

func (dht *FullRT) processValues(ctx context.Context, key string, vals <-chan RecvdVal,
	newVal func(ctx context.Context, v RecvdVal, better bool) bool) (best []byte, peersWithBest map[peer.ID]struct{}, aborted bool) {
loop:
	for {
		if aborted {
			return
		}

		select {
		case v, ok := <-vals:
			if !ok {
				break loop
			}

			// Select best value
			if best != nil {
				if bytes.Equal(best, v.Val) {
					peersWithBest[v.From] = struct{}{}
					aborted = newVal(ctx, v, false)
					continue
				}
				sel, err := dht.Validator.Select(key, [][]byte{best, v.Val})
				if err != nil {
					logger.Warnw("failed to select best value", "key", internal.LoggableRecordKeyString(key), "error", err)
					continue
				}
				if sel != 1 {
					aborted = newVal(ctx, v, false)
					continue
				}
			}
			peersWithBest = make(map[peer.ID]struct{})
			peersWithBest[v.From] = struct{}{}
			best = v.Val
			aborted = newVal(ctx, v, true)
		case <-ctx.Done():
			return
		}
	}

	return
}

func (dht *FullRT) updatePeerValues(ctx context.Context, key string, val []byte, peers []peer.ID) {
	fixupRec := record.MakePutRecord(key, val)
	for _, p := range peers {
		go func(p peer.ID) {
			//TODO: Is this possible?
			if p == dht.h.ID() {
				err := dht.putLocal(key, fixupRec)
				if err != nil {
					logger.Error("Error correcting local dht entry:", err)
				}
				return
			}
			ctx, cancel := context.WithTimeout(ctx, time.Second*5)
			defer cancel()
			err := dht.protoMessenger.PutValue(ctx, p, fixupRec)
			if err != nil {
				logger.Debug("Error correcting DHT entry: ", err)
			}
		}(p)
	}
}

type lookupWithFollowupResult struct {
	peers []peer.ID // the top K not unreachable peers at the end of the query
}

func (dht *FullRT) getValues(ctx context.Context, key string, stopQuery chan struct{}) (<-chan RecvdVal, <-chan *lookupWithFollowupResult) {
	valCh := make(chan RecvdVal, 1)
	lookupResCh := make(chan *lookupWithFollowupResult, 1)

	logger.Debugw("finding value", "key", internal.LoggableRecordKeyString(key))

	if rec, err := dht.getLocal(key); rec != nil && err == nil {
		select {
		case valCh <- RecvdVal{
			Val:  rec.GetValue(),
			From: dht.h.ID(),
		}:
		case <-ctx.Done():
		}
	}
	peers, err := dht.GetClosestPeers(ctx, key)
	if err != nil {
		lookupResCh <- &lookupWithFollowupResult{}
		close(valCh)
		close(lookupResCh)
		return valCh, lookupResCh
	}

	go func() {
		defer close(valCh)
		defer close(lookupResCh)
		queryFn := func(ctx context.Context, p peer.ID) error {
			// For DHT query command
			routing.PublishQueryEvent(ctx, &routing.QueryEvent{
				Type: routing.SendingQuery,
				ID:   p,
			})

			rec, peers, err := dht.protoMessenger.GetValue(ctx, p, key)
			switch err {
			case routing.ErrNotFound:
				// in this case, they responded with nothing,
				// still send a notification so listeners can know the
				// request has completed 'successfully'
				routing.PublishQueryEvent(ctx, &routing.QueryEvent{
					Type: routing.PeerResponse,
					ID:   p,
				})
				return nil
			default:
				return err
			case nil, internal.ErrInvalidRecord:
				// in either of these cases, we want to keep going
			}

			// TODO: What should happen if the record is invalid?
			// Pre-existing code counted it towards the quorum, but should it?
			if rec != nil && rec.GetValue() != nil {
				rv := RecvdVal{
					Val:  rec.GetValue(),
					From: p,
				}

				select {
				case valCh <- rv:
				case <-ctx.Done():
					return ctx.Err()
				}
			}

			// For DHT query command
			routing.PublishQueryEvent(ctx, &routing.QueryEvent{
				Type:      routing.PeerResponse,
				ID:        p,
				Responses: peers,
			})

			return nil
		}

		dht.execOnMany(ctx, queryFn, peers, false)
		lookupResCh <- &lookupWithFollowupResult{peers: peers}
	}()
	return valCh, lookupResCh
}

// Provider abstraction for indirect stores.
// Some DHTs store values directly, while an indirect store stores pointers to
// locations of the value, similarly to Coral and Mainline DHT.

// Provide makes this node announce that it can provide a value for the given key
func (dht *FullRT) Provide(ctx context.Context, key cid.Cid, brdcst bool) (err error) {
	if !dht.enableProviders {
		return routing.ErrNotSupported
	} else if !key.Defined() {
		return fmt.Errorf("invalid cid: undefined")
	}
	keyMH := key.Hash()
	logger.Debugw("providing", "cid", key, "mh", internal.LoggableProviderRecordBytes(keyMH))

	// add self locally
	dht.ProviderManager.AddProvider(ctx, keyMH, dht.h.ID())
	if !brdcst {
		return nil
	}

	closerCtx := ctx
	if deadline, ok := ctx.Deadline(); ok {
		now := time.Now()
		timeout := deadline.Sub(now)

		if timeout < 0 {
			// timed out
			return context.DeadlineExceeded
		} else if timeout < 10*time.Second {
			// Reserve 10% for the final put.
			deadline = deadline.Add(-timeout / 10)
		} else {
			// Otherwise, reserve a second (we'll already be
			// connected so this should be fast).
			deadline = deadline.Add(-time.Second)
		}
		var cancel context.CancelFunc
		closerCtx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
	}

	var exceededDeadline bool
	peers, err := dht.GetClosestPeers(closerCtx, string(keyMH))
	switch err {
	case context.DeadlineExceeded:
		// If the _inner_ deadline has been exceeded but the _outer_
		// context is still fine, provide the value to the closest peers
		// we managed to find, even if they're not the _actual_ closest peers.
		if ctx.Err() != nil {
			return ctx.Err()
		}
		exceededDeadline = true
	case nil:
	default:
		return err
	}

	successes := dht.execOnMany(ctx, func(ctx context.Context, p peer.ID) error {
		err := dht.protoMessenger.PutProvider(ctx, p, keyMH, dht.h)
		return err
	}, peers, true)

	if exceededDeadline {
		return context.DeadlineExceeded
	}

	if successes == 0 {
		return fmt.Errorf("failed to complete provide")
	}

	return ctx.Err()
}

// execOnMany executes the given function on each of the peers, although it may only wait for a certain chunk of peers
// to respond before considering the results "good enough" and returning.
//
// If sloppyExit is true then this function will return without waiting for all of its internal goroutines to close.
// If sloppyExit is true then the passed in function MUST be able to safely complete an arbitrary amount of time after
// execOnMany has returned (e.g. do not write to resources that might get closed or set to nil and therefore result in
// a panic instead of just returning an error).
func (dht *FullRT) execOnMany(ctx context.Context, fn func(context.Context, peer.ID) error, peers []peer.ID, sloppyExit bool) int {
	if len(peers) == 0 {
		return 0
	}

	// having a buffer that can take all of the elements is basically a hack to allow for sloppy exits that clean up
	// the goroutines after the function is done rather than before
	errCh := make(chan error, len(peers))
	numSuccessfulToWaitFor := int(float64(len(peers)) * dht.waitFrac)

	putctx, cancel := context.WithTimeout(ctx, dht.timeoutPerOp)
	defer cancel()

	for _, p := range peers {
		go func(p peer.ID) {
			errCh <- fn(putctx, p)
		}(p)
	}

	var numDone, numSuccess, successSinceLastTick int
	var ticker *time.Ticker
	var tickChan <-chan time.Time

	for numDone < len(peers) {
		select {
		case err := <-errCh:
			numDone++
			if err == nil {
				numSuccess++
				if numSuccess >= numSuccessfulToWaitFor && ticker == nil {
					// Once there are enough successes, wait a little longer
					ticker = time.NewTicker(time.Millisecond * 500)
					defer ticker.Stop()
					tickChan = ticker.C
					successSinceLastTick = numSuccess
				}
				// This is equivalent to numSuccess * 2 + numFailures >= len(peers) and is a heuristic that seems to be
				// performing reasonably.
				// TODO: Make this metric more configurable
				// TODO: Have better heuristics in this function whether determined from observing static network
				// properties or dynamically calculating them
				if numSuccess+numDone >= len(peers) {
					cancel()
					if sloppyExit {
						return numSuccess
					}
				}
			}
		case <-tickChan:
			if numSuccess > successSinceLastTick {
				// If there were additional successes, then wait another tick
				successSinceLastTick = numSuccess
			} else {
				cancel()
				if sloppyExit {
					return numSuccess
				}
			}
		}
	}
	return numSuccess
}

func (dht *FullRT) ProvideMany(ctx context.Context, keys []multihash.Multihash) error {
	if !dht.enableProviders {
		return routing.ErrNotSupported
	}

	// Compute addresses once for all provides
	pi := peer.AddrInfo{
		ID:    dht.h.ID(),
		Addrs: dht.h.Addrs(),
	}
	pbPeers := dht_pb.RawPeerInfosToPBPeers([]peer.AddrInfo{pi})

	// TODO: We may want to limit the type of addresses in our provider records
	// For example, in a WAN-only DHT prohibit sharing non-WAN addresses (e.g. 192.168.0.100)
	if len(pi.Addrs) < 1 {
		return fmt.Errorf("no known addresses for self, cannot put provider")
	}

	fn := func(ctx context.Context, k peer.ID) error {
		peers, err := dht.GetClosestPeers(ctx, string(k))
		if err != nil {
			return err
		}
		successes := dht.execOnMany(ctx, func(ctx context.Context, p peer.ID) error {
			pmes := dht_pb.NewMessage(dht_pb.Message_ADD_PROVIDER, multihash.Multihash(k), 0)
			pmes.ProviderPeers = pbPeers

			return dht.messageSender.SendMessage(ctx, p, pmes)
		}, peers, true)
		if successes == 0 {
			return fmt.Errorf("no successful provides")
		}
		return nil
	}

	keysAsPeerIDs := make([]peer.ID, 0, len(keys))
	for _, k := range keys {
		keysAsPeerIDs = append(keysAsPeerIDs, peer.ID(k))
	}

	return dht.bulkMessageSend(ctx, keysAsPeerIDs, fn, true)
}

func (dht *FullRT) PutMany(ctx context.Context, keys []string, values [][]byte) error {
	if !dht.enableValues {
		return routing.ErrNotSupported
	}

	if len(keys) != len(values) {
		return fmt.Errorf("number of keys does not match the number of values")
	}

	keysAsPeerIDs := make([]peer.ID, 0, len(keys))
	keyRecMap := make(map[string][]byte)
	for i, k := range keys {
		keysAsPeerIDs = append(keysAsPeerIDs, peer.ID(k))
		keyRecMap[k] = values[i]
	}

	if len(keys) != len(keyRecMap) {
		return fmt.Errorf("does not support duplicate keys")
	}

	fn := func(ctx context.Context, k peer.ID) error {
		peers, err := dht.GetClosestPeers(ctx, string(k))
		if err != nil {
			return err
		}
		successes := dht.execOnMany(ctx, func(ctx context.Context, p peer.ID) error {
			keyStr := string(k)
			return dht.protoMessenger.PutValue(ctx, p, record.MakePutRecord(keyStr, keyRecMap[keyStr]))
		}, peers, true)
		if successes == 0 {
			return fmt.Errorf("no successful puts")
		}
		return nil
	}

	return dht.bulkMessageSend(ctx, keysAsPeerIDs, fn, false)
}

func (dht *FullRT) bulkMessageSend(ctx context.Context, keys []peer.ID, fn func(ctx context.Context, k peer.ID) error, isProvRec bool) error {
	if len(keys) == 0 {
		return nil
	}

	sortedKeys := kb.SortClosestPeers(keys, kb.ID(make([]byte, 32)))

	var numSends uint64 = 0
	var numSendsSuccessful uint64 = 0

	wg := sync.WaitGroup{}
	onePctKeys := uint64(len(sortedKeys)) / 100

	bulkSendFn := func(chunk []peer.ID) {
		defer wg.Done()
		for _, key := range chunk {
			if ctx.Err() != nil {
				break
			}

			sendsSoFar := atomic.AddUint64(&numSends, 1)
			if onePctKeys > 0 && sendsSoFar%onePctKeys == 0 {
				logger.Infof("bulk sending goroutine: %.1f%% done - %d/%d done", 100*float64(sendsSoFar)/float64(len(sortedKeys)), sendsSoFar, len(sortedKeys))
			}
			if err := fn(ctx, key); err != nil {
				var l interface{}
				if isProvRec {
					l = internal.LoggableProviderRecordBytes(key)
				} else {
					l = internal.LoggableRecordKeyString(key)
				}
				logger.Infof("failed to complete bulk sending of key :%v. %v", l, err)
			} else {
				atomic.AddUint64(&numSendsSuccessful, 1)
			}
		}
	}

	// divide the keys into groups so that we can talk to more peers at a time, because the keys are sorted in
	// XOR/Kadmelia space consecutive puts will be too the same, or nearly the same, set of peers. Working in parallel
	// means less waiting on individual dials to complete and also continuing to make progress even if one segment of
	// the network is being slow, or we are maxing out the connection, stream, etc. to those peers.
	keyGroups := divideIntoGroups(sortedKeys, dht.bulkSendParallelism)
	wg.Add(len(keyGroups))
	for _, chunk := range keyGroups {
		go bulkSendFn(chunk)
	}

	wg.Wait()

	if numSendsSuccessful == 0 {
		return fmt.Errorf("failed to complete bulk sending")
	}

	logger.Infof("bulk send complete: %d of %d successful", numSendsSuccessful, len(keys))

	return nil
}

// divideIntoGroups divides the set of keys into (at most) the number of groups
func divideIntoGroups(keys []peer.ID, groups int) [][]peer.ID {
	var keyGroups [][]peer.ID
	if len(keys) < groups {
		for i := 0; i < len(keys); i++ {
			keyGroups = append(keyGroups, keys[i:i+1])
		}
		return keyGroups
	}

	chunkSize := len(keys) / groups
	remainder := len(keys) % groups

	start := 0
	end := chunkSize
	for i := 0; i < groups; i++ {
		var chunk []peer.ID
		// distribute the remainder as one extra entry per parallel thread
		if remainder > 0 {
			chunk = keys[start : end+1]
			remainder--
			start = end + 1
			end = end + 1 + chunkSize
		} else {
			chunk = keys[start:end]
			start = end
			end = end + chunkSize
		}
		keyGroups = append(keyGroups, chunk)
	}
	return keyGroups
}

// FindProviders searches until the context expires.
func (dht *FullRT) FindProviders(ctx context.Context, c cid.Cid) ([]peer.AddrInfo, error) {
	if !dht.enableProviders {
		return nil, routing.ErrNotSupported
	} else if !c.Defined() {
		return nil, fmt.Errorf("invalid cid: undefined")
	}

	var providers []peer.AddrInfo
	for p := range dht.FindProvidersAsync(ctx, c, dht.bucketSize) {
		providers = append(providers, p)
	}
	return providers, nil
}

// FindProvidersAsync is the same thing as FindProviders, but returns a channel.
// Peers will be returned on the channel as soon as they are found, even before
// the search query completes. If count is zero then the query will run until it
// completes. Note: not reading from the returned channel may block the query
// from progressing.
func (dht *FullRT) FindProvidersAsync(ctx context.Context, key cid.Cid, count int) <-chan peer.AddrInfo {
	if !dht.enableProviders || !key.Defined() {
		peerOut := make(chan peer.AddrInfo)
		close(peerOut)
		return peerOut
	}

	chSize := count
	if count == 0 {
		chSize = 1
	}
	peerOut := make(chan peer.AddrInfo, chSize)

	keyMH := key.Hash()

	logger.Debugw("finding providers", "cid", key, "mh", internal.LoggableProviderRecordBytes(keyMH))
	go dht.findProvidersAsyncRoutine(ctx, keyMH, count, peerOut)
	return peerOut
}

func (dht *FullRT) findProvidersAsyncRoutine(ctx context.Context, key multihash.Multihash, count int, peerOut chan peer.AddrInfo) {
	defer close(peerOut)

	findAll := count == 0
	var ps *peer.Set
	if findAll {
		ps = peer.NewSet()
	} else {
		ps = peer.NewLimitedSet(count)
	}

	provs := dht.ProviderManager.GetProviders(ctx, key)
	for _, p := range provs {
		// NOTE: Assuming that this list of peers is unique
		if ps.TryAdd(p) {
			pi := dht.h.Peerstore().PeerInfo(p)
			select {
			case peerOut <- pi:
			case <-ctx.Done():
				return
			}
		}

		// If we have enough peers locally, don't bother with remote RPC
		// TODO: is this a DOS vector?
		if !findAll && ps.Size() >= count {
			return
		}
	}

	peers, err := dht.GetClosestPeers(ctx, string(key))
	if err != nil {
		return
	}

	queryctx, cancelquery := context.WithCancel(ctx)
	defer cancelquery()

	fn := func(ctx context.Context, p peer.ID) error {
		// For DHT query command
		routing.PublishQueryEvent(ctx, &routing.QueryEvent{
			Type: routing.SendingQuery,
			ID:   p,
		})

		provs, closest, err := dht.protoMessenger.GetProviders(ctx, p, key)
		if err != nil {
			return err
		}

		logger.Debugf("%d provider entries", len(provs))

		// Add unique providers from request, up to 'count'
		for _, prov := range provs {
			dht.maybeAddAddrs(prov.ID, prov.Addrs, peerstore.TempAddrTTL)
			logger.Debugf("got provider: %s", prov)
			if ps.TryAdd(prov.ID) {
				logger.Debugf("using provider: %s", prov)
				select {
				case peerOut <- *prov:
				case <-ctx.Done():
					logger.Debug("context timed out sending more providers")
					return ctx.Err()
				}
			}
			if !findAll && ps.Size() >= count {
				logger.Debugf("got enough providers (%d/%d)", ps.Size(), count)
				cancelquery()
				return nil
			}
		}

		// Give closer peers back to the query to be queried
		logger.Debugf("got closer peers: %d %s", len(closest), closest)

		routing.PublishQueryEvent(ctx, &routing.QueryEvent{
			Type:      routing.PeerResponse,
			ID:        p,
			Responses: closest,
		})
		return nil
	}

	dht.execOnMany(queryctx, fn, peers, false)
}

// FindPeer searches for a peer with given ID.
func (dht *FullRT) FindPeer(ctx context.Context, id peer.ID) (_ peer.AddrInfo, err error) {
	if err := id.Validate(); err != nil {
		return peer.AddrInfo{}, err
	}

	logger.Debugw("finding peer", "peer", id)

	// Check if were already connected to them
	if pi := dht.FindLocal(id); pi.ID != "" {
		return pi, nil
	}

	peers, err := dht.GetClosestPeers(ctx, string(id))
	if err != nil {
		return peer.AddrInfo{}, err
	}

	queryctx, cancelquery := context.WithCancel(ctx)
	defer cancelquery()

	addrsCh := make(chan *peer.AddrInfo, 1)
	newAddrs := make([]multiaddr.Multiaddr, 0)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		addrsSoFar := make(map[multiaddr.Multiaddr]struct{})
		for {
			select {
			case ai, ok := <-addrsCh:
				if !ok {
					return
				}

				for _, a := range ai.Addrs {
					_, found := addrsSoFar[a]
					if !found {
						newAddrs = append(newAddrs, a)
						addrsSoFar[a] = struct{}{}
					}
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	fn := func(ctx context.Context, p peer.ID) error {
		// For DHT query command
		routing.PublishQueryEvent(ctx, &routing.QueryEvent{
			Type: routing.SendingQuery,
			ID:   p,
		})

		peers, err := dht.protoMessenger.GetClosestPeers(ctx, p, id)
		if err != nil {
			logger.Debugf("error getting closer peers: %s", err)
			return err
		}

		// For DHT query command
		routing.PublishQueryEvent(ctx, &routing.QueryEvent{
			Type:      routing.PeerResponse,
			ID:        p,
			Responses: peers,
		})

		for _, a := range peers {
			if a.ID == id {
				select {
				case addrsCh <- a:
				case <-ctx.Done():
					return ctx.Err()
				}
				return nil
			}
		}
		return nil
	}

	dht.execOnMany(queryctx, fn, peers, false)

	close(addrsCh)
	wg.Wait()

	if len(newAddrs) > 0 {
		connctx, cancelconn := context.WithTimeout(ctx, time.Second*5)
		defer cancelconn()
		_ = dht.h.Connect(connctx, peer.AddrInfo{
			ID:    id,
			Addrs: newAddrs,
		})
	}

	// Return peer information if we tried to dial the peer during the query or we are (or recently were) connected
	// to the peer.
	connectedness := dht.h.Network().Connectedness(id)
	if connectedness == network.Connected || connectedness == network.CanConnect {
		return dht.h.Peerstore().PeerInfo(id), nil
	}

	return peer.AddrInfo{}, routing.ErrNotFound
}

var _ routing.Routing = (*FullRT)(nil)

// getLocal attempts to retrieve the value from the datastore.
//
// returns nil, nil when either nothing is found or the value found doesn't properly validate.
// returns nil, some_error when there's a *datastore* error (i.e., something goes very wrong)
func (dht *FullRT) getLocal(key string) (*recpb.Record, error) {
	logger.Debugw("finding value in datastore", "key", internal.LoggableRecordKeyString(key))

	rec, err := dht.getRecordFromDatastore(mkDsKey(key))
	if err != nil {
		logger.Warnw("get local failed", "key", internal.LoggableRecordKeyString(key), "error", err)
		return nil, err
	}

	// Double check the key. Can't hurt.
	if rec != nil && string(rec.GetKey()) != key {
		logger.Errorw("BUG: found a DHT record that didn't match it's key", "expected", internal.LoggableRecordKeyString(key), "got", rec.GetKey())
		return nil, nil

	}
	return rec, nil
}

// putLocal stores the key value pair in the datastore
func (dht *FullRT) putLocal(key string, rec *recpb.Record) error {
	data, err := proto.Marshal(rec)
	if err != nil {
		logger.Warnw("failed to put marshal record for local put", "error", err, "key", internal.LoggableRecordKeyString(key))
		return err
	}

	return dht.datastore.Put(mkDsKey(key), data)
}

func mkDsKey(s string) ds.Key {
	return ds.NewKey(base32.RawStdEncoding.EncodeToString([]byte(s)))
}

// returns nil, nil when either nothing is found or the value found doesn't properly validate.
// returns nil, some_error when there's a *datastore* error (i.e., something goes very wrong)
func (dht *FullRT) getRecordFromDatastore(dskey ds.Key) (*recpb.Record, error) {
	buf, err := dht.datastore.Get(dskey)
	if err == ds.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		logger.Errorw("error retrieving record from datastore", "key", dskey, "error", err)
		return nil, err
	}
	rec := new(recpb.Record)
	err = proto.Unmarshal(buf, rec)
	if err != nil {
		// Bad data in datastore, log it but don't return an error, we'll just overwrite it
		logger.Errorw("failed to unmarshal record from datastore", "key", dskey, "error", err)
		return nil, nil
	}

	err = dht.Validator.Validate(string(rec.GetKey()), rec.GetValue())
	if err != nil {
		// Invalid record in datastore, probably expired but don't return an error,
		// we'll just overwrite it
		logger.Debugw("local record verify failed", "key", rec.GetKey(), "error", err)
		return nil, nil
	}

	return rec, nil
}

// FindLocal looks for a peer with a given ID connected to this dht and returns the peer and the table it was found in.
func (dht *FullRT) FindLocal(id peer.ID) peer.AddrInfo {
	switch dht.h.Network().Connectedness(id) {
	case network.Connected, network.CanConnect:
		return dht.h.Peerstore().PeerInfo(id)
	default:
		return peer.AddrInfo{}
	}
}

func (dht *FullRT) maybeAddAddrs(p peer.ID, addrs []multiaddr.Multiaddr, ttl time.Duration) {
	// Don't add addresses for self or our connected peers. We have better ones.
	if p == dht.h.ID() || dht.h.Network().Connectedness(p) == network.Connected {
		return
	}
	dht.h.Peerstore().AddAddrs(p, addrs, ttl)
}
