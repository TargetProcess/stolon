package store

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"golang.org/x/net/context"

	etcdv3 "github.com/coreos/etcd/clientv3"
	"github.com/docker/libkv"
	"github.com/docker/libkv/store"
)

var (
	// ErrAbortTryLock is thrown when a user stops trying to seek the lock
	// by sending a signal to the stop chan, this is used to verify if the
	// operation succeeded
	ErrAbortTryLock = errors.New("lock operation aborted")
)

// EtcdV3 is the receiver type for the Store interface for etcd v3 client API
type Etcd struct {
	client *etcdv3.Client
}

type etcdLock struct {
	client *etcdv3.Client
}

const (
	periodicSync      = 5 * time.Minute
	defaultLockTTL    = 20 * time.Second
	defaultUpdateTime = 5 * time.Second
)

// Register registers etcd to libkv
func Register() {
	libkv.AddStore(store.ETCDV3, New)
}

// New creates a new Etcd client given a list
// of endpoints and an optional tls config
func New(addrs []string, options *store.Config) (store.Store, error) {
	s := &Etcd{}

	var (
		entries []string
		err     error
	)

	entries = store.CreateEndpoints(addrs, "http")
	cfg := &etcdv3.Config{
		Endpoints:        entries,
		AutoSyncInterval: periodicSync,
	}

	// Set options
	if options != nil {
		if options.TLS != nil {
			cfg.TLS = options.TLS
		}
		if options.ConnectionTimeout != 0 {
			cfg.DialTimeout = options.ConnectionTimeout
		}
		if options.Username != "" {
			cfg.Username = options.Username
			cfg.Password = options.Password
		}
	}

	s.client, err = etcdv3.New(*cfg)
	if err != nil {
		log.Fatal(err)
	}

	return s, nil
}

// Normalize the key for usage in Etcd
func (s *Etcd) normalize(key string) string {
	key = store.Normalize(key)
	return strings.TrimPrefix(key, "/")
}

// Get the value at "key", returns the last modified
// index to use in conjunction to Atomic calls
func (s *Etcd) Get(key string) (pair *store.KVPair, err error) {
	resp, err := s.client.Get(context.Background(), s.normalize(key))
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, store.ErrKeyNotFound
	}
	// just get the first value
	kv := resp.Kvs[0]

	pair = &store.KVPair{
		Key:       string(kv.Key),
		Value:     kv.Value,
		LastIndex: uint64(kv.ModRevision),
	}

	return pair, nil
}

// Put a value at "key"
func (s *Etcd) Put(key string, value []byte, opts *store.WriteOptions) error {
	fmt.Println("Puting key %s via etcd v3 client./n", key)
	putOps := []etcdv3.OpOption{}
	ctx := context.Background()

	if opts != nil {
		if opts.TTL > 0 {
			lease, err := s.client.Lease.Grant(ctx, int64(opts.TTL.Seconds()))
			if err != nil {
				return err
			}
			putOps = append(putOps, etcdv3.WithLease(lease.ID))
		}
	}
	_, err := s.client.Put(ctx, s.normalize(key), string(value), putOps...)
	return err
}

// Delete a value at "key"
func (s *Etcd) Delete(key string) error {
	resp, err := s.client.Delete(context.Background(), s.normalize(key))
	if err != nil {
		return err
	}
	if resp.Deleted == 0 {
		return store.ErrKeyNotFound
	}
	return nil
}

// Exists checks if the key exists inside the store
func (s *Etcd) Exists(key string) (bool, error) {
	_, err := s.Get(key)
	if err != nil {
		if err == store.ErrKeyNotFound {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Watch for changes on a "key"
// It returns a channel that will receive changes or pass
// on errors. Upon creation, the current value will first
// be sent to the channel. Providing a non-nil stopCh can
// be used to stop watching.
func (s *Etcd) Watch(key string, stopCh <-chan struct{}) (<-chan *store.KVPair, error) {
	// watchCh is sending back events to the caller
	watchCh := make(chan *store.KVPair)

	go func() {
		defer close(watchCh)
		ctx := context.Background()

		resp, err := s.client.Get(ctx, s.normalize(key))
		if err != nil {
			return
		}

		rch := s.client.Watch(ctx, s.normalize(key),
			etcdv3.WithRev(resp.Header.Revision),
			etcdv3.WithFilterDelete(),
		)

		for {
			// Check if the watch was stopped by the caller
			select {
			case <-stopCh:
				return
			case wresp := <-rch:
				for _, ev := range wresp.Events {
					if ev.Kv == nil {
						continue
					}
					watchCh <- &store.KVPair{
						Key:       string(ev.Kv.Key),
						Value:     ev.Kv.Value,
						LastIndex: uint64(ev.Kv.ModRevision),
					}
				}
			}
		}
	}()

	return watchCh, nil
}

// WatchTree watches for changes on a "directory"
// It returns a channel that will receive changes or pass
// on errors. Upon creating a watch, the current childs values
// will be sent to the channel. Providing a non-nil stopCh can
// be used to stop watching.
func (s *Etcd) WatchTree(directory string, stopCh <-chan struct{}) (<-chan []*store.KVPair, error) {
	// watchCh is sending back events to the caller
	watchCh := make(chan []*store.KVPair)

	go func() {
		defer close(watchCh)

		ctx := context.Background()

		resp, err := s.client.Get(ctx, s.normalize(directory), etcdv3.WithPrefix())
		if err != nil {
			return
		}

		rch := s.client.Watch(context.Background(), s.normalize(directory),
			etcdv3.WithRev(resp.Header.Revision),
			etcdv3.WithPrefix())

		for {
			// Check if the watch was stopped by the caller
			select {
			case <-stopCh:
				return
			case wresp := <-rch:
				for range wresp.Events {
					kvs, err := s.List(directory)
					if err != nil {
						return
					}
					watchCh <- kvs
				}
			}
		}
	}()

	return watchCh, nil
}

// AtomicPut puts a value at "key" if the key has not been
// modified in the meantime, throws an error if this is the case
func (s *Etcd) AtomicPut(key string, value []byte, previous *store.KVPair, opts *store.WriteOptions) (bool, *store.KVPair, error) {
	var (
		putOps           = []etcdv3.OpOption{}
		ctx              = context.Background()
		keyName          = s.normalize(key)
		lastModRev int64 = 0
	)

	if opts != nil {
		if opts.TTL > 0 {
			lease, err := s.client.Lease.Grant(ctx, int64(opts.TTL.Seconds()))
			if err != nil {
				return false, nil, err
			}
			putOps = append(putOps, etcdv3.WithLease(lease.ID))
		}
	}

	if previous != nil {
		lastModRev = int64(previous.LastIndex)
	}

	resp, err := s.client.Txn(ctx).If(
		etcdv3.Compare(etcdv3.ModRevision(keyName), "=", lastModRev),
	).Then(
		etcdv3.OpPut(keyName, string(value), putOps...),
		etcdv3.OpGet(keyName),
	).Commit()
	if err != nil {
		return false, nil, err
	}
	if !resp.Succeeded {
		return false, nil, store.ErrKeyModified
	}
	if len(resp.Responses) != 2 {
		return false, nil, errors.New("failed to execute all transactions")
	}
	if len(resp.Responses[1].GetResponseRange().Kvs) != 1 {
		return false, nil, errors.New("failed to retrieve the current value after put")
	}
	kv := resp.Responses[1].GetResponseRange().Kvs[0]
	updated := &store.KVPair{
		Key:       string(kv.Key),
		Value:     kv.Value,
		LastIndex: uint64(kv.ModRevision),
	}
	return true, updated, nil
}

// AtomicDelete deletes a value at "key" if the key
// has not been modified in the meantime, throws an
// error if this is the case
func (s *Etcd) AtomicDelete(key string, previous *store.KVPair) (bool, error) {
	if previous == nil {
		return false, store.ErrPreviousNotSpecified
	}

	var (
		ctx        = context.Background()
		lastModRev = int64(previous.LastIndex)
		keyName    = s.normalize(key)
	)

	resp, err := s.client.Txn(ctx).If(
		etcdv3.Compare(etcdv3.ModRevision(keyName), "=", lastModRev),
	).Then(
		etcdv3.OpDelete(keyName),
	).Commit()
	if err != nil {
		return false, err
	}
	if !resp.Succeeded {
		return false, fmt.Errorf("failed to execute all transactions: %#v", resp)
	}
	if resp.Responses[0].GetResponseDeleteRange().Deleted == 0 {
		return false, store.ErrKeyNotFound
	}
	return true, nil
}

//// List child nodes of a given directory
func (s *Etcd) List(directory string) ([]*store.KVPair, error) {
	resp, err := s.client.Get(context.Background(), s.normalize(directory),
		etcdv3.WithPrefix(),
		etcdv3.WithSort(etcdv3.SortByKey, etcdv3.SortAscend),
	)
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, store.ErrKeyNotFound
	}

	list := []*store.KVPair{}
	for _, kv := range resp.Kvs {
		list = append(list, &store.KVPair{
			Key:       string(kv.Key),
			Value:     kv.Value,
			LastIndex: uint64(kv.ModRevision),
		})
	}
	return list, nil
}

// DeleteTree deletes a range of keys under a given directory
func (s *Etcd) DeleteTree(directory string) error {
	resp, err := s.client.Delete(context.Background(), s.normalize(directory),
		etcdv3.WithPrefix())
	if err != nil {
		return err
	}
	if resp.Deleted == 0 {
		return store.ErrKeyNotFound
	}
	return err
}

//// NewLock returns a handle to a lock struct which can
//// be used to provide mutual exclusion on a key
func (s *Etcd) NewLock(key string, options *store.LockOptions) (lock store.Locker, err error) {
	return nil, store.ErrCallNotSupported
}

// Lock attempts to acquire the lock and blocks while
// doing so. It returns a channel that is closed if our
// lock is lost or if an error occurs
func (l *etcdLock) Lock(stopChan chan struct{}) (<-chan struct{}, error) {
	return nil, store.ErrCallNotSupported
}

// Unlock the "key". Calling unlock while
// not holding the lock will throw an error
func (l *etcdLock) Unlock() error {
	return store.ErrCallNotSupported
}

// Close closes the client connection
func (s *Etcd) Close() {
	s.client.Close()
	return
}
