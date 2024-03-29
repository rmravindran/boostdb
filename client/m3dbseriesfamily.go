package client

import (
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	dbencoding "github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
	core "github.com/rmravindran/boostdb/core"
)

// Implements the SeriesFamily interface
type M3DBSeriesFamily struct {
	// Name of the family
	name string

	// Domain name of the family
	domainName string

	// Namespace of the table
	namespace ident.ID

	// Current version of the table
	version uint16

	// Underlying Boost Session
	session *BoostSession

	// Distribution factor
	distributionFactor uint16

	// Next distribution index.
	nextDistributionIndex atomic.Uint32

	// Dictionary Limit
	dictionaryLimit uint32

	// Max concurrent writes
	maxConcurrentWrites uint32

	// Write control mutex
	writeControlMutex sync.Mutex

	// Atomic write counter for pending writes
	pendingWrites atomic.Int32
}

// NewM3DBSeriesFamily creates a new M3DBTAPTable
func NewM3DBSeriesFamily(
	name string,
	domainName string,
	namespace ident.ID,
	version uint16,
	session *BoostSession,
	distributionFactor uint16,
	dictionaryLimit uint32,
	maxConcurrentWrites uint32) *M3DBSeriesFamily {

	if domainName == "" {
		domainName = "dd"
	}
	domainName = strings.ToLower(domainName)

	if name == "" {
		name = "naf"
	}
	name = strings.ToLower(name)

	if distributionFactor == 0 {
		distributionFactor = 1
	}

	ret := &M3DBSeriesFamily{
		name:                  name,
		domainName:            domainName,
		namespace:             namespace,
		version:               version,
		session:               session,
		distributionFactor:    distributionFactor,
		nextDistributionIndex: atomic.Uint32{},
		dictionaryLimit:       dictionaryLimit,
		maxConcurrentWrites:   maxConcurrentWrites,
		writeControlMutex:     sync.Mutex{},
		pendingWrites:         atomic.Int32{},
	}
	ret.pendingWrites.Store(0)
	ret.nextDistributionIndex.Store(0)

	return ret
}

// Name returns the name of the table
func (sf *M3DBSeriesFamily) Name() string {
	return sf.name
}

// Namespace returns the namespace of the table
func (sf *M3DBSeriesFamily) Namespace() ident.ID {
	return sf.namespace
}

// Wait waits for all pending writes to complete or the timeout to occur.
// If timeout is 0, wait indefinitely until all pending writes are completed.
// Returns an error if timeout occurs.
func (sf *M3DBSeriesFamily) Wait(timeout time.Duration) error {
	totalUs := 0
	for {
		if sf.pendingWrites.Load() == 0 {
			break
		}
		time.Sleep(100 * time.Microsecond)
		totalUs += 100
		if (timeout > 0) && (totalUs > int(timeout/time.Microsecond)) {
			return errors.New("timeout waiting for pending writes to complete")
		}
	}

	return nil
}

// WriteTagged writes a float64 value into the table having the specified attributes
// and timestamp.
func (sf *M3DBSeriesFamily) Write(
	id ident.ID,
	attributes ident.TagIterator,
	timestamp xtime.UnixNano,
	value float64,
	unit xtime.Unit,
	completionFn core.WriteCompletionFn) error {

	return sf.WriteTagged(
		id,
		nil,
		attributes,
		timestamp,
		value,
		unit,
		completionFn)
}

// Write a float64 value into the table having the specified tags and
// attributes.
func (sf *M3DBSeriesFamily) WriteTagged(
	id ident.ID,
	tags ident.TagIterator,
	attributes ident.TagIterator,
	timestamp xtime.UnixNano,
	value float64,
	unit xtime.Unit,
	completionFn core.WriteCompletionFn) error {

	sf.waitIfTooManyPendingWrites()

	// Find the id from the distribution factor
	nextDistributionIndex := sf.nextDistributionIndex.Add(1)
	nextDistributionIndex %= uint32(sf.distributionFactor)

	// Qualified Series Name
	seriesName := core.GetQualifiedSeriesName(
		sf.domainName,
		sf.name,
		uint16(nextDistributionIndex),
		id)
	qualifiedId := ident.StringID(seriesName)

	return sf.session.WriteValueWithTaggedAttributes(
		sf.namespace,
		qualifiedId,
		tags,
		attributes,
		timestamp,
		value,
		unit,
		sf.symbolTableStreamNameResolver,
		func(err error) {
			sf.writeCompletionFn(err)
			if completionFn != nil {
				completionFn(err)
			}
		})
}

// Fetch values from the database for an ID.
func (sf *M3DBSeriesFamily) Fetch(
	id ident.ID,
	startInclusive xtime.UnixNano,
	endExclusive xtime.UnixNano,
	multiUseIter bool,
) (*BoostSeriesIterator, error) {

	// There is an iterator for every shard in the distribution
	iterators := make([]dbencoding.SeriesIterator, 0, sf.distributionFactor)
	var dId uint16
	for dId = 0; dId < sf.distributionFactor; dId++ {
		// Qualified Series Name
		seriesName := core.GetQualifiedSeriesName(
			sf.domainName,
			sf.name,
			dId,
			id)
		qualifiedId := ident.StringID(seriesName)

		seriesIt, err := sf.session.Fetch(
			sf.namespace, qualifiedId, startInclusive, endExclusive)
		if err != nil {
			return nil, err
		}
		iterators = append(iterators, seriesIt)
	}

	return NewBoostSeriesIterator(
		iterators,
		sf.symbolTableStreamNameResolver,
		sf.session.fetchOrCreateSymTable,
		startInclusive,
		endExclusive,
		multiUseIter), nil
}

func (sf *M3DBSeriesFamily) symbolTableStreamNameResolver(
	qualifiedSeriesId ident.ID) string {
	return sf.domainName + "::" + sf.name + "::symboltable"
}

// Wait if there are too many pending writes
func (sf *M3DBSeriesFamily) waitIfTooManyPendingWrites() {
	for {
		oldVal := sf.pendingWrites.Load()
		if oldVal < int32(sf.maxConcurrentWrites) {
			if sf.pendingWrites.CompareAndSwap(oldVal, oldVal+1) {
				break
			}
		}
	}
}

// Write completion function
func (sf *M3DBSeriesFamily) writeCompletionFn(err error) {
	sf.pendingWrites.Add(-1)
}
