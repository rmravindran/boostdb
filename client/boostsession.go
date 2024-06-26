package client

import (
	gocontext "context"
	"encoding/binary"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/encoding"
	dbencoding "github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/x/ident"
	"github.com/rmravindran/boostdb/core"

	xtime "github.com/m3db/m3/src/x/time"
)

type BoostSession struct {
	session             client.Session
	maxSymTables        int
	symTables           *lru.Cache[string, *core.SymTable]
	numSymbolUpdates    uint64
	numAttributeUpdates uint64
	maxConcurrentWrites uint32
	pendingWrites       atomic.Int32
	rwControl           sync.Mutex
	numWrites           uint64
}

// NewBoostSession returns a new session that can be used to write to the database.
func NewBoostSession(
	session client.Session,
	maxSymTables int,
	maxConcurrentWrites uint32) *BoostSession {
	bs := &BoostSession{
		session:             session,
		maxSymTables:        maxSymTables,
		numSymbolUpdates:    0,
		numAttributeUpdates: 0,
		maxConcurrentWrites: maxConcurrentWrites,
		rwControl:           sync.Mutex{},
		numWrites:           0,
	}
	bs.pendingWrites.Store(0)

	cache, err := lru.New[string, *core.SymTable](maxSymTables)
	if err != nil {
		return nil
	}
	bs.symTables = cache
	return bs
}

// WriteClusterAvailability returns whether cluster is available for writes.
func (bs *BoostSession) WriteClusterAvailability() (bool, error) {
	return bs.session.WriteClusterAvailability()
}

// ReadClusterAvailability returns whether cluster is available for reads.
func (bs *BoostSession) ReadClusterAvailability() (bool, error) {
	return bs.session.ReadClusterAvailability()
}

// Write value to the database for an ID.
func (bs *BoostSession) Write(
	namespace,
	id ident.ID,
	t xtime.UnixNano,
	value float64,
	unit xtime.Unit,
	annotation []byte,
) error {
	return bs.session.Write(namespace, id, t, value, unit, annotation)
}

// WriteTagged value to the database for an ID and given tags.
func (bs *BoostSession) WriteTagged(
	namespace,
	id ident.ID,
	tags ident.TagIterator,
	t xtime.UnixNano,
	value float64,
	unit xtime.Unit,
	annotation []byte,
) error {
	return bs.session.WriteTagged(namespace, id, tags, t, value, unit, annotation)
}

// Write tagged value that has attributes.
func (bs *BoostSession) WriteValueWithTaggedAttributes(
	namespace,
	id ident.ID,
	tags ident.TagIterator,
	attributes ident.TagIterator,
	t xtime.UnixNano,
	value float64,
	unit xtime.Unit,
	symTableNameResolver core.SymbolTableStreamNameResolver,
	completionFn core.WriteCompletionFn,
) error {

	// Check if the symbol table exists for this data series. This is done
	// under a lock
	bs.rwControl.Lock()

	symTableName := symTableNameResolver(id)
	symTable, ok := bs.symTables.Get(symTableName)
	if !ok {
		m3dbStreamWriter := core.NewM3DBSymStreamWriter(
			namespace,
			ident.StringID(symTableName),
			bs.session)
		symTable = core.NewSymTable(symTableName, 1, m3dbStreamWriter)
		bs.symTables.Add(symTableName, symTable)
	}

	attrMap := make(map[string]string)
	for attributes.Next() {
		attrC := attributes.Current()
		attrMap[attrC.Name.String()] = attrC.Value.String()
	}

	indexedHeader, hasMissing := symTable.GetIndexedHeader(attrMap)
	if hasMissing {
		// Add the missing values to the symbol table
		bs.updateSymbolsAndAttributes(symTable, attrMap)
		indexedHeader, hasMissing = symTable.GetIndexedHeader(attrMap)
		if hasMissing {
			bs.rwControl.Unlock()
			return errors.New("unable to find all attributes in the symbol table")
		}
	}

	annotation := make([]byte, 4+(4*len(indexedHeader)))
	binary.LittleEndian.PutUint16(annotation, symTable.Version())
	binary.LittleEndian.PutUint16(annotation[2:], uint16(len(indexedHeader)))
	tmp := annotation[4:]
	for i, index := range indexedHeader {
		binary.LittleEndian.PutUint32(tmp[i*4:], uint32(index))
	}

	// Unlock the mutex
	bs.rwControl.Unlock()

	// Don't schedule if there are too many pending writes
	bs.waitIfTooManyPendingWrites()

	bs.numWrites++

	if bs.numWrites == 1 {
		println("first write series    : ", id.String())
		println("first write tags      : ", tags)
		println("first write value     : ", value)
		println("first write anno size : ", len(annotation))
		println("first write anno      : ", core.ByteArrayToHex(annotation))
	}
	go func(
		namespace,
		id ident.ID,
		tags ident.TagIterator,
		t xtime.UnixNano,
		value float64,
		unit xtime.Unit,
		completionFn core.WriteCompletionFn,
		numWrites uint64) {
		var ret error = nil
		if tags != nil {
			ret = bs.session.WriteTagged(namespace, id, tags, t, value, unit, annotation)
		} else {
			ret = bs.session.Write(namespace, id, t, value, unit, annotation)
		}
		bs.writeCompletionFn(ret)

		// Call the user provided completion function
		completionFn(ret)

	}(namespace, id, tags.Duplicate(), t, value, unit, completionFn, bs.numWrites)

	return nil
}

// Fetch values from the database for an ID.
func (bs *BoostSession) FetchValueWithTaggedAttribute(
	namespace ident.ID,
	id ident.ID,
	startInclusive xtime.UnixNano,
	endExclusive xtime.UnixNano,
	symTableNameResolver core.SymbolTableStreamNameResolver,
	multiUseIter bool,
) (*BoostSeriesIterator, error) {
	seriesIt, err := bs.session.Fetch(namespace, id, startInclusive, endExclusive)
	if err != nil {
		return nil, err
	}

	iterators := make([]dbencoding.SeriesIterator, 0, 1)
	iterators = append(iterators, seriesIt)

	return NewBoostSeriesIterator(
		iterators,
		symTableNameResolver,
		bs.fetchOrCreateSymTable,
		startInclusive,
		endExclusive,
		multiUseIter), nil
}

func (bs *BoostSession) fetchOrCreateSymTable(
	namespaceId ident.ID,
	symTableName string,
	version uint16,
	timeBegin xtime.UnixNano,
	timeEnd xtime.UnixNano) (*core.SymTable, error) {

	// Find the version encoded in the annotation

	// Check if the symbol table exists for this data series. This is done
	// under a lock
	bs.rwControl.Lock()
	defer bs.rwControl.Unlock()

	symTable, ok := bs.symTables.Get(symTableName)
	if !ok {
		var err error = nil
		symTable, err = bs.readSymTableStream(
			namespaceId,
			ident.StringID(symTableName),
			version,
			timeBegin,
			timeEnd)
		if err != nil {
			return nil, err
		}
		bs.symTables.Add(symTableName, symTable)
	}

	return symTable, nil
}

// Fetch values from the database for an ID.
func (bs *BoostSession) Fetch(
	namespace,
	id ident.ID,
	startInclusive,
	endExclusive xtime.UnixNano,
) (encoding.SeriesIterator, error) {
	return bs.session.Fetch(namespace, id, startInclusive, endExclusive)
}

// FetchIDs values from the database for a set of IDs.
func (bs *BoostSession) FetchIDs(
	namespace ident.ID,
	ids ident.Iterator,
	startInclusive,
	endExclusive xtime.UnixNano,
) (encoding.SeriesIterators, error) {
	return bs.session.FetchIDs(namespace, ids, startInclusive, endExclusive)
}

// FetchTagged resolves the provided query to known IDs, and fetches the data for them.
func (bs *BoostSession) FetchTagged(
	ctx gocontext.Context,
	namespace ident.ID,
	q index.Query,
	opts index.QueryOptions,
) (encoding.SeriesIterators, client.FetchResponseMetadata, error) {
	return bs.session.FetchTagged(ctx, namespace, q, opts)
}

// FetchTaggedIDs resolves the provided query to known IDs.
func (bs *BoostSession) FetchTaggedIDs(
	ctx gocontext.Context,
	namespace ident.ID,
	q index.Query,
	opts index.QueryOptions,
) (client.TaggedIDsIterator, client.FetchResponseMetadata, error) {
	return bs.session.FetchTaggedIDs(ctx, namespace, q, opts)
}

// Aggregate aggregates values from the database for the given set of constraints.
func (bs *BoostSession) Aggregate(
	ctx gocontext.Context,
	namespace ident.ID,
	q index.Query,
	opts index.AggregationOptions,
) (client.AggregatedTagsIterator, client.FetchResponseMetadata, error) {
	return bs.session.Aggregate(ctx, namespace, q, opts)
}

// ShardID returns the given shard for an ID for callers
// to easily discern what shard is failing when operations
// for given IDs begin failing.
func (bs *BoostSession) ShardID(id ident.ID) (uint32, error) {
	return bs.session.ShardID(id)
}

// IteratorPools exposes the internal iterator pools used by the session to clients.
func (bs *BoostSession) IteratorPools() (encoding.IteratorPools, error) {
	return bs.session.IteratorPools()
}

// Close the session
func (bs *BoostSession) Close() error {
	return bs.session.Close()
}

// Wait waits for all pending writes to complete or the timeout to occur.
// If timeout is 0, wait indefinitely until all pending writes are completed.
// Returns an error if timeout occurs.
func (bs *BoostSession) Wait(timeout time.Duration) error {
	totalUs := 0

	for {
		if bs.pendingWrites.Load() == 0 {
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

// Update symbol table with missing attribute values and also add the attribute
// values to the attribute in the symbol table
func (bs *BoostSession) updateSymbolsAndAttributes(symTable *core.SymTable, attributes map[string]string) error {
	symbols := make([]string, 0)
	for _, value := range attributes {
		if !symTable.AttributeValueExists(value) {
			symbols = append(symbols, value)
		}
	}

	err := symTable.UpdateDictionary(symbols, nil)
	if err != nil {
		return err
	}
	bs.numSymbolUpdates++

	// Update the attributes
	for attrName, attrValue := range attributes {
		err = symTable.InsertAttributeValue(attrName, attrValue, nil)
		if err != nil {
			return err
		}
		bs.numAttributeUpdates++
	}

	return nil
}

// Use the M3DBSymStreamReader to read the symbol table stream
func (bs *BoostSession) readSymTableStream(
	namespace ident.ID,
	streamId ident.ID,
	version uint16,
	startTime xtime.UnixNano,
	endTime xtime.UnixNano) (*core.SymTable, error) {

	symTableReader := core.NewM3DBSymStreamReader(namespace, streamId, bs.session)
	err := symTableReader.Seek(startTime, endTime)
	if err != nil {
		return nil, err
	}

	// First loop until we find the init instruction matching the requested
	// version.
	var (
		v           uint16
		seqNum      uint32
		instruction core.TableInstruction
	)
	_, seqNum, err = bs.findInitInstruction(symTableReader, version)
	if err != nil {
		return nil, err
	}

	// Read the InitSymTable instruction parameters and add create symtable.
	instrParams, err := symTableReader.ReadInitInstruction()
	if err != nil {
		return nil, err
	}
	symTable := core.NewSymTable(streamId.String(), version, nil)
	symTable.UpdateDictionary(instrParams, nil)

	// Loop through the stream until we find the EndSymTable instruction
	// or we reach the end of the stream (NOPInstruction). Verify that the
	// sequence numbers are sequential.
	for {
		prevSeqNum := seqNum
		v, seqNum, instruction, err = symTableReader.Next()
		if err != nil {
			return nil, err
		}

		if instruction == core.NOPInstruction {
			// We reached the end of the stream. This table is the being
			// updated from the write side.
			break
		}

		if v != version {
			// This write must have failed in the middle. We need to search for
			// the next InitSymTable instruction with the same version.
			_, seqNum, err = bs.findInitInstruction(symTableReader, version)
			if err != nil {
				return nil, err
			}
			// Read the InitSymTable instruction parameters and add create
			// symtable.
			instrParams, err := symTableReader.ReadInitInstruction()
			if err != nil {
				return nil, err
			}
			symTable := core.NewSymTable(streamId.String(), version, nil)
			symTable.UpdateDictionary(instrParams, nil)
		}

		if seqNum != prevSeqNum+1 {
			// TODO, should we continue further to find another InitSymTable
			// instruction with the same version?
			return nil, errors.New("invalid sequence number")
		}

		if instruction == core.EndSymTable {
			// Last instruction. Finalize the symtable and return
			symTable.Finalize()
			break
		}

		switch instruction {
		case core.UpdateSymTable:
			instrParams, err = symTableReader.ReadUpdateInstruction()
			if err != nil {
				return nil, err
			}
			symTable.UpdateDictionary(instrParams, nil)
		case core.AddAttribute:
			attrName, _, indexValues, err := symTableReader.ReadAttributeInstruction()
			if err != nil {
				return nil, err
			}
			symTable.InsertAttributeIndices(attrName, indexValues, nil)
		}
	}

	return symTable, nil
}

// Find the InitSymTable instruction in the stream for a symbol table having
// the specified version and return the symtable version, sequence number and
// any error.
func (bs *BoostSession) findInitInstruction(
	symTableReader *core.M3DBSymStreamReader,
	version uint16) (uint16, uint32, error) {

	// First loop until we find the init instruction matching the requested
	// version.
	var (
		v           uint16
		seqNum      uint32
		instruction core.TableInstruction
		err         error
	)
	for {
		v, seqNum, instruction, err = symTableReader.Next()
		if err != nil {
			return 0, 0, err
		}
		if instruction == core.InitSymTable && version == v {
			break
		}
	}

	if instruction != core.InitSymTable {
		return 0, 0, errors.New("unable to find InitSymTable instruction")
	} else if seqNum != 1 {
		return 0, 0, errors.New("invalid sequence number for InitSymTable")
	} else if version != v {
		return 0, 0, errors.New("could not find a valid symtable with the specified version")
	}

	return v, seqNum, nil
}

// Wait if there are too many pending writes
func (bs *BoostSession) waitIfTooManyPendingWrites() {
	for {
		oldVal := bs.pendingWrites.Load()
		if oldVal < int32(bs.maxConcurrentWrites) {
			if bs.pendingWrites.CompareAndSwap(oldVal, oldVal+1) {
				break
			}
		}
	}
}

// Write completion function
func (bs *BoostSession) writeCompletionFn(err error) {
	bs.pendingWrites.Add(-1)
}
