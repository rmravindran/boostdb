package client

import (
	"encoding/binary"
	"errors"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
	"github.com/rmravindran/boostdb/core"
)

type CacheDpData struct {
	dp         ts.Datapoint
	timeUnit   xtime.Unit
	annotation ts.Annotation
}

type SymTableFetchFunction func(
	namespaceId ident.ID,
	symTableName string,
	version uint16,
	timeBegin xtime.UnixNano,
	timeEnd xtime.UnixNano) (*core.SymTable, error)

type SeriesShardIterator struct {
	seriesIter    encoding.SeriesIterator
	iterIndex     int
	dp            ts.Datapoint
	timeUnit      xtime.Unit
	annotation    ts.Annotation
	attributeIter ident.TagIterator
	hasRead       bool
	hasConsumed   bool
	isDone        bool
	isEOT         bool

	// TODO: Use ats series representation to cache the data
	dpCache []CacheDpData
}

type BoostSeriesIterator struct {
	symTableNameResolver core.SymbolTableStreamNameResolver
	symTableFetchFn      SymTableFetchFunction
	symTable             *core.SymTable
	startTime            xtime.UnixNano
	endTime              xtime.UnixNano
	numRead              uint64
	seriesShardIter      []SeriesShardIterator
	orderedShardIndices  []int
	rowNum               int
	currentTime          xtime.UnixNano
	currentIterIndex     int
	preFetchCount        int
	numToSkip            int
	iterError            error
	doCache              bool
	isDone               bool
}

type ShardIterPosState struct {
	dp          ts.Datapoint
	timeUnit    xtime.Unit
	annotation  ts.Annotation
	iterIndex   int
	hasRead     bool
	hasConsumed bool
	isDone      bool
	isEOT       bool
}

type BoostSeriesIteratorPosition struct {
	rowNum           int
	currentIterIndex int
	preFetchCount    int
	numToSkip        int
	sharIterIndices  []ShardIterPosState
}

// NewBoostSeriesIterator returns a new series iterator
func NewBoostSeriesIterator(
	seriesIterators []encoding.SeriesIterator,
	symTableNameResolver core.SymbolTableStreamNameResolver,
	symTableFetchFn SymTableFetchFunction,
	startTime xtime.UnixNano,
	endTime xtime.UnixNano,
	doCache bool) *BoostSeriesIterator {

	ret := &BoostSeriesIterator{
		symTableNameResolver: symTableNameResolver,
		symTableFetchFn:      symTableFetchFn,
		symTable:             nil,
		startTime:            startTime,
		endTime:              endTime,
		numRead:              0,
		seriesShardIter:      make([]SeriesShardIterator, 0, len(seriesIterators)),
		orderedShardIndices:  make([]int, 0),
		rowNum:               -1,
		currentTime:          startTime,
		currentIterIndex:     -1,
		preFetchCount:        0,
		iterError:            nil,
		doCache:              doCache,
		isDone:               false,
	}

	for _, seriesIter := range seriesIterators {
		ret.seriesShardIter = append(ret.seriesShardIter, SeriesShardIterator{
			seriesIter:    seriesIter,
			iterIndex:     -1,
			annotation:    nil,
			attributeIter: nil,
			isDone:        false,
			isEOT:         false,
			dpCache:       make([]CacheDpData, 0),
		})
	}

	return ret
}

// Reset the iterator to the beginning of the series, if the iterator type
// supports it.
func (bsi *BoostSeriesIterator) Begin() error {

	// If we have already reached the endy, then re-iteration is not possible
	// unless the caching is enabled.
	if !bsi.doCache && bsi.isDone {
		return errors.New("iterator is a forward-only iterator")
	}

	// We are already there
	//if bsi.currentIterIndex == -1 {
	//	return nil
	//}

	bsi.rowNum = -1
	bsi.currentTime = bsi.startTime
	bsi.currentIterIndex = -1
	bsi.preFetchCount = 0
	bsi.isDone = false
	bsi.numRead = 0

	for i := range bsi.seriesShardIter {
		shardIter := &bsi.seriesShardIter[i]
		shardIter.iterIndex = -1
		shardIter.annotation = nil
		shardIter.attributeIter = nil
		shardIter.isDone = false
	}

	return nil
}

// Moves to the next item
func (bsi *BoostSeriesIterator) Next() bool {

	bsi.numToSkip++
	if bsi.currentIterIndex != -1 {
		bsi.seriesShardIter[bsi.currentIterIndex].hasConsumed = true
	}

	bsi.currentIterIndex = -1

	// If there are still pre-fetched items, then return true
	bsi.preFetchCount--
	if bsi.preFetchCount > 0 {
		return true
	}

	bsi.preFetchCount = 0

	// Do next on all the shard iterators and prefetch some items
	for i := range bsi.seriesShardIter {
		shardIter := &bsi.seriesShardIter[i]
		if shardIter.isDone {
			continue
		}
		shardIter.hasRead = false
		shardIter.hasConsumed = false
		shardIter.attributeIter = nil
		shardIter.annotation = nil

		cacheSize := len(shardIter.dpCache)

		shardIter.iterIndex++

		// If we have a cache & the iterator is within the cache bounds, we don't need to
		// dig into the underlying iterator
		if !bsi.doCache || shardIter.iterIndex >= cacheSize {
			// If End of Time, then we are done
			if shardIter.isEOT {
				shardIter.isDone = true
			} else {
				// Try to get it from the underlying iterator
				shardIter.isDone = !shardIter.seriesIter.Next()
				if shardIter.isDone {
					shardIter.isEOT = true
				}
			}
		}

		if !shardIter.isDone {
			bsi.preFetchCount++
		}
	}

	bsi.isDone = (bsi.preFetchCount == 0)

	return !bsi.isDone
}

// Return the Series Iterator position
func (bsi *BoostSeriesIterator) Position() *BoostSeriesIteratorPosition {
	pos := &BoostSeriesIteratorPosition{
		rowNum:           bsi.rowNum,
		currentIterIndex: bsi.currentIterIndex,
		preFetchCount:    bsi.preFetchCount,
		numToSkip:        bsi.numToSkip,
		sharIterIndices:  make([]ShardIterPosState, len(bsi.seriesShardIter)),
	}

	for i := range bsi.seriesShardIter {
		shardIter := &bsi.seriesShardIter[i]
		pos.sharIterIndices[i].iterIndex = shardIter.iterIndex
		pos.sharIterIndices[i].hasRead = shardIter.hasRead
		pos.sharIterIndices[i].hasConsumed = shardIter.hasConsumed
		pos.sharIterIndices[i].isDone = shardIter.isDone
		pos.sharIterIndices[i].isEOT = shardIter.isEOT
		pos.sharIterIndices[i].dp = shardIter.dp
		pos.sharIterIndices[i].timeUnit = shardIter.timeUnit
		pos.sharIterIndices[i].annotation = shardIter.annotation
	}

	return pos
}

// Seek to the specified position
func (bsi *BoostSeriesIterator) Seek(pos *BoostSeriesIteratorPosition) error {

	// If we have already reached the endy, seeking is not possible
	// unless the caching is enabled.
	if !bsi.doCache && bsi.isDone {
		return errors.New("iterator is a forward-only iterator")
	}

	bsi.rowNum = pos.rowNum
	bsi.currentIterIndex = pos.currentIterIndex
	bsi.preFetchCount = pos.preFetchCount
	bsi.numToSkip = pos.numToSkip

	for i := range bsi.seriesShardIter {
		shardIter := &bsi.seriesShardIter[i]
		shardIter.iterIndex = pos.sharIterIndices[i].iterIndex
		shardIter.hasRead = pos.sharIterIndices[i].hasRead
		shardIter.hasConsumed = pos.sharIterIndices[i].hasConsumed
		shardIter.isDone = pos.sharIterIndices[i].isDone
		shardIter.isEOT = pos.sharIterIndices[i].isEOT
		shardIter.dp = pos.sharIterIndices[i].dp
		shardIter.timeUnit = pos.sharIterIndices[i].timeUnit
		shardIter.annotation = pos.sharIterIndices[i].annotation
	}

	return nil
}

// Returns the current item. Users should not hold onto the returned
// values as it may get invalidated when the Next is called.
func (bsi *BoostSeriesIterator) Current() (
	ts.Datapoint, xtime.Unit, ts.Annotation) {

	bsi.numRead++

	// Go through all the shard iterators and find the one with that is
	// not done and has the time stamp closest to the current time

	// If we have a currentIterIndex, then we already got one
	if bsi.currentIterIndex != -1 {
		return bsi.seriesShardIter[bsi.currentIterIndex].dp, xtime.Nanosecond, bsi.seriesShardIter[bsi.currentIterIndex].annotation
	}

	// Return the item with the smallest timestamp among all the shard iterators
	// Since Next() may have been called numToSkip times, we may have to skip
	// numToSkip next items to get to what we want.

	// If series was already ordered once (from previous iteration),
	// re-use that.
	if bsi.doCache && bsi.rowNum+1 < len(bsi.orderedShardIndices) {
		bsi.rowNum++
		nextIndex := bsi.orderedShardIndices[bsi.rowNum]
		bsi.currentIterIndex = nextIndex

		shardIter := &bsi.seriesShardIter[nextIndex]
		bsi.fetchAndUpdateCache(shardIter)

		return shardIter.dp, xtime.Nanosecond, shardIter.annotation
	}

	// We need to order it
	var nextIndex int

	for {
		nextIndex = -1
		for i := range bsi.seriesShardIter {
			shardIter := &bsi.seriesShardIter[i]

			if shardIter.isDone || shardIter.hasConsumed {
				continue
			}

			// If we have not yet read this shard, then read it
			if !shardIter.hasRead {

				bsi.fetchAndUpdateCache(shardIter)
			}

			if nextIndex == -1 {
				nextIndex = i
			} else {
				if shardIter.dp.TimestampNanos.Before(
					bsi.seriesShardIter[nextIndex].dp.TimestampNanos) {
					nextIndex = i
				}
			}
		}

		// Save the ordering info for this row.
		bsi.currentIterIndex = nextIndex
		bsi.rowNum++
		bsi.orderedShardIndices = append(bsi.orderedShardIndices, nextIndex)

		bsi.numToSkip--
		if bsi.numToSkip == 0 {
			break
		}

		// Skip the next item
		bsi.seriesShardIter[nextIndex].hasConsumed = true
	}

	return bsi.seriesShardIter[nextIndex].dp, xtime.Nanosecond, bsi.seriesShardIter[nextIndex].annotation
}

func (bsi *BoostSeriesIterator) fetchAndUpdateCache(shardIter *SeriesShardIterator) {
	//shardIter.iterIndex++
	cacheSize := len(shardIter.dpCache)
	if bsi.doCache && shardIter.iterIndex < cacheSize {
		shardIter.dp = shardIter.dpCache[shardIter.iterIndex].dp
		shardIter.timeUnit = shardIter.dpCache[shardIter.iterIndex].timeUnit
		shardIter.annotation = shardIter.dpCache[shardIter.iterIndex].annotation
		shardIter.hasRead = true
	} else {
		currDp, tUnit, currAnnotation := shardIter.seriesIter.Current()
		shardIter.dp = currDp
		shardIter.timeUnit = tUnit
		shardIter.annotation = make(ts.Annotation, len(currAnnotation))
		copy(shardIter.annotation, currAnnotation)
		shardIter.hasRead = true
		if bsi.doCache {
			shardIter.dpCache = append(shardIter.dpCache,
				CacheDpData{
					shardIter.dp,
					shardIter.timeUnit,
					make(ts.Annotation, len(currAnnotation)),
				})
			copy(shardIter.dpCache[shardIter.iterIndex].annotation, shardIter.annotation)
		}
	}
}

// Err returns any errors encountered
func (bsi *BoostSeriesIterator) Err() error {
	return bsi.iterError
}

// Return true if the iterator is complete
func (bsi *BoostSeriesIterator) IsDone() bool {
	return bsi.isDone
}

// Close closes the iterator
func (bsi *BoostSeriesIterator) Close() {
	for _, iter := range bsi.seriesShardIter {
		iter.annotation = nil
		iter.attributeIter = nil
		iter.seriesIter.Close()
	}
}

// ID returns the ID of the series
func (bsi *BoostSeriesIterator) ID() ident.ID {
	return bsi.seriesShardIter[0].seriesIter.ID()
}

// Namespace returns the namespace of the series
func (bsi *BoostSeriesIterator) Namespace() ident.ID {
	return bsi.seriesShardIter[0].seriesIter.Namespace()
}

// Tags returns the tags of the series
func (bsi *BoostSeriesIterator) Tags() ident.TagIterator {
	return bsi.seriesShardIter[0].seriesIter.Tags()
}

// Attributes returns the attributes of the series
func (bsi *BoostSeriesIterator) Attributes() ident.TagIterator {

	// If we have not made the first call to Next, then it is an error
	if bsi.currentIterIndex == -1 {
		return nil
	}

	if bsi.seriesShardIter[bsi.currentIterIndex].attributeIter != nil {
		return bsi.seriesShardIter[bsi.currentIterIndex].attributeIter
	}

	annotation := bsi.seriesShardIter[bsi.currentIterIndex].annotation

	if bsi.numRead == 1 {
		println("first read series     : ", bsi.seriesShardIter[bsi.currentIterIndex].seriesIter.ID().String())
		println("first read value      : ", bsi.seriesShardIter[bsi.currentIterIndex].dp.Value)
		println("first read anno size  : ", len(annotation))
		println("first read anno       : ", core.ByteArrayToHex(annotation))
	}

	if len(annotation) == 0 {
		return nil
	}

	// First 2 bytes the version of the symtable
	version := binary.LittleEndian.Uint16(annotation)
	if (bsi.symTable == nil) || (bsi.symTable.Version() != version) {
		symTableName := bsi.symTableNameResolver(bsi.ID())
		symTable, err := bsi.symTableFetchFn(
			bsi.Namespace(),
			symTableName,
			version,
			bsi.startTime,
			bsi.endTime)
		if err != nil {
			return nil
		}
		bsi.symTable = symTable
	}

	indexedHeaderSz := int(binary.LittleEndian.Uint16(annotation[2:]))
	indexedHeader := make([]int, indexedHeaderSz)
	tmp := annotation[4:]
	for i := range indexedHeader {
		indexedHeader[i] = int(binary.LittleEndian.Uint32(tmp[i*4:]))
	}

	attributeMap := bsi.symTable.GetAttributesFromIndexedHeader(indexedHeader)
	attrTags := make([]ident.Tag, len(attributeMap))
	ndx := 0
	for name, value := range attributeMap {
		attrTags[ndx] = ident.Tag{Name: ident.StringID(name), Value: ident.StringID(value)}
		ndx++
	}
	bsi.seriesShardIter[bsi.currentIterIndex].attributeIter = ident.NewTagsIterator(ident.NewTags(attrTags...))
	return bsi.seriesShardIter[bsi.currentIterIndex].attributeIter
}
