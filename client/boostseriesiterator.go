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

		// If we have a cache, we go after it first
		if bsi.doCache && shardIter.iterIndex < cacheSize {
			//if shardIter.isCacheReady {
			//shardIter.iterIndex++
			//if cacheSize == shardIter.iterIndex {
			//	shardIter.isDone = true
			//}
		} else {
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

// Returns the current item. Users should not hold onto the returned
// values as it may get invalidated when the Next is called.
func (bsi *BoostSeriesIterator) Current() (
	ts.Datapoint, xtime.Unit, ts.Annotation) {

	// Go through all the shard iterators and find the one with that is
	// not done and has the time stamp closest to the current time

	// If we have a currentIterIndex, then we already got one
	if bsi.currentIterIndex != -1 {
		return bsi.seriesShardIter[bsi.currentIterIndex].dp, xtime.Nanosecond, bsi.seriesShardIter[bsi.currentIterIndex].annotation
	}

	// Return the item with the smallest timestamp among all the shard iterators
	// Since Next() may have been called numToSkip times, we may have to skip
	// numToSkip next items to get to what we want.
	var nextIndex int

	// If series was already ordered once (from previous iteration),
	// re-use that.
	if bsi.doCache && bsi.rowNum+1 < len(bsi.orderedShardIndices) {
		bsi.rowNum++
		nextIndex = bsi.orderedShardIndices[bsi.rowNum]
		bsi.currentIterIndex = nextIndex

		shardIter := &bsi.seriesShardIter[nextIndex]
		bsi.fetchAndUpdateCache(shardIter)

		return shardIter.dp, xtime.Nanosecond, shardIter.annotation
	}

	// We need to order it

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
				/*

					// If the shard has cached info, then we can use that.
					//cacheSize := len(shardIter.dpCache)
					if shardIter.isCacheReady {
						shardIter.dp = shardIter.dpCache[shardIter.iterIndex].dp
						shardIter.timeUnit = shardIter.dpCache[shardIter.iterIndex].timeUnit
						shardIter.annotation = shardIter.dpCache[shardIter.iterIndex].annotation
						shardIter.hasRead = true
					} else {
						currDp, tUnit, currAnnotation := shardIter.seriesIter.Current()
						shardIter.dp = currDp
						shardIter.timeUnit = tUnit
						shardIter.annotation = currAnnotation
						shardIter.hasRead = true
						if bsi.doCache {
							shardIter.dpCache = append(shardIter.dpCache,
								CacheDpData{
									shardIter.dp,
									shardIter.timeUnit,
									make(ts.Annotation, 0, len(currAnnotation)),
								})
							shardIter.iterIndex++
							shardIter.dpCache[shardIter.iterIndex].annotation = shardIter.annotation
						}
					}
				*/
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
		shardIter.annotation = currAnnotation
		shardIter.hasRead = true
		if bsi.doCache {
			shardIter.dpCache = append(shardIter.dpCache,
				CacheDpData{
					shardIter.dp,
					shardIter.timeUnit,
					make(ts.Annotation, 0, len(currAnnotation)),
				})
			shardIter.dpCache[shardIter.iterIndex].annotation = shardIter.annotation
		}
	}
}

// Err returns any errors encountered
func (bsi *BoostSeriesIterator) Err() error {
	return bsi.iterError
}

// Begin
func (bsi *BoostSeriesIterator) Begin() error {

	// If we have already reached the endy, then re-iteration is not possible
	// unless the caching is enabled.
	if !bsi.doCache && bsi.isDone {
		return errors.New("iterator cannot be re-used")
	}

	// We are already there
	if bsi.currentIterIndex == -1 {
		return nil
	}

	bsi.rowNum = -1
	bsi.currentTime = bsi.startTime
	bsi.currentIterIndex = -1
	bsi.preFetchCount = 0
	bsi.isDone = false

	for i := range bsi.seriesShardIter {
		shardIter := &bsi.seriesShardIter[i]
		shardIter.iterIndex = -1
		shardIter.annotation = nil
		shardIter.attributeIter = nil
		shardIter.isDone = false
	}

	return nil
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

	// First 2 bytes the version of the symtable
	version := binary.LittleEndian.Uint16(annotation)
	if (bsi.symTable == nil) || (bsi.symTable.Version() != version) {
		symTableName := bsi.symTableNameResolver(bsi.ID())
		//symTableName := core.GetSymbolTableName(bsi.ID().String())
		//symTableName := "m3_symboltable_" + bsi.ID().String()
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
