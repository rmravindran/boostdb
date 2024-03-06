package client

import (
	"encoding/binary"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
	"github.com/rmravindran/boostdb/core"
)

type SymTableFetchFunction func(
	namespaceId ident.ID,
	symTableName string,
	version uint16,
	timeBegin xtime.UnixNano,
	timeEnd xtime.UnixNano) (*core.SymTable, error)

type SeriesShardIterator struct {
	seriesIter    encoding.SeriesIterator
	dp            ts.Datapoint
	timeUnit      xtime.Unit
	annotation    ts.Annotation
	attributeIter ident.TagIterator
	hasRead       bool
	hasConsumed   bool
	isDone        bool
}

type BoostSeriesIterator struct {
	symTableNameResolver core.SymbolTableStreamNameResolver
	symTableFetchFn      SymTableFetchFunction
	symTable             *core.SymTable
	startTime            xtime.UnixNano
	endTime              xtime.UnixNano
	seriesShardIter      []SeriesShardIterator
	currentTime          xtime.UnixNano
	currentIterIndex     int
	preFetchCount        int
	numToSkip            int
	iterError            error
}

// NewBoostSeriesIterator returns a new series iterator
func NewBoostSeriesIterator(
	seriesIterators []encoding.SeriesIterator,
	symTableNameResolver core.SymbolTableStreamNameResolver,
	symTableFetchFn SymTableFetchFunction,
	startTime xtime.UnixNano,
	endTime xtime.UnixNano) *BoostSeriesIterator {

	ret := &BoostSeriesIterator{
		symTableNameResolver: symTableNameResolver,
		symTableFetchFn:      symTableFetchFn,
		symTable:             nil,
		startTime:            startTime,
		endTime:              endTime,
		seriesShardIter:      make([]SeriesShardIterator, 0, len(seriesIterators)),
		currentTime:          startTime,
		currentIterIndex:     -1,
		preFetchCount:        0,
		iterError:            nil,
	}

	for _, seriesIter := range seriesIterators {
		ret.seriesShardIter = append(ret.seriesShardIter, SeriesShardIterator{
			seriesIter:    seriesIter,
			annotation:    nil,
			attributeIter: nil,
			isDone:        false,
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
		if bsi.seriesShardIter[i].isDone {
			continue
		}
		bsi.seriesShardIter[i].hasRead = false
		bsi.seriesShardIter[i].hasConsumed = false
		bsi.seriesShardIter[i].attributeIter = nil
		bsi.seriesShardIter[i].annotation = nil
		bsi.seriesShardIter[i].isDone = !bsi.seriesShardIter[i].seriesIter.Next()
		if !bsi.seriesShardIter[i].isDone {
			bsi.preFetchCount++
		}
	}

	return bsi.preFetchCount > 0
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
	for {
		nextIndex = -1
		for i := range bsi.seriesShardIter {
			if bsi.seriesShardIter[i].isDone || bsi.seriesShardIter[i].hasConsumed {
				continue
			}

			// If we have not yet read this shard, then read it
			if !bsi.seriesShardIter[i].hasRead {
				currDp, tUnit, currAnnotation := bsi.seriesShardIter[i].seriesIter.Current()
				bsi.seriesShardIter[i].dp = currDp
				bsi.seriesShardIter[i].timeUnit = tUnit
				bsi.seriesShardIter[i].annotation = currAnnotation
				bsi.seriesShardIter[i].hasRead = true
			}

			if nextIndex == -1 {
				nextIndex = i
			} else {
				if bsi.seriesShardIter[i].dp.TimestampNanos.Before(
					bsi.seriesShardIter[nextIndex].dp.TimestampNanos) {
					nextIndex = i
				}
			}
		}

		bsi.numToSkip--
		if bsi.numToSkip == 0 {
			break
		}
		// Skip the next item
		bsi.seriesShardIter[nextIndex].hasConsumed = true
	}
	bsi.currentIterIndex = nextIndex
	return bsi.seriesShardIter[nextIndex].dp, xtime.Nanosecond, bsi.seriesShardIter[nextIndex].annotation
}

// Err returns any errors encountered
func (bsi *BoostSeriesIterator) Err() error {
	return bsi.iterError
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
