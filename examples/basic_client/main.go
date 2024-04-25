// Copyright (c) 2023 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package main

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"time"

	m3client "github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
	"github.com/rmravindran/boostdb/client"
	"github.com/rmravindran/boostdb/query/executor"
	"github.com/rmravindran/boostdb/query/parser"

	yaml "gopkg.in/yaml.v2"
)

const (
	namespace = "metrics_0_30m"
)

var (
	namespaceID = ident.StringID(namespace)
	numUniqHost = 10000
)

type config struct {
	Client m3client.Configuration `yaml:"client"`
}

var configFile = "config.yaml"

func main() {

	cfgBytes, err := os.ReadFile(configFile)
	if err != nil {
		log.Fatalf("unable to read config file: %s, err: %v", configFile, err)
	}

	cfg := &config{}
	if err := yaml.UnmarshalStrict(cfgBytes, cfg); err != nil {
		log.Fatalf("unable to parse YAML: %v", err)
	}

	client, err := cfg.Client.NewClient(m3client.ConfigurationParameters{})
	if err != nil {
		log.Fatalf("unable to create new M3DB client: %v", err)
	}

	fmt.Printf("gomaxprocs %d\n", runtime.GOMAXPROCS(0))

	opt := client.Options()

	opt = opt.SetWriteBatchSize(256).
		SetHostQueueOpsFlushSize(256).
		SetAsyncWriteMaxConcurrency(8096).
		//SetWriteOpPoolSize(8096).
		SetHostQueueOpsArrayPoolSize(128).
		//SetUseV2BatchAPIs(true).
		SetHostQueueOpsFlushInterval(100 * time.Microsecond)
	chOpt := opt.ChannelOptions()
	chOpt.DefaultConnectionOptions.SendBufferSize = 4 * 1024 * 1024
	chOpt.DefaultConnectionOptions.RecvBufferSize = 4 * 1024 * 1024

	log.Printf("AsyncWriteMaxCuncurrency: %d", opt.AsyncWriteMaxConcurrency())
	log.Printf("WriteOpPoolSize: %d", opt.WriteOpPoolSize())
	log.Printf("HostQueueOpsArrayPoolSize: %d", opt.HostQueueOpsArrayPoolSize())
	log.Printf("WriteBatchSize: %d", opt.WriteBatchSize())
	log.Printf("HostQueueOpsFlushSize: %d", opt.HostQueueOpsFlushSize())
	log.Printf("HostQueueOpsFlushInterval: %s", opt.HostQueueOpsFlushInterval().String())
	log.Printf("SendBufferSize: %d", opt.ChannelOptions().DefaultConnectionOptions.SendBufferSize)
	log.Printf("RecvBufferSize: %d", opt.ChannelOptions().DefaultConnectionOptions.RecvBufferSize)

	session, err := client.NewSessionWithOptions(opt)
	if err != nil {
		log.Fatalf("unable to create new M3DB session: %v", err)
	}

	defer session.Close()

	log.Printf("------ run tagged attribute example ------")

	f, err := os.Create("cpu_profile.prof")
	if err != nil {
		log.Fatal(err)
	}
	pprof.StartCPUProfile(f)
	runtime.SetCPUProfileRate(100)
	defer pprof.StopCPUProfile()

	// Write and read large table data
	writeAndReadLargeTableData(session, 10000)
}

func writeAndReadLargeTableData(session m3client.Session, count int) {

	// Create session with large number of attribute values
	var maxConcurrentWrites uint32 = 512

	boostSession := client.NewBoostSession(
		session,
		1000,
		maxConcurrentWrites)

	// Create new M3DBTapTable
	sf := client.NewM3DBSeriesFamily(
		"myAppSF",
		"myAppDomain",
		namespaceID,
		1,
		boostSession,
		getDistributionFactor(namespace, "myAppDomain", "myAppSF"),
		100000000,
		maxConcurrentWrites)

	var (
		seriesID = ident.StringID("cpu_user_util_myservice1")
		tags     = []ident.Tag{
			{Name: ident.StringID("region"), Value: ident.StringID("us-east-1")},
			{Name: ident.StringID("service"), Value: ident.StringID("myservice1")},
		}
		tagsIter = ident.NewTagsIterator(ident.NewTags(tags...))
	)

	// Write with fresh dictionary
	writeSF(sf, seriesID, tagsIter, count)

	// Write with reuse of the dictionary
	log.Printf("Write with reuse of the dictionary")
	start, end := writeSF(sf, seriesID, tagsIter, count)
	time.Sleep(5000 * time.Millisecond)

	readUsingSQL(boostSession, "myAppSF", "cpu_user_util_myservice1", start, end, count)

	// Now read back and check the values are in order.
	//readSF(sf, seriesID, start, end)
}

// Return the distribution factor for the series
func getDistributionFactor(namespace string, domain string, seriesFamily string) uint16 {
	return 64
}

func readUsingSQL(
	boostSession *client.BoostSession,
	seriesFamily string,
	seriesName string,
	startTime xtime.UnixNano,
	endTime xtime.UnixNano,
	maxValues int) {

	defer timer("read-large-series-with-attributes-using-sql")()
	log.Printf("------ read large data (with attributes) using sql from db ------")

	sqlQuery := fmt.Sprintf("SELECT %s.host, %s FROM myAppDomain.%s", seriesName, seriesName, seriesFamily)
	//sqlQuery := fmt.Sprintf("SELECT %s.host, %s FROM myAppDomain.%s WHERE %s < 100.0", seriesName, seriesName, seriesFamily, seriesName)

	parser := parser.NewParser()
	queryOps, err := parser.Parse(sqlQuery)
	if err != nil {
		log.Fatalf("unable to parse query: %v", err)
		return
	}

	planner := executor.NewPlanner()
	plan := planner.GeneratePlan(queryOps)
	if plan == nil {
		log.Fatalf("unable to generate query execution plan")
		return
	}

	// Create a new executor
	// TODO: Change executor to take in the boostsession as input
	executor := executor.NewExecutor(
		namespace,
		"myAppDomain",
		getDistributionFactor,
		plan,
		boostSession,
		startTime,
		endTime,
		time.Duration(time.Millisecond*500),
		10000)

	expVal := 1.0
	globalRowCounter := 0
	for {
		err, hasResult := executor.Execute()
		if err != nil {
			log.Fatalf("error executing the read query")
		}

		// Nothing more to do
		if !hasResult {
			break
		}

		result, err := executor.ResultSet()
		if err != nil {
			log.Fatalf("error executing the read query")
		}
		for row := 0; row < result.NumRows(); row++ {
			col0Value, _ := result.GetString(row, 0)
			col1Value, _ := result.GetFloat(row, 1)
			if col1Value != expVal {
				log.Fatalf("unexpected value: found %v but expected %v", col1Value, expVal)
			}

			hostNum := globalRowCounter % numUniqHost
			hostName := fmt.Sprintf("host-%07d", hostNum)
			if col0Value != hostName {
				log.Fatalf("unexpected value: found %v but expected %v", col0Value, hostName)
			}

			expVal++
			globalRowCounter++
		}

		if globalRowCounter == maxValues {
			log.Printf("reached end")
		}
	}
}

func writeSF(sf *client.M3DBSeriesFamily,
	seriesID ident.ID,
	tagsIter ident.TagIterator,
	count int) (xtime.UnixNano, xtime.UnixNano) {
	defer timer("write-large-series-m3db-table")()
	start := xtime.Now()
	writtenValue := 1.0

	// Same series, but with 1000000 attributes
	for i := 0; i < count; i++ {
		hostNum := i % numUniqHost
		hostName := fmt.Sprintf("host-%07d", hostNum)
		attributes := []ident.Tag{
			{Name: ident.StringID("host"), Value: ident.StringID(hostName)},
		}
		attrIter := ident.NewTagsIterator(ident.NewTags(attributes...))

		// Write a ts value with tags and attributes
		timestamp := xtime.Now()
		err := sf.WriteTagged(
			seriesID,
			tagsIter,
			attrIter,
			timestamp,
			writtenValue,
			xtime.Nanosecond,
			nil)
		if err != nil {
			log.Fatalf("error writing series %s, err: %v", seriesID.String(), err)
		}
		writtenValue++
	}
	errWait := sf.Wait(5 * time.Second)
	if errWait != nil {
		log.Fatalf("error waiting for pending writes to complete: %v", errWait)
	}
	end := xtime.Now()

	return start, end
}

func readSF(sf *client.M3DBSeriesFamily,
	seriesID ident.ID,
	start xtime.UnixNano,
	end xtime.UnixNano,
	maxValues int) {
	defer timer("read-large-series-with-attributes")()
	log.Printf("------ read large data (with attributes) from db ------")

	expVal := 1.0
	// Now lets read the series back out
	seriesIter, err := sf.Fetch(
		seriesID,
		start.Add(-time.Millisecond*5), // Adjust by 5 milliseconds
		end,
		true)
	if err != nil {
		log.Fatalf("error fetching data for untagged series: %v", err)
	}
	ndx := 0
	for seriesIter.Next() {
		dp, _, _ := seriesIter.Current()
		if ndx == maxValues {
			log.Printf("reached end")
		}
		if dp.Value != expVal {
			log.Fatalf("unexpected value: found %v but expected %v", dp.Value, expVal)
		}
		expVal++
		//log.Printf("Series Value %s: %v", dp.TimestampNanos.String(), dp.Value)

		// Lets also print out the tags and attributes
		tags := seriesIter.Tags()
		for tags.Next() {
			tag := tags.Current()
			log.Printf("Tag %s=%s", tag.Name.String(), tag.Value.String())
		}

		attributes := seriesIter.Attributes()
		if attributes.Next() {
			attribute := attributes.Current()
			hostName := fmt.Sprintf("host-%07d", ndx)
			if (attribute.Name.String() != "host") || (attribute.Value.String() != hostName) {
				log.Fatalf("unexpected attribute: found %s=%s but expected host=%s", attribute.Name.String(), attribute.Value.String(), hostName)
			}
		}
		ndx++
	}

	if err := seriesIter.Err(); err != nil {
		log.Fatalf("error in series iterator: %v", err)
	}

}

func timer(name string) func() {
	start := time.Now()
	return func() {
		fmt.Printf("%s took %v\n", name, time.Since(start))
	}
}
