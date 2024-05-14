package executor

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"testing"
	"time"

	m3client "github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
	"github.com/rmravindran/boostdb/client"
	"github.com/rmravindran/boostdb/query/parser"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

type config struct {
	Client m3client.Configuration `yaml:"client"`
}

type ExecutorTestCommon struct {
	namespaceID         ident.ID
	m3Session           m3client.Session
	client              m3client.Client
	version             uint16
	domain              string
	seriesFamily        string
	distributionFactor  uint16
	configFile          string
	readBatchWindowSize time.Duration
	readBatchSize       int
	maxConcurrentWrites uint32
	maxSymTables        int
	dictionaryLimit     uint32
	config              *config
	boostSession        *client.BoostSession
}

// NewExecutorTestCommon creates a new ExecutorTestCommon
func NewExecutorTestCommon() *ExecutorTestCommon {
	return &ExecutorTestCommon{
		namespaceID:         ident.StringID("metrics_0_30m"),
		version:             1,
		domain:              "bTestDomain",
		seriesFamily:        "bTestSF",
		distributionFactor:  64,
		configFile:          "../../testdata/executordata/config.yaml",
		readBatchWindowSize: time.Duration(200 * time.Millisecond),
		readBatchSize:       1000,
		maxConcurrentWrites: 512,
		maxSymTables:        1000,
		dictionaryLimit:     10000000,
		config:              &config{},
		boostSession:        nil,
	}
}

// Initialize the executor commons
func (c *ExecutorTestCommon) Init() {
	cfgBytes, err := os.ReadFile(c.configFile)
	if err != nil {
		log.Fatalf("unable to read config file: %s, err: %v", c.configFile, err)
	}

	if err := yaml.UnmarshalStrict(cfgBytes, c.config); err != nil {
		log.Fatalf("unable to parse YAML: %v", err)
	}

	c.client, err = c.config.Client.NewClient(m3client.ConfigurationParameters{})
	if err != nil {
		log.Fatalf("unable to create new M3DB client: %v", err)
	}

	fmt.Printf("gomaxprocs %d\n", runtime.GOMAXPROCS(0))

	opt := c.client.Options()

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

	c.m3Session, err = c.client.NewSessionWithOptions(opt)
	if err != nil {
		log.Fatalf("unable to create new M3DB session: %v", err)
	}

	c.boostSession = client.NewBoostSession(
		c.m3Session,
		c.maxSymTables,
		c.maxConcurrentWrites)
}

// Return the distribution factor for the series
func (c *ExecutorTestCommon) getDistributionFactorFn(namespace string, domain string, seriesFamily string) uint16 {
	return c.distributionFactor
}

// Close the executor commons
func (c *ExecutorTestCommon) Close() {
	c.m3Session.Close()
}

// Return the seriesId and tags iterator for the cpu_utilization series
func (c *ExecutorTestCommon) getCPUUtilSeriesIDAndTags(seriesName string) (
	ident.ID,
	ident.TagIterator) {

	tags := ident.NewTagsIterator(ident.NewTags(
		ident.StringTag("dc", "dc1"),
		ident.StringTag("env", "test"),
	))
	return ident.StringID(seriesName), tags
}

// Unit test simple executor
func TestExecutor_SimpleSelect(t *testing.T) {

	// Create a new executor test common
	c := NewExecutorTestCommon()
	defer c.Close()

	// Initialize the executor test common
	c.Init()

	// Create new M3DBTapTable
	sf := client.NewM3DBSeriesFamily(
		c.seriesFamily,
		c.domain,
		c.namespaceID,
		c.version,
		c.boostSession,
		c.distributionFactor,
		c.dictionaryLimit,
		c.maxConcurrentWrites)

	seriesName := "cpu_utilization"
	seriesID, tags := c.getCPUUtilSeriesIDAndTags(seriesName)

	// Record the start time. We need this when we are quering the series
	startTime := xtime.Now()

	// Write 10 values to the series with hostname and value attributes
	for i := 0; i < 10; i++ {

		attribs := ident.NewTagsIterator(ident.NewTags(
			ident.StringTag("host", fmt.Sprintf("host-%07d", i+1)),
		))

		err := sf.WriteTagged(
			seriesID,
			tags,
			attribs,
			xtime.Now(),
			float64(i+1),
			xtime.Nanosecond,
			nil)
		require.Nilf(t, err, "unable to write to series: %v", err)
	}

	// Wait at most 3 seconds for all pending writes to complete
	errWait := sf.Wait(3 * time.Second)
	if errWait != nil {
		log.Fatalf("error waiting for pending writes to complete: %v", errWait)
	}
	endTime := xtime.Now()
	//time.Sleep(5000 * time.Millisecond)

	// Simple query
	sqlQuery := fmt.Sprintf("SELECT %s.host, %s FROM %s.%s WHERE %s < 100.0", seriesName, seriesName, c.domain, c.seriesFamily, seriesName)

	// Parse the query
	parser := parser.NewParser()
	queryOps, err := parser.Parse(sqlQuery)
	require.Nilf(t, err, "unable to parse query: %v", err)

	// Generate the query execution plan
	planner := NewPlanner()
	plan := planner.GeneratePlan(queryOps)
	require.NotNilf(t, plan, "unable to generate query execution plan")

	// Create an executor
	executor := NewExecutor(
		c.namespaceID.String(),
		c.domain,
		c.getDistributionFactorFn,
		plan,
		c.boostSession,
		startTime,
		endTime,
		c.readBatchWindowSize,
		c.readBatchSize)

	// Execute the query
	err, hasResult := executor.Execute()
	require.Nilf(t, err, "unable to execute query: %v", err)
	require.Truef(t, hasResult, "no result returned")

	// Get ResultSet and check the values
	result, err := executor.ResultSet()
	require.Nilf(t, err, "unable to get ResultSet: %v", err)
	require.Equal(t, result.NumRows(), 10)
	require.Equal(t, result.NumCols(), 2)
	for row := 0; row < result.NumRows(); row++ {
		testHost, err := result.GetString(row, 0)
		require.Nilf(t, err, "unable to get value for column at index 0: %v", err)

		testValue, err := result.GetFloat(row, 1)
		require.Nilf(t, err, "unable to get value for column at index 1: %v", err)

		expectedHost := fmt.Sprintf("host-%07d", row+1)
		require.Equal(t, expectedHost, testHost)
		require.Equal(t, float64(row+1), testValue)
	}
}
