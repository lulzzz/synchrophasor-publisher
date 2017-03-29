package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"runtime/pprof"
	"strconv"
	"strings"
	"time"

	"runtime"

	"github.com/golang/glog"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/michaeldye/synchrophasor-proto/pmu_server"
	"github.com/michaeldye/synchrophasor-proto/synchrophasor_dpe"
)

const (
	defaultPMUServerAddress = "pmu:8008"

	defaultDPEServerAddress = "dpe:9009"

	dpeReconnectionDelayS = 20

	// number of separate publishing threads / maintained connections with destination service
	numPublishers = 10

	// buffers are managed by publishers and (if failure occurs) written to disk
	bufferLength = 1000

	// When max buffers is reached, the system will *drop* new samples; it's expected that if the machine this is on is sufficiently spec'd, and the volume of input data is not overwhelming, this system will publish samples to a configured service or write the samples to disk before dropping is necessary.
	//maxBuffers = bufferLength * 10

	inputBufferMax = 2e6

	// after this threshhold, failed message sends will get dropped (as whole buffers) before they are written to disk; writing to disk is a single-worker, bound task on purpose
	failSendMaxBuffers = 2e6

	// max channel size for already-cached failed sends; OK for these to get dropped, the source will re-read them from disk periodically and retry them
	retrySendMaxBuffers = 2e2

	operationSampleMax = 2e4
)

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

type buffer struct {
	writePtr int
	data     [bufferLength]*pmu_server.SynchrophasorDatum
}

func newBuffer() *buffer {
	return &buffer{
		writePtr: 0,
		data:     [bufferLength]*pmu_server.SynchrophasorDatum{},
	}
}

// returns -1 if write not possible b/c buffer is full, otherwise returns index of written location
func (b *buffer) write(datumP *pmu_server.SynchrophasorDatum) int {
	if b.isFull() {
		return -1
	}

	b.data[b.writePtr] = datumP
	b.writePtr++
	return b.writePtr
}

func (b *buffer) isFull() bool {
	return b.writePtr == bufferLength
}

func (b *buffer) isEmpty() bool {
	return b.writePtr == 0
}

func (b *buffer) remove(idx int) {
	b.data[idx] = nil
	b.writePtr--
}

type failMeta struct {
	err   error
	errTs uint64
	// errDataIndices []int64 // the indices in the sparse data slice to which this error info applies
	// errDataIDs []int64 // the datum IDs in the sparse data slice to which this error info applies
}

type dataSendFailure struct {
	// sparse slice of failed send data
	buffer   *buffer
	failMeta []*failMeta
	retries  int
}

type failCache struct {
	failed           dataSendFailure
	cachedBufferPath string
}

type operationSample struct {
	successfulSentBuffers int
}

func main() {
	flag.Parse()

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			panic(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	pmuServerAddress := os.Getenv("PMU_SERVER_ADDRESS")
	if pmuServerAddress == "" {
		pmuServerAddress = defaultPMUServerAddress
	}

	dpeServerAddress := os.Getenv("DPE_SERVER_ADDRESS")
	if dpeServerAddress == "" {
		dpeServerAddress = defaultDPEServerAddress
	}

	// a non-blocking, async publish channel; input read from gRPC is written directly to it
	publishChan := make(chan *pmu_server.SynchrophasorDatum, inputBufferMax)

	// a non-blocking, async single channel for publishers to write failed sends to or succeeded retries to; failed sends might be retried again or written to disk if they haven't already been written
	publishFailedChan := make(chan *failCache, failSendMaxBuffers)

	// a non-blocking, async publish channel; output read by multiple publishers, input from single cacheManager
	cachedAndRetryChan := make(chan *failCache, retrySendMaxBuffers)

	operationSampleChan := make(chan *operationSample, operationSampleMax)

	// reads from publishChan and cachedAndRetryChan (preferring first) and writes to failedSend
	startPublishers(dpeServerAddress, publishChan, cachedAndRetryChan, publishFailedChan, operationSampleChan)

	// reads from publishFailedChan (failed sends *and* previously failed, but now successful sends) and writes to cachedAndRetryChan
	go cacheManager(publishFailedChan, cachedAndRetryChan)

	// reads from initial input source and writes data to publishers
	go subscriber(pmuServerAddress, publishChan)

	// reads from publishFailed, cachedAndRetry, and operationSample chans to form stats picture, then try publishing

	reportStats(publishFailedChan, cachedAndRetryChan, operationSampleChan)

	//plan:
	// create two channels: outbound to publishers, one inbound for failed sends
	// start publisher pool (reads from publisher channel, writes to failed)
	// start single fs writer (reads from failed, writes to disk)
	// start single fs reader (writes to publisher channel, scans disk and reads from it)

}

func reportStats(publishFailedChan <-chan *failCache, cachedAndRetryChan <-chan *failCache, operationSampleChan <-chan *operationSample) {
	for {
		glog.Infof("Gonna report some mean stats...")

		time.Sleep(10 * time.Second)
	}
}

func read(client pmu_server.SynchrophasorDataClient, publishChan chan<- *pmu_server.SynchrophasorDatum) error {
	pmuStream, err := client.Sample(context.Background(), &pmu_server.SamplingFilter{})
	if err != nil {
		glog.Fatalf("Error establishing stream from PMU server: %v", err)
	}

	for {
		datum, err := pmuStream.Recv()
		if err == io.EOF {
			return fmt.Errorf("Server sent EOF")
		}

		if err != nil {
			return fmt.Errorf("%v.Sample(_) = _, %v", pmuStream, err)
		}

		publishChan <- datum
		runtime.Gosched()
	}
}

func pmuConnect(pmuServerAddress string) (pmu_server.SynchrophasorDataClient, *grpc.ClientConn, error) {
	conn, err := grpc.Dial(pmuServerAddress, grpc.WithInsecure(), grpc.WithTimeout(time.Duration(20)*time.Second), grpc.WithBlock())
	if err != nil {
		return nil, nil, err
	}

	client := pmu_server.NewSynchrophasorDataClient(conn)
	return client, conn, nil
}

func subscriber(pmuServerAddress string, publishChan chan<- *pmu_server.SynchrophasorDatum) {
	for {
		glog.Infof("Connecting to %v", pmuServerAddress)
		pmuClient, pmuConn, err := pmuConnect(pmuServerAddress)
		if err != nil {
			glog.Errorf("Error connecting to PMU server: %v", err)
		}

		if pmuClient != nil {
			// this call blocks until the stream ends; that's an error in our system
			if err := read(pmuClient, publishChan); err != nil {
				glog.Errorf("Error reading from server: %v", err)
			}

			glog.Infof("Attempting to close PMU client connection before trying to reconnect to the server")
			pmuConn.Close()
		}

		// delay reconnection
		time.Sleep(10 * time.Second)
	}
}

func cacheManager(publishFailedChan <-chan *failCache, cachedAndRetryChan chan<- *failCache) {

	// for now, just copy if still failed
	for {
		select {
		case failCache := <-publishFailedChan:
			if !failCache.failed.buffer.isEmpty() {
				(*failCache).failed.retries++
				cachedAndRetryChan <- failCache
			}
		}

		runtime.Gosched()
	}
}

func startPublishers(dpeServerAddress string, publishChan <-chan *pmu_server.SynchrophasorDatum, cachedAndRetryChan <-chan *failCache, publishFailedChan chan<- *failCache, operationSampleChan chan<- *operationSample) {

	// use 1 so publisher id output doesn't look as funny
	for ix := 1; ix <= numPublishers; ix++ {
		go connectAndPublish(ix, dpeServerAddress, publishChan, cachedAndRetryChan, publishFailedChan, operationSampleChan)
	}
}

func dpeConnect(dpeServerAddress string) (synchrophasor_dpe.SynchrophasorDPEClient, *grpc.ClientConn, error) {
	conn, err := grpc.Dial(dpeServerAddress, grpc.WithInsecure(), grpc.WithTimeout(time.Duration(20)*time.Second), grpc.WithBlock())
	if err != nil {
		return nil, nil, err
	}

	client := synchrophasor_dpe.NewSynchrophasorDPEClient(conn)
	return client, conn, nil
}

func connectAndPublish(id int, dpeServerAddress string, publishChan <-chan *pmu_server.SynchrophasorDatum, cachedAndRetryChan <-chan *failCache, publishFailedChan chan<- *failCache, operationSampleChan chan<- *operationSample) {
	glog.Infof("Started publisher %v", id)

	lat := os.Getenv("HZN_LAT")
	lon := os.Getenv("HZN_LON")
	agreementID := os.Getenv("HZN_AGREEMENTID")
	deviceID := os.Getenv("DEVICE_ID")
	haPartners := strings.Split(os.Getenv("HZN_HA_PARTNERS"), ",")

	// do conversions
	latF, _ := strconv.ParseFloat(lat, 32)
	lonF, _ := strconv.ParseFloat(lon, 32)

	send := func(datum *pmu_server.SynchrophasorDatum, stream synchrophasor_dpe.SynchrophasorDPE_StoreClient) error {

		glog.V(6).Infof("Publishing datum: %v", datum)

		if stream != nil {
			if err := stream.Send(&synchrophasor_dpe.HorizonDatumWrapper{
				Type:        "synchrophasor",
				Lat:         float32(latF),
				Lon:         float32(lonF),
				DeviceID:    deviceID,
				AgreementID: agreementID,
				Datum:       datum,
				HAPartners:  haPartners,
			}); err != nil {
				return fmt.Errorf("Error sending data to stream: %v", err)
			}
		}

		return nil
	}

	var dpeClient synchrophasor_dpe.SynchrophasorDPEClient
	var dpeConn *grpc.ClientConn
	var stream synchrophasor_dpe.SynchrophasorDPE_StoreClient

	pubError := func(unsentDatum *pmu_server.SynchrophasorDatum) {
		// TODO: handle requeue; that involves buffering failures up then publishing to the channel; could be simpler if necessary
		glog.Errorf("Failed to send datum: %v", unsentDatum)

		//TODO: replace this with the gRPC backoff handling
		// delay this publisher's reconnection reconnection
		time.Sleep(dpeReconnectionDelayS * time.Second)

		// trying to clean up
		stream = nil
		if dpeConn != nil {
			dpeConn.Close()
		}
		dpeClient = nil
	}

	for {
		select {
		case datum := <-publishChan:

			if stream == nil {
				// set up the stream

				if dpeClient == nil {
					if cl, co, err := dpeConnect(dpeServerAddress); err != nil {
						glog.Errorf("Error connecting to DPE server: %v", err)
						pubError(datum)
						continue

					} else {
						dpeClient = cl
						dpeConn = co
					}
				}

				st, err := dpeClient.Store(context.Background())
				if err != nil {
					glog.Errorf("Error calling Store on DPE server RPC: %v", err)
					pubError(datum)

				} else {
					stream = st
				}
			}

			if err := send(datum, stream); err != nil {
				// TODO: handle requeue
				pubError(datum)
			}
		}

		// forevar !
		runtime.Gosched()
	}
}
