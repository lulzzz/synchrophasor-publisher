package main

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"time"

	"github.com/golang/glog"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/keepalive"

	"github.com/michaeldye/synchrophasor-proto/pmu_server"
	"github.com/michaeldye/synchrophasor-proto/synchrophasor_dpe"
)

const (
	defaultPMUServerAddress = "pmu:8008"

	defaultDPEServerAddress = "dpe:9009"

	workloadVersionFileName = "VERSION"

	// number of separate publishing threads / maintained connections with destination service
	numPublishers = 1

	// buffers are managed by publishers and (if failure occurs) written to disk
	bufferLength = 50

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

// written on first start or system bails
var workloadVersion string

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
}

type failCache struct {
	failed           *dataSendFailure
	cachedBufferPath string
}

type operationSample struct {
	successfulSentBuffers int
}

func workloadVersionFromFile() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}

	fileContent, err := ioutil.ReadFile(filepath.Join(dir, workloadVersionFileName))
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(string(fileContent)), nil
}

func main() {
	flag.Parse()

	// disable gRPC logging by default
	grpclog.SetLogger(log.New(ioutil.Discard, "", 0))

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			panic(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	workloadVersion := os.Getenv("WORKLOAD_VERSION")
	if workloadVersion != "" {
		glog.Infof("Set workload version from envvar to: %v", workloadVersion)
	} else {
		wv, err := workloadVersionFromFile()
		if err != nil {
			glog.Errorf("Unable to read workload version from file. Error: %v", err)
		}
		workloadVersion = wv
		glog.Infof("Set workload version from VERSION file to: %v", workloadVersion)
	}

	// bail if workloadVersion not yet set
	if workloadVersion == "" {
		panic("Unable to determine workload version. Either deploy this executable with a sibling VERSION file or override with the WORKLOAD_VERSION environment variable")
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
	publishFailedChan := make(chan *pmu_server.SynchrophasorDatum, failSendMaxBuffers)

	// a non-blocking, async publish channel; output read by multiple publishers, input from single cacheManager
	cachedAndRetryChan := make(chan *pmu_server.SynchrophasorDatum, retrySendMaxBuffers)

	operationSampleChan := make(chan *operationSample, operationSampleMax)

	// reads from publishChan and cachedAndRetryChan (preferring first) and writes to failedSend
	startPublishers(dpeServerAddress, publishChan, cachedAndRetryChan, publishFailedChan, operationSampleChan)

	// reads from initial input source and writes data to publishers
	go subscriber(pmuServerAddress, publishChan)

	// reads from publishFailed, cachedAndRetry, and operationSample chans to form stats picture, then try publishing

	// reads from publishFailedChan (failed sends *and* previously failed, but now successful sends) and writes to cachedAndRetryChan
	cacheManager(publishFailedChan, cachedAndRetryChan, operationSampleChan)

	//plan:
	// create two channels: outbound to publishers, one inbound for failed sends
	// start publisher pool (reads from publisher channel, writes to failed)
	// start single fs writer (reads from failed, writes to disk)
	// start single fs reader (writes to publisher channel, scans disk and reads from it)

}

func cacheManager(publishFailedChan <-chan *pmu_server.SynchrophasorDatum, cachedAndRetryChan chan<- *pmu_server.SynchrophasorDatum, operationSampleChan <-chan *operationSample) {
	// for now, just copy if still failed
	for {
		select {
		case failedDatum := <-publishFailedChan:
			glog.Info("Found previously-failed record, adding to cache for later retry")
			cachedAndRetryChan <- failedDatum

		default:
			// nothing to process
		}

		time.Sleep(1 * time.Millisecond)
	}
}

func read(client pmu_server.SynchrophasorDataClient, publishChan chan<- *pmu_server.SynchrophasorDatum) error {
	pmuStream, err := client.Sample(context.Background(), &pmu_server.SamplingFilter{})
	if err != nil {
		glog.Fatalf("Error establishing stream from PMU server: %v", err)
	}

	glog.Infof("Streaming data from /pmu_server.SynchrophasorData/Sample")

	for {
		datum, err := pmuStream.Recv()
		if err == io.EOF {
			return fmt.Errorf("Server sent EOF")
		}

		if err != nil {
			return fmt.Errorf("%v.Sample(_) = _, %v", pmuStream, err)
		}

		glog.V(6).Infof("Read datum with ts: %v", datum.Ts)
		publishChan <- datum
		runtime.Gosched()
	}
}

func pmuConnect(pmuServerAddress string) (pmu_server.SynchrophasorDataClient, *grpc.ClientConn, error) {
	conn, err := grpc.Dial(pmuServerAddress, grpc.WithInsecure(), grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 20 * time.Second, Timeout: 10 * time.Second, PermitWithoutStream: true}), grpc.WithBackoffMaxDelay(time.Duration(5)*time.Second), grpc.WithTimeout(time.Duration(10)*time.Second), grpc.WithBlock())
	if err != nil {
		return nil, nil, err
	}

	client := pmu_server.NewSynchrophasorDataClient(conn)
	return client, conn, nil
}

func subscriber(pmuServerAddress string, publishChan chan<- *pmu_server.SynchrophasorDatum) {
	for {
		glog.Infof("Attempting connection to %v", pmuServerAddress)
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
	}
}

func startPublishers(dpeServerAddress string, publishChan <-chan *pmu_server.SynchrophasorDatum, cachedAndRetryChan <-chan *pmu_server.SynchrophasorDatum, publishFailedChan chan<- *pmu_server.SynchrophasorDatum, operationSampleChan chan<- *operationSample) {

	// count from 1 so publisher id output doesn't look as funny
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

func connectAndPublish(id int, dpeServerAddress string, publishChan <-chan *pmu_server.SynchrophasorDatum, cachedAndRetryChan <-chan *pmu_server.SynchrophasorDatum, publishFailedChan chan<- *pmu_server.SynchrophasorDatum, operationSampleChan chan<- *operationSample) {
	glog.Infof("Started DPE publisher %v", id)

	lat := os.Getenv("HZN_LAT")
	lon := os.Getenv("HZN_LON")
	agreementID := os.Getenv("HZN_AGREEMENTID")
	deviceID := os.Getenv("DEVICE_ID")
	haPartners := strings.Split(os.Getenv("HZN_HA_PARTNERS"), ",")

	// do conversions
	latF, _ := strconv.ParseFloat(lat, 32)
	lonF, _ := strconv.ParseFloat(lon, 32)

	send := func(datum *pmu_server.SynchrophasorDatum, stream synchrophasor_dpe.SynchrophasorDPE_StoreClient) error {

		if stream != nil {
			if err := stream.Send(&synchrophasor_dpe.HorizonDatumWrapper{
				Type:            "synchrophasor",
				Lat:             float32(latF),
				Lon:             float32(lonF),
				DeviceId:        deviceID,
				AgreementId:     agreementID,
				WorkloadVersion: workloadVersion,
				Datum:           datum,
				HaPartners:      haPartners,
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

		glog.Errorf("Failed to send datum, cached for retry. Datum: %v", unsentDatum)
		publishFailedChan <- unsentDatum

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
				pubError(datum)
			} else {
				// succeeded sending this datum, so try to handle previous failures (if any)
				select {
				case failed := <-cachedAndRetryChan:
					glog.V(3).Infof("Publishing previously failed record with ts: %v", failed.Ts)
					if err := send(failed, stream); err != nil {
						pubError(failed)
					}
				default:
					// none found
				}
			}
		}

		// forevar !
		time.Sleep(10 * time.Millisecond)
	}
}
