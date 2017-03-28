package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/golang/glog"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/michaeldye/synchrophasor-proto/pmu_server"
	"github.com/michaeldye/synchrophasor-proto/synchrophasor_dpe"
)

const (
	defaultPMUServerAddress = "pmu:8008"
	defaultDPEServerAddress = "dpe:9009"

	// system won't drain until a buffer is full
	bufferLength = 30

	// system blocks while finding a buffer to drain blocks while finding a cache to drain blocks while finding a cache to drain blocks while finding a cache to drain and will consider all
	maxCache = bufferLength * 10
)

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

// TODO: add function to buffer to serialize to disk for further cache storage

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

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

	cache := [maxCache]*buffer{}
	cacheMutex := &sync.Mutex{}

	glog.Infof("Using cores: %v", runtime.GOMAXPROCS(-1))

	for {
		go connectAndRead(pmuServerAddress, &cache, cacheMutex)
		go connectAndSend(dpeServerAddress, &cache, cacheMutex)

		start := time.Now().Unix()

		// TODO: add use of the grpc stats handler too
		// start a stats and reporting loop
		for {
			// N.B. we don't synchronize this section so the queue stats are only approximate

			fullBuffers := 0

			for _, buf := range cache {
				if buf != nil && buf.isFull() {
					fullBuffers++
				}
			}

			glog.Infof("RAM cache at %2.2f%% capacity: %v buffers full of %v\n", ((float64(fullBuffers) / float64(maxCache)) * 100), fullBuffers, maxCache)
			time.Sleep(10 * time.Second)

			// die at same time to get comparable stats
			if (*cpuprofile != "") && time.Now().Unix()-start > 120 {
				return
			}
		}
	}
}

func connectAndSend(dpeServerAddress string, cache *[maxCache]*buffer, cacheMutex *sync.Mutex) {
	for {
		dpeClient, dpeConn, err := dpeConnect(dpeServerAddress)
		if err != nil {
			glog.Errorf("Error connecting to DPE server: %v", err)
		}

		if dpeClient != nil {
			if err := send(dpeClient, cache, cacheMutex); err != nil {
				glog.Errorf("Error sending data: %v", err)
			}

			glog.Infof("Attempting to close DPE client connection before trying to reconnect to the server")
			dpeConn.Close()
		}

		// delay reconnection
		time.Sleep(60 * time.Second)
	}
}

func connectAndRead(pmuServerAddress string, cache *[maxCache]*buffer, cacheMutex *sync.Mutex) {
	for {
		glog.Infof("Connecting to %v", pmuServerAddress)
		pmuClient, pmuConn, err := pmuConnect(pmuServerAddress)
		if err != nil {
			glog.Errorf("Error connecting to PMU server: %v", err)
		}

		if pmuClient != nil {
			// this call blocks until the stream ends; that's an error in our system
			if err := read(pmuClient, cache, cacheMutex); err != nil {
				glog.Errorf("Error reading from server: %v", err)
			}

			glog.Infof("Attempting to close PMU client connection before trying to reconnect to the server")
			pmuConn.Close()
		}

		// delay reconnection
		time.Sleep(10 * time.Second)
	}
}

// TODO: consider using the gRPC backup connection lib here
func pmuConnect(pmuServerAddress string) (pmu_server.SynchrophasorDataClient, *grpc.ClientConn, error) {
	conn, err := grpc.Dial(pmuServerAddress, grpc.WithInsecure(), grpc.WithTimeout(time.Duration(20)*time.Second), grpc.WithBlock())
	if err != nil {
		return nil, nil, err
	}

	client := pmu_server.NewSynchrophasorDataClient(conn)
	return client, conn, nil
}

// TODO: reduce duplication between this connection initiation and the previous
func dpeConnect(dpeServerAddress string) (synchrophasor_dpe.SynchrophasorDPEClient, *grpc.ClientConn, error) {
	conn, err := grpc.Dial(dpeServerAddress, grpc.WithInsecure(), grpc.WithTimeout(time.Duration(20)*time.Second), grpc.WithBlock())
	if err != nil {
		return nil, nil, err
	}

	client := synchrophasor_dpe.NewSynchrophasorDPEClient(conn)
	return client, conn, nil
}

func read(client pmu_server.SynchrophasorDataClient, cacheP *[maxCache]*buffer, cacheMutex *sync.Mutex) error {
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

		store(datum, cacheP, cacheMutex)
		runtime.Gosched()
	}
}

// find a buffer to write to
func findFreeBuffer(cacheP *[maxCache]*buffer) (*buffer, error) {

	for idx, cacheBuffer := range *cacheP {

		if cacheBuffer == nil {
			buff := newBuffer()
			(*cacheP)[idx] = buff
			glog.V(5).Infof("Found storage location [%v] in cache for new buffer", idx)
			return buff, nil

		} else if !cacheBuffer.isFull() {
			return cacheBuffer, nil
		}
	}

	return nil, fmt.Errorf("Cache is at capacity (%v buckets of %v elements each), dropping record!!", maxCache, bufferLength)
}

func store(datum *pmu_server.SynchrophasorDatum, cacheP *[maxCache]*buffer, cacheMutex *sync.Mutex) {

	// TODO: optimize this, it's wasteful to re-determine the location with every call to store

	// lock to determine write location
	cacheMutex.Lock()
	defer cacheMutex.Unlock()

	buf, err := findFreeBuffer(cacheP)
	if err != nil {
		glog.Errorf("Error saving datum %v, err: %v", datum.GetId(), err)
	} else {
		buf.write(datum)
	}
}

func send(client synchrophasor_dpe.SynchrophasorDPEClient, cacheP *[maxCache]*buffer, cacheMutex *sync.Mutex) error {

	lat := os.Getenv("HZN_LAT")
	lon := os.Getenv("HZN_LON")
	agreementID := os.Getenv("HZN_AGREEMENTID")

	for {
		// TODO: if no buckets are full, sleep; if buckets are full, send all full buckets then clear them (synchronize the read, unlock, send, then synchronize the clear)

		var buff *buffer
		var buffIdx int

		cacheMutex.Lock()
		for idx := range *cacheP {

			b := (*cacheP)[idx]

			if b != nil && b.isFull() {
				// we've found one
				glog.V(5).Infof("Found buffer at %v for sending", idx)
				buff = b
				buffIdx = idx
				break
			}
		}
		cacheMutex.Unlock()

		if buff == nil {
			glog.V(6).Infof("No buffer found to forward to DPE")

		} else {
			glog.V(5).Infof("Found buffer to forward, sending to DPE")

			stream, err := client.Store(context.Background())
			if err != nil {
				return fmt.Errorf("Error establishing stream to DPE server: %v", err)
			}

			for _, datum := range (*buff).data {
				err = stream.Send(&synchrophasor_dpe.HorizonDatumWrapper{
					Lat:         lat,
					Lon:         lon,
					AgreementID: agreementID,
					Datum:       datum,
				})
			}

			if err != nil {
				return fmt.Errorf("Error sending data to stream: %v", err)
			}

			cacheMutex.Lock()
			glog.V(5).Infof("Gonna drain buffer at %v with record count: %v", buffIdx, buff.writePtr)
			(*cacheP)[buffIdx] = nil
			cacheMutex.Unlock()
		}
		runtime.Gosched()
	}

}
