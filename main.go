package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
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
	bufferLength = 100

	// system blocks while finding a buffer to drain blocks while finding a cache to drain blocks while finding a cache to drain blocks while finding a cache to drain and will consider all
	maxCache = 2e5
)

func main() {
	flag.Parse()

	pmuServerAddress := os.Getenv("PMU_SERVER_ADDRESS")
	if pmuServerAddress == "" {
		pmuServerAddress = defaultPMUServerAddress
	}

	dpeServerAddress := os.Getenv("DPE_SERVER_ADDRESS")
	if dpeServerAddress == "" {
		dpeServerAddress = defaultDPEServerAddress
	}

	// TODO: make a ring buffer of specific size
	var cache [maxCache][]*pmu_server.SynchrophasorDatum
	cacheMutex := &sync.Mutex{}

	// TODO: maybe do a defer, panic, recover?

	for {
		glog.Infof("Connecting to %v", pmuServerAddress)
		pmuClient, pmuConn, err := pmuConnect(pmuServerAddress)
		if err != nil {
			glog.Errorf("Error connecting to PMU server: %v", err)
		}

		dpeClient, dpeConn, err := dpeConnect(dpeServerAddress)
		if err != nil {
			glog.Errorf("Error connecting to DPE server: %v", err)
		}

		// start the forwarder
		go send(dpeClient, &cache, cacheMutex)

		// this call blocks until the stream ends; that's an error in our system
		err = read(pmuClient, &cache, cacheMutex)
		if err != nil {
			glog.Errorf("Error reading from server: %v", err)
		}

		glog.Infof("Attempting to close PMU client connection before trying to reconnect to the server")
		pmuConn.Close()

		glog.Infof("Attempting to close DPE client connection before trying to reconnect to the server")
		dpeConn.Close()

		time.Sleep(30 * time.Second)
	}
}

// TODO: consider using the gRPC backup connection lib here
func pmuConnect(pmuServerAddress string) (pmu_server.SynchrophasorDataClient, *grpc.ClientConn, error) {
	conn, err := grpc.Dial(pmuServerAddress, grpc.WithInsecure(), grpc.WithTimeout(time.Duration(20)*time.Second))
	if err != nil {
		return nil, nil, err
	}

	client := pmu_server.NewSynchrophasorDataClient(conn)
	return client, conn, nil
}

// TODO: reduce duplication between this connection initiation and the previous
func dpeConnect(dpeServerAddress string) (synchrophasor_dpe.SynchrophasorDPEClient, *grpc.ClientConn, error) {
	conn, err := grpc.Dial(dpeServerAddress, grpc.WithInsecure(), grpc.WithTimeout(time.Duration(20)*time.Second))
	if err != nil {
		return nil, nil, err
	}

	client := synchrophasor_dpe.NewSynchrophasorDPEClient(conn)
	return client, conn, nil
}

func read(client pmu_server.SynchrophasorDataClient, cacheP *[maxCache][]*pmu_server.SynchrophasorDatum, cacheMutex *sync.Mutex) error {
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

		runtime.Gosched()
		store(datum, cacheP, cacheMutex)
	}
}

func calcCacheLocation(cache [maxCache][]*pmu_server.SynchrophasorDatum, cacheMutex *sync.Mutex) {

}

func store(datum *pmu_server.SynchrophasorDatum, cacheP *[maxCache][]*pmu_server.SynchrophasorDatum, cacheMutex *sync.Mutex) {

	// TODO: optimize this, it's wasteful to re-determine the location with every call to store

	// lock to determine write location
	cacheMutex.Lock()
	defer cacheMutex.Unlock()
	cacheIdx := -1

	// find a spot for a buffer, create one and retain it
	for idx, buf := range *cacheP {

		if buf == nil {
			buffer := make([]*pmu_server.SynchrophasorDatum, 0, bufferLength)
			(*cacheP)[idx] = buffer
			cacheIdx = idx
			glog.V(5).Infof("Found storage location [%v] in cache for new buffer", cacheIdx)
			break

		} else if len(buf) < cap(buf) {
			cacheIdx = idx
			break
		}

	}

	if cacheIdx < 0 {
		glog.Errorf("Cache is at capacity (%v buckets of %v elements each), dropping record!!: %v", maxCache, bufferLength, datum.GetId())
	} else {
		(*cacheP)[cacheIdx] = append((*cacheP)[cacheIdx], datum)
		glog.V(6).Infof("Stored %v. Cache total: %v. Buffer total: %v", datum.GetId(), len(*cacheP), len((*cacheP)[cacheIdx]))
	}

}

func send(client synchrophasor_dpe.SynchrophasorDPEClient, cacheP *[maxCache][]*pmu_server.SynchrophasorDatum, cacheMutex *sync.Mutex) {

	lat := os.Getenv("HZN_LAT")
	lon := os.Getenv("HZN_LON")
	agreementID := os.Getenv("HZN_AGREEMENTID")

	for {
		// TODO: if no buckets are full, sleep; if buckets are full, send all full buckets then clear them (synchronize the read, unlock, send, then synchronize the clear)

		var buffer *[]*pmu_server.SynchrophasorDatum
		var bufferIdx int

		cacheMutex.Lock()
		for idx := range *cacheP {

			b := (*cacheP)[idx]

			// TODO: sloppy, redo this and eliminate duplication
			if b != nil && len(b) == cap(b) {
				// we've found one
				glog.V(5).Infof("Found buffer at %v for sending", idx)
				buffer = &b
				bufferIdx = idx
				break
			}
		}
		cacheMutex.Unlock()

		if buffer == nil {
			glog.V(6).Infof("No buffer found to forward to DPE")

		} else {
			glog.V(5).Infof("Found buffer to forward, sending to DPE")

			stream, err := client.Store(context.Background())
			if err != nil {
				glog.Fatalf("Error establishing stream to DPE server: %v", err)
			}

			for _, datum := range *buffer {
				err = stream.Send(&synchrophasor_dpe.HorizonDatumWrapper{
					Lat:         lat,
					Lon:         lon,
					AgreementID: agreementID,
					Datum:       datum,
				})
			}

			if err != nil {
				glog.Errorf("Error sending data to stream")
			} else {
				cacheMutex.Lock()
				glog.V(5).Infof("Gonna drain buffer at %v with record count: %v", bufferIdx, len(*buffer))
				(*cacheP)[bufferIdx] = nil
				cacheMutex.Unlock()
			}
		}

		time.Sleep(40 * time.Millisecond)
	}
}
