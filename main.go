package main

import (
	"fmt"
	"github.com/golang/glog"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"io"

	"time"

	"flag"

	pmu_server "github.com/michaeldye/synchrophasor-proto/pmu_server"
)

const (
	defaultPMUServerAddress = "pmu:8008"
)

func main() {
	flag.Parse()

	// TODO: maybe do a defer, panic, recover?

	for {
		glog.Infof("Connecting to %v", defaultPMUServerAddress)
		client, conn, err := connect()
		if err != nil {
			glog.Errorf("Error connecting to server: %v", err)
		}

		// this call blocks until the stream ends; that's an error in our system
		err = read(client)

		if err != nil {
			glog.Errorf("Error reading from server: %v", err)
		}

		glog.Infof("Attempting to close client connection before trying to reconnect to the server")
		conn.Close()

		time.Sleep(30 * time.Second)
	}
}

// TODO: consider using the gRPC backup connection lib here
func connect() (pmu_server.SynchrophasorDataClient, *grpc.ClientConn, error) {
	conn, err := grpc.Dial(defaultPMUServerAddress, grpc.WithInsecure(), grpc.WithTimeout(time.Duration(20)*time.Second))
	if err != nil {
		return nil, nil, err
	}

	client := pmu_server.NewSynchrophasorDataClient(conn)
	return client, conn, nil
}

func read(client pmu_server.SynchrophasorDataClient) error {
	stream, err := client.Sample(context.Background(), &pmu_server.SamplingFilter{})
	if err != nil {
		glog.Fatalf("Error streaming samples: %v", err)
	}

	for {
		datum, err := stream.Recv()
		if err == io.EOF {
			return fmt.Errorf("Server sent EOF")
		}

		if err != nil {
			return fmt.Errorf("%v.Sample(_) = _, %v", client, err)
		}

		glog.Infof("Datum id: %v, values: %v", datum.GetId(), datum.GetPhaseData())
	}
}
