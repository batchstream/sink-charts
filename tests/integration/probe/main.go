// Opt-in in-cluster business probe. Mutations and immediate reads are not retried.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	sink "github.com/batchstream/sink-go"
	"google.golang.org/grpc/credentials/insecure"
)

type options struct {
	address  string
	store    string
	dataset  string
	mode     string
	count    int
	duration time.Duration
	stop     bool
}

type document struct {
	ID    string `bson:"_id"`
	Value int    `bson:"value"`
}

type report struct {
	Mode         string    `json:"mode"`
	Acknowledged int       `json:"acknowledged"`
	Verified     int       `json:"verified"`
	Errors       []string  `json:"errors"`
	Started      time.Time `json:"started"`
	Finished     time.Time `json:"finished"`
}

func main() {
	opts := options{}
	flag.StringVar(&opts.address, "address", "dns:///qual-sink-gateway-headless:8080", "Gateway address")
	flag.StringVar(&opts.store, "store", "mongo", "Store")
	flag.StringVar(&opts.dataset, "dataset", "rollout", "Unique test collection")
	flag.StringVar(&opts.mode, "mode", "continuous", "continuous, publish or verify")
	flag.IntVar(&opts.count, "count", 20000, "Async record count")
	flag.DurationVar(&opts.duration, "duration", 20*time.Minute, "Safety deadline")
	flag.BoolVar(&opts.stop, "stop", false, "Tell the running probe to finish and reconcile")
	flag.Parse()
	if opts.stop {
		if err := os.WriteFile("/output/stop", nil, 0600); err != nil {
			panic(err)
		}
		return
	}
	result, err := run(opts)
	if err != nil {
		result.Errors = append(result.Errors, err.Error())
	}
	result.Finished = time.Now().UTC()
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		panic(err)
	}
	if len(result.Errors) > 0 || (opts.mode != "publish" && result.Acknowledged != result.Verified) {
		os.Exit(1)
	}
}

func run(opts options) (report, error) {
	result := report{Mode: opts.mode, Started: time.Now().UTC(), Errors: []string{}}
	retry := sink.RetryPolicy{MaxAttempts: 1}
	clientOptions := sink.ClientOptions{ReadRetry: retry}
	dialOptions := sink.DialOptions{Client: clientOptions, TransportCredentials: insecure.NewCredentials(), DNSRefreshInterval: 5 * time.Second}
	client, err := sink.Dial(opts.address, dialOptions)
	if err != nil {
		return result, err
	}
	defer client.Close()
	datasetOptions := sink.DatasetOptions{URI: "sink://" + opts.store + "/charttest/" + opts.dataset, Encoding: sink.DocumentEncodingBSON}
	dataset, err := sink.NewDataset(client, datasetOptions)
	if err != nil {
		return result, err
	}
	if opts.mode == "verify" {
		result.Acknowledged = opts.count
		deadline := time.Now().Add(5 * time.Minute)
		for time.Now().Before(deadline) {
			verified, verifyErr := verify(dataset, opts.count)
			if verifyErr == nil {
				result.Verified = verified
				return result, nil
			}
			time.Sleep(time.Second)
		}
		return result, fmt.Errorf("asynchronous application did not converge")
	}
	if opts.mode == "publish" {
		for start := 0; start < opts.count; start += 500 {
			records := []sink.Record{}
			for index := start; index < min(start+500, opts.count); index++ {
				value := document{ID: fmt.Sprint(index), Value: index}
				record := sink.Record{Key: sink.StringKey(value.ID), Value: value}
				records = append(records, record)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			request := sink.NewDatasetWriteRequest(records...).WithCompletionMode(sink.CompletionReturnAfterAccepted)
			_, err := dataset.Create(ctx, request)
			cancel()
			if err != nil {
				return result, err
			}
			result.Acknowledged += len(records)
		}
		return result, nil
	}
	deadline := time.Now().Add(opts.duration)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for time.Now().Before(deadline) {
		if _, err := os.Stat("/output/stop"); err == nil {
			break
		}
		<-ticker.C
		index := result.Acknowledged
		value := document{ID: fmt.Sprint(index), Value: index}
		record := sink.Record{Key: sink.StringKey(value.ID), Value: value}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		writeRequest := sink.NewDatasetWriteRequest(record)
		_, err := dataset.Create(ctx, writeRequest)
		cancel()
		if err != nil {
			return result, fmt.Errorf("create %d: %w", index, err)
		}
		result.Acknowledged++
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		readRequest := sink.NewDatasetReadRequest(record.Key)
		found, err := dataset.Read(ctx, readRequest)
		cancel()
		if err != nil {
			return result, fmt.Errorf("immediate read %d: %w", index, err)
		}
		if len(found) != 1 || found[0].Status != sink.ReadFound {
			return result, fmt.Errorf("missing immediate read %d", index)
		}
	}
	result.Verified, err = verify(dataset, result.Acknowledged)
	return result, err
}

func verify(dataset *sink.Dataset, count int) (int, error) {
	verified := 0
	for start := 0; start < count; start += 100 {
		keys := []sink.Key{}
		for index := start; index < min(start+100, count); index++ {
			keys = append(keys, sink.StringKey(fmt.Sprint(index)))
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		request := sink.NewDatasetReadRequest(keys...)
		found, err := dataset.Read(ctx, request)
		cancel()
		if err != nil {
			return verified, err
		}
		if len(found) != len(keys) {
			return verified, fmt.Errorf("read result count mismatch")
		}
		for offset, record := range found {
			if record.Status != sink.ReadFound {
				return verified, fmt.Errorf("missing record %d", start+offset)
			}
			var value document
			if err := record.Document.Decode(&value); err != nil {
				return verified, err
			}
			if value.ID != fmt.Sprint(start+offset) || value.Value != start+offset {
				return verified, fmt.Errorf("content mismatch at %d", start+offset)
			}
			verified++
		}
	}
	return verified, nil
}
