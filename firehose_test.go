package kinetic

import (
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/firehose"
	. "github.com/smartystreets/goconvey/convey"
)

func TestFireHose(t *testing.T) {
	producer, _ := new(Firehose).InitC("your-stream", "", "", "accesskey", "secretkey", "us-east-1", 4)
	producer.NewEndpoint("localhost", "your-stream")
	producer.(*Firehose).client = new(fakefirehose)
	producer.ReInit()
	Convey("Given a running firehose producer", t, func() {
		Convey("it should send data to the firehose stream", func() {
			for i := 0; i < 100; i++ {
				producer.Send(new(Message).Init([]byte("this is a message"), ""))
				runtime.Gosched()
			}
			time.Sleep(10 * time.Second)
			So(atomic.LoadInt64(&(producer.(*Firehose).client.(*fakefirehose).count)), ShouldEqual, 100)
			So(producer.(*Firehose).getMsgCount(), ShouldEqual, 100)
		})
	})
}

// Mocks for aws Firehose.
// This implements github.com/aws/aws-sdk-go/service/firehose/firehoseiface.FirehoseAPI
type fakefirehose struct {
	count int64
}

func (f *fakefirehose) CreateDeliveryStreamRequest(*firehose.CreateDeliveryStreamInput) (*request.Request, *firehose.CreateDeliveryStreamOutput) {
	return nil, nil
}

func (f *fakefirehose) CreateDeliveryStream(*firehose.CreateDeliveryStreamInput) (*firehose.CreateDeliveryStreamOutput, error) {
	return nil, nil
}

func (f *fakefirehose) DeleteDeliveryStreamRequest(*firehose.DeleteDeliveryStreamInput) (*request.Request, *firehose.DeleteDeliveryStreamOutput) {
	return nil, nil
}

func (f *fakefirehose) DeleteDeliveryStream(*firehose.DeleteDeliveryStreamInput) (*firehose.DeleteDeliveryStreamOutput, error) {
	return nil, nil
}
func (f *fakefirehose) DescribeDeliveryStreamRequest(*firehose.DescribeDeliveryStreamInput) (*request.Request, *firehose.DescribeDeliveryStreamOutput) {
	return nil, nil
}
func (f *fakefirehose) DescribeDeliveryStream(*firehose.DescribeDeliveryStreamInput) (*firehose.DescribeDeliveryStreamOutput, error) {
	return nil, nil
}
func (f *fakefirehose) ListDeliveryStreamsRequest(*firehose.ListDeliveryStreamsInput) (*request.Request, *firehose.ListDeliveryStreamsOutput) {
	return nil, nil
}
func (f *fakefirehose) ListDeliveryStreams(*firehose.ListDeliveryStreamsInput) (*firehose.ListDeliveryStreamsOutput, error) {
	return nil, nil
}
func (f *fakefirehose) PutRecordRequest(*firehose.PutRecordInput) (*request.Request, *firehose.PutRecordOutput) {
	return nil, nil
}
func (f *fakefirehose) PutRecord(*firehose.PutRecordInput) (*firehose.PutRecordOutput, error) {
	return nil, nil
}
func (f *fakefirehose) PutRecordBatchRequest(*firehose.PutRecordBatchInput) (*request.Request, *firehose.PutRecordBatchOutput) {
	return nil, nil
}
func (f *fakefirehose) PutRecordBatch(*firehose.PutRecordBatchInput) (*firehose.PutRecordBatchOutput, error) {
	atomic.AddInt64(&(f.count), 1)
	return &firehose.PutRecordBatchOutput{
		RequestResponses: []*firehose.PutRecordBatchResponseEntry{
			&firehose.PutRecordBatchResponseEntry{},
		},
	}, nil
}
func (f *fakefirehose) UpdateDestinationRequest(*firehose.UpdateDestinationInput) (*request.Request, *firehose.UpdateDestinationOutput) {
	return nil, nil
}
func (f *fakefirehose) UpdateDestination(*firehose.UpdateDestinationInput) (*firehose.UpdateDestinationOutput, error) {
	return nil, nil
}
