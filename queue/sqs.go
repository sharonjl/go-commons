package queue

import (
	"errors"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/sharonjl/go-commons/math"
	"sync"
)

type sqsQueue struct {
	queueName string
	queueUrl  string
	svc       *sqs.SQS
}

func NewSQSQueue(queueName string, awsSession *session.Session) Queue {
	svc := sqs.New(awsSession)
	params := &sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	}
	resp, err := svc.GetQueueUrl(params)
	if err != nil {
		log.WithFields(log.Fields{
			"err":   err,
			"queue": queueName,
		}).Error("queue: unable to get queue url")
	}

	return &sqsQueue{
		queueName: queueName,
		queueUrl:  aws.StringValue(resp.QueueUrl),
		svc:       svc,
	}
}

func (q *sqsQueue) Poll(h Handler) {
	params := &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(q.queueUrl),
		MaxNumberOfMessages: aws.Int64(10),
		WaitTimeSeconds:     aws.Int64(20),
	}

	for {
		log.WithFields(log.Fields{
			"queue": q.queueUrl,
		}).Info("queue: polling for messages")

		resp, err := q.svc.ReceiveMessage(params)
		if err != nil {
			log.WithFields(log.Fields{
				"queue": q.queueUrl,
				"err":   err,
			}).Error("queue: polling for messages")
		}

		if len(resp.Messages) > 0 {
			q.run(h, resp.Messages)
		}
	}
}

const maxItemsInBatch = 10

func (q *sqsQueue) Publish(m ...string) error {
	c := len(m)
	if m == nil || c == 0 {
		return errors.New("queue: nothing to publish")
	}

	l := math.IntRoundTo(c, maxItemsInBatch)
	b := l / maxItemsInBatch

	for i := 0; i < b; i++ {
		entries := make([]*sqs.SendMessageBatchRequestEntry, maxItemsInBatch)
		c := 0
		for j := 0; j < maxItemsInBatch; j++ {
			k := i*maxItemsInBatch + j
			if k >= len(m) {
				break
			}

			id := fmt.Sprintf("m_%d", k)
			entries[j] = &sqs.SendMessageBatchRequestEntry{ // Required
				Id:          aws.String(id),   // Required
				MessageBody: aws.String(m[k]), // Required
			}
			c++
		}

		params := &sqs.SendMessageBatchInput{
			Entries:  entries[:c],
			QueueUrl: aws.String(q.queueUrl), // Required
		}
		_, err := q.svc.SendMessageBatch(params)
		if err != nil {
			log.WithFields(log.Fields{
				"err":   err,
				"msg":   m,
				"queue": q.queueUrl,
			}).Error("queue: error publishing message")
			return err
		}
	}
	return nil
}

func (q *sqsQueue) run(h Handler, messages []*sqs.Message) {
	numMessages := len(messages)
	log.WithFields(log.Fields{
		"queue": q.queueName,
	}).Infof("queue: received %d messages", numMessages)

	var wg sync.WaitGroup
	wg.Add(numMessages)
	for i := range messages {
		go func(m *sqs.Message) {
			defer wg.Done()
			if err := q.handleMessage(m, h); err != nil {
				log.WithFields(log.Fields{
					"err":   err,
					"msg":   m,
					"queue": q.queueUrl,
				}).Error("queue: error handling message")
			}
		}(messages[i])
	}
	wg.Wait()
}

func (q *sqsQueue) handleMessage(m *sqs.Message, h Handler) error {
	var err error
	err = h.Handle(m)
	if err != nil {
		return err
	}

	_, err = q.svc.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      aws.String(q.queueUrl),
		ReceiptHandle: m.ReceiptHandle,
	})
	return err
}
