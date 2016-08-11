package queue

import (
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type Queue interface {
	Poll(h Handler)
	Publish(m...string) error
}

type Handler interface {
	Handle(msg interface{}) error
}

type SQSHandlerFunc func(msg *sqs.Message) error

func (f SQSHandlerFunc) Handle(msg interface{}) error {
	if m, ok := msg.(*sqs.Message); ok {
		return f(m)
	}
	return fmt.Errorf("error: message must be of *sqs.Message, not %T", msg)
}

type MessageDispatchFunc func(msg *Message) error

func (f MessageDispatchFunc) Handle(msg interface{}) error {
	if m, ok := msg.(*sqs.Message); ok {
		msg, err := MsgUnmarshal(aws.StringValue(m.Body))
		if err != nil {
			return err
		}
		msg.Id = aws.StringValue(m.MessageId)
		return f(msg)
	}
	return fmt.Errorf("error: message must be of *Message, not %T", msg)
}

type Message struct {
	Id      string      `json:"-"`
	Type    MessageType `json:"type"`
	Message string      `json:"message"`
}

type MessageType string

func MsgMarshal(src *Message) (string, error) {
	b, err := json.Marshal(src)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func MsgUnmarshal(src string) (*Message, error) {
	var dest *Message
	if err := json.Unmarshal([]byte(src), dest); err != nil {
		return nil, err
	}
	return dest, nil
}

func MsgWrap(typ MessageType, src interface{}) (*Message, error) {
	b, err := json.Marshal(src)
	if err != nil {
		return nil, err
	}
	return &Message{
		Type:    typ,
		Message: string(b),
	}, nil
}

func MsgUnwrap(typ MessageType, src *Message, dest interface{}) error {
	if err := json.Unmarshal([]byte(src.Message), dest); err != nil {
		return err
	}
	return nil
}
