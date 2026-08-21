package caravan_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/caravan"
	"github.com/kode4food/caravan/table"
)

type updaterMessage struct {
	ID    string
	Value int
}

func TestNewTable(t *testing.T) {
	tbl, err := caravan.NewTable[string, int]("col1", "col2", "col3")
	assert.NoError(t, err)
	assert.NotNil(t, tbl)
}

func TestNewTableUpdater(t *testing.T) {
	tbl, err := caravan.NewTable[string, int]("value")
	assert.NoError(t, err)

	col := table.NewColumn(
		"value", func(msg updaterMessage) int { return msg.Value },
	)
	updater, err := caravan.NewTableUpdater(
		tbl, func(msg updaterMessage) string { return msg.ID }, col,
	)
	assert.NoError(t, err)
	assert.NotNil(t, updater)
}

func TestTableUpdate(t *testing.T) {
	tbl, err := caravan.NewTable[string, int]("value")
	assert.NoError(t, err)

	col := table.NewColumn(
		"value", func(msg updaterMessage) int { return msg.Value },
	)
	updater, err := caravan.NewTableUpdater(
		tbl, func(msg updaterMessage) string { return msg.ID }, col,
	)
	assert.NoError(t, err)

	in := caravan.NewTopic[updaterMessage]()
	out := caravan.NewTopic[updaterMessage]()
	running := caravan.TopicConsumer(in).
		TableUpdate(updater).
		TopicProducer(out).
		Start()
	defer running.Stop()

	producer := in.NewProducer()
	defer producer.Close()
	msg := updaterMessage{ID: "message-1", Value: 42}
	producer.Send() <- msg

	consumer := out.NewConsumer()
	defer consumer.Close()
	assert.Equal(t, msg, <-consumer.Receive())

	get, err := tbl.Getter("value")
	assert.NoError(t, err)
	values, err := get(msg.ID)
	assert.NoError(t, err)
	assert.Equal(t, []int{msg.Value}, values)
}
