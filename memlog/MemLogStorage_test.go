package memlog

import (
	"code.google.com/p/forestbus.server/commitlog"
	"code.google.com/p/forestbus.server/model"
	"testing"
)

func getLogStore() commitlog.LogStorage {
	return NewMemoryLogStorage()
}

func TestGetMessagesEmpty(t *testing.T) {
	store := getLogStore()
	res, err := store.GetMessages(-1, -1)
	if res.GetCount() != 0 || err != nil {
		t.Errorf("Unexpected result or error: %v,%v", res, err)
	}
	res, err = store.GetMessages(-1, 0)
	if res.GetCount() != 0 || err != nil {
		t.Errorf("Unexpected result or error: %v,%v", res, err)
	}
	res, err = store.GetMessages(-1, 1)
	if res.GetCount() != 0 || err != nil {
		t.Errorf("Unexpected result or error: %v,%v", res, err)
	}
	res, err = store.GetMessages(0, 0)
	if res.GetCount() != 0 || err != nil {
		t.Errorf("Unexpected result or error: %v,%v", res, err)
	}
	res, err = store.GetMessages(0, 1)
	if res.GetCount() != 0 || err != nil {
		t.Errorf("Unexpected result or error: %v,%v", res, err)
	}
}

func getMessages(count int) model.Messages {
	msgPayloads := make([][]byte, count)
	for i, _ := range msgPayloads {
		msgPayloads[i] = []byte{byte(i + 1)}
	}
	msgs := model.MessagesFromClientData(msgPayloads)

	return msgs
}

func TestGetOneMessage(t *testing.T) {
	store := getLogStore()
	store.AppendMessages(getMessages(1))
	res, err := store.GetMessages(-1, -1)
	if res.GetCount() != 0 || err != nil {
		t.Errorf("Unexpected result or error: %v,%v", res, err)
	}
	res, err = store.GetMessages(-1, 0)
	if res.GetCount() != 0 || err != nil {
		t.Errorf("Unexpected result or error: %v,%v", res, err)
	}
	res, err = store.GetMessages(-1, 1)
	if res.GetCount() != 0 || err != nil {
		t.Errorf("Unexpected result or error: %v,%v", res, err)
	}
	res, err = store.GetMessages(0, 0)
	if res.GetCount() != 0 || err != nil {
		t.Errorf("Unexpected result or error: %v,%v", res, err)
	}
	res, err = store.GetMessages(0, 1)
	if res.GetCount() != 0 || err != nil {
		t.Errorf("Unexpected result or error: %v,%v", res, err)
	}
	res, err = store.GetMessages(1, 0)
	if res.GetCount() != 0 || err != nil {
		t.Errorf("Unexpected result or error: %v,%v", res, err)
	}
	res, err = store.GetMessages(1, 1)
	if res.GetCount() != 1 || err != nil {
		t.Errorf("Unexpected result or error: %v,%v, %v", res, err, store.(*MemoryLogStorage).data)
	}
}

func TestAppend(t *testing.T) {
	store := getLogStore()
	lastIndex, err := store.AppendMessages(model.EMPTY_MESSAGES)
	if lastIndex != 0 || err != nil {
		t.Errorf("Unexpected result or error: %v,%v", lastIndex, err)
	}

	lastIndex, err = store.AppendMessages(getMessages(1))
	if lastIndex != 1 || err != nil {
		t.Errorf("Unexpected result or error: %v,%v", lastIndex, err)
	}

	lastIndex, err = store.AppendMessages(getMessages(2))
	if lastIndex != 3 || err != nil {
		t.Errorf("Unexpected result or error: %v,%v", lastIndex, err)
	}
}

func TestFirstAppend(t *testing.T) {
	store := getLogStore()
	lastIndex, err := store.AppendFirstMessages(getMessages(1), 100)
	if lastIndex != 100 || err != nil {
		t.Errorf("Unexpected result or error: %v,%v", lastIndex, err)
	}

	// Check we can get them
	msgs, err := store.GetMessages(100, 1)
	if err != nil {
		t.Error(err)
	}
	if msgs.GetCount() != 1 {
		t.Errorf("Unable to get first messages.")
	}

	// Check firstIndex is working
	firstIndex, err := store.GetFirstIndex()
	if err != nil {
		t.Error(err)
	}
	if firstIndex != 100 {
		t.Errorf("Expected first index of 100, got: %v\n", firstIndex)
	}

	// We should error if we try append first again.
	lastIndex, err = store.AppendFirstMessages(getMessages(1), 100)
	if err == nil {
		t.Errorf("Expected error, got nothing.")
	}
}

func TestGetLastIndex(t *testing.T) {
	store := getLogStore()
	lastIndex, err := store.GetLastIndex()
	if lastIndex != 0 || err != nil {
		t.Errorf("Unexpected result or error: %v, %v", lastIndex, err)
	}

	appendIndex, _ := store.AppendMessages(getMessages(1))

	lastIndex, err = store.GetLastIndex()
	if lastIndex != 1 || lastIndex != appendIndex || err != nil {
		t.Errorf("Unexpected result or error: %v, %v", lastIndex, err)
	}

	appendIndex, _ = store.AppendMessages(getMessages(2))

	lastIndex, err = store.GetLastIndex()
	if lastIndex != 3 || lastIndex != appendIndex || err != nil {
		t.Errorf("Unexpected result or error: %v, %v", lastIndex, err)
	}

}

func TestTruncateEmptyStore(t *testing.T) {
	store := getLogStore()

	err := store.TruncateMessages(-1)
	if err != nil {
		t.Error("Error not null")
	}
	err = store.TruncateMessages(0)
	if err != nil {
		t.Error("Error not null")
	}
	err = store.TruncateMessages(1)
	if err != nil {
		t.Error("Error not null")
	}
	err = store.TruncateMessages(2)
	if err != nil {
		t.Error("Error not null")
	}
}

func TestTruncateOneMessage(t *testing.T) {
	store := getLogStore()
	store.AppendMessages(getMessages(1))

	err := store.TruncateMessages(5)
	if err != nil {
		t.Error("Error")
	}
	lastIndex, err := store.GetLastIndex()
	if lastIndex != 1 || err != nil {
		t.Errorf("Unexpected result or error: %v,%v", lastIndex, err)
	}

	err = store.TruncateMessages(2)
	if err != nil {
		t.Error("Error")
	}
	lastIndex, err = store.GetLastIndex()
	if lastIndex != 1 || err != nil {
		t.Errorf("Unexpected result or error: %v, %v", lastIndex, err)
	}

	err = store.TruncateMessages(1)
	if err != nil {
		t.Error("Error")
	}
	lastIndex, err = store.GetLastIndex()
	if lastIndex != 0 || err != nil {
		t.Errorf("Unexpected result or error: %v, %v", lastIndex, err)
	}
}

func TestTruncateManyMessage(t *testing.T) {
	store := getLogStore()
	store.AppendMessages(getMessages(10))
	store.AppendMessages(getMessages(20))
	store.AppendMessages(getMessages(30))

	err := store.TruncateMessages(27)
	if err != nil {
		t.Error("Error")
	}
	lastIndex, err := store.GetLastIndex()
	if lastIndex != 26 || err != nil {
		t.Errorf("Unexpected result or error: %v, %v", lastIndex, err)
	}

}
