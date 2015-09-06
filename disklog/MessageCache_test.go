package disklog

import (
	"github.com/owlfish/forestbus-server/model"
	"testing"
)

func TestCreateCache(t *testing.T) {
	newCache := CreateCache(10)
	if len(newCache.cache) != 0 {
		t.Error("Cache length not 0 at creation.")
	}
	if cap(newCache.cache) != 10 {
		t.Error("Capacity of cache not 10 at creation.")
	}
}

func testBadIndexes(cache *MessageCache, t *testing.T) {
	msgs := cache.GetMessages(1, 0)
	if msgs.GetCount() != 0 {
		t.Errorf("Get index 1 with zero count returned length: %v\n", msgs.GetCount())
	}
	msgs = cache.GetMessages(-1, 1)
	if msgs.GetCount() != 0 {
		t.Errorf("Get index -1 count 1 returned length: %v\n", msgs.GetCount())
	}
	msgs = cache.GetMessages(10, 0)
	if msgs.GetCount() != 0 {
		t.Errorf("Get index 10 count 0 returned length: %v\n", msgs.GetCount())
	}
	msgs = cache.GetMessages(10, -1)
	if msgs.GetCount() != 0 {
		t.Errorf("Get index 10 cout -1 returned length: %v\n", msgs.GetCount())
	}
}

func TestGetMessagesBadIndexEmptyCache(t *testing.T) {
	newCache := CreateCache(10)
	testBadIndexes(newCache, t)
	msg := newCache.GetMessages(1, 1)
	if msg.GetCount() != 0 {
		t.Error("Got non-zero length returned from an empty cache.")
	}
}

func getMessages(count int) model.Messages {
	msgPayloads := make([][]byte, count)
	for i, _ := range msgPayloads {
		msgPayloads[i] = []byte{byte(i + 1)}
		// Extend to 10kB to help trigger segment allocation.
		remaining := 10*1024 - len(msgPayloads[i])
		msgPayloads[i] = append(msgPayloads[i], make([]byte, remaining, remaining)...)
	}
	msgs := model.MessagesFromClientData(msgPayloads)

	return msgs
}

func TestGetMessagesBadIndexSingleEntryCache(t *testing.T) {
	newCache := CreateCache(10)
	newCache.AppendMessages(1, getMessages(1))
	testBadIndexes(newCache, t)
	msgs := newCache.GetMessages(1, 1)
	if msgs.GetCount() != 1 {
		t.Errorf("Unable to Get single message: %v", msgs)
	}
	data, _ := msgs.Payloads()
	if data[0][0] != 1 {
		t.Errorf("Data payload not 1 as expected: %v", data[0])
	}
	msgs = newCache.GetMessages(2, 1)
	if msgs.GetCount() != 0 {
		t.Error("Got messages for index > 1")
	}
}

func TestGetMessagesBadIndexTwoEntriesCache(t *testing.T) {
	newCache := CreateCache(10)
	newCache.AppendMessages(1, getMessages(2))
	testBadIndexes(newCache, t)
	msgs := newCache.GetMessages(1, 1)
	if msgs.GetCount() != 1 {
		t.Errorf("Unable to Get single message: %v", msgs)
	}
	data, _ := msgs.Payloads()
	if data[0][0] != 1 {
		t.Errorf("Data not 1 as expected: %v", data[0])
	}
	msgs = newCache.GetMessages(2, 1)
	if msgs.GetCount() != 1 {
		t.Error("Got no messages for index 2")
	}
	data, _ = msgs.Payloads()
	if data[0][0] != 2 {
		t.Errorf("Data not 2 as expected: %v", data[0])
	}
}

func TestGetMessagesBadIndexTwoEntriesSeperateAppendsCache(t *testing.T) {
	newCache := CreateCache(10)
	newCache.AppendMessages(1, getMessages(1))
	newCache.AppendMessages(2, getMessages(1))
	testBadIndexes(newCache, t)
	msgs := newCache.GetMessages(1, 1)
	if msgs.GetCount() != 1 {
		t.Errorf("Unable to Get single message: %v", msgs)
	}
	data, _ := msgs.Payloads()
	if data[0][0] != 1 {
		t.Errorf("Data not 1 as expected: %v", data[0])
	}
	if len(newCache.cache) != 2 {
		t.Error("Cache legnth is not 2 as expected.")
	}
	msgs = newCache.GetMessages(2, 1)
	if msgs.GetCount() != 1 {
		t.Errorf("Got no messages for index 2. Cache: %v and %v.  Result: %v\n", newCache.cache[0], newCache.cache[1], msgs)
	}
	data, _ = msgs.Payloads()
	if data[0][0] != 1 {
		t.Errorf("Data not 1 as expected: %v", data[0])
	}
	msgs = newCache.GetMessages(3, 1)
	if msgs.GetCount() != 0 {
		t.Error("Got messages beyond those expected.")
	}
}

func TestIndexLongerThanContentCache(t *testing.T) {
	newCache := CreateCache(10)
	newCache.AppendMessages(1, getMessages(1))
	msgs := newCache.GetMessages(2, 1)
	if msgs.GetCount() != 0 {
		t.Errorf("Unexpected result getting index 2 with only one entry: %v", msgs)
	}
}

func TestIndexMiddleOfCache(t *testing.T) {
	newCache := CreateCache(10)
	newCache.AppendMessages(1, getMessages(10))
	newCache.AppendMessages(11, getMessages(10))
	msgs := newCache.GetMessages(11, 1)
	if msgs.GetCount() != 1 {
		t.Errorf("Unexpected result getting index 2 with only one entry: %v", msgs)
	}
	data, _ := msgs.Payloads()
	if data[0][0] != 1 {
		t.Errorf("Payload of message was not 1: %v\n", data[0])
	}
	msgs = newCache.GetMessages(15, 1)
	if msgs.GetCount() != 1 {
		t.Errorf("Unexpected result getting index 2 with only one entry: %v", msgs)
	}
	data, _ = msgs.Payloads()
	if data[0][0] != 5 {
		t.Errorf("Payload of message was not 5: %v\n", data[0])
	}
}

func TestTruncateEmptyCache(t *testing.T) {
	newCache := CreateCache(10)
	newCache.Truncate(-1)
	newCache.Truncate(0)
	newCache.Truncate(1)
	newCache.Truncate(5)
}

func TestTruncateOneMessageCache(t *testing.T) {
	newCache := CreateCache(10)
	newCache.AppendMessages(1, getMessages(1))
	newCache.Truncate(5)
	if len(newCache.cache) != 1 || newCache.cache[0].cache.GetCount() != 1 {
		t.Errorf("Unexpected cache length after truncation 5 %v", len(newCache.cache))
	}
	newCache.Truncate(2)
	if len(newCache.cache) != 1 || newCache.cache[0].cache.GetCount() != 1 {
		t.Errorf("Unexpected cache length after truncation 2 %v", len(newCache.cache))
	}
	newCache.Truncate(1)
	if len(newCache.cache) != 0 {
		t.Errorf("Unexpected cache length after truncation")
	}

}

func TestTruncateManyMessageCache(t *testing.T) {
	newCache := CreateCache(10)
	newCache.AppendMessages(1, getMessages(10))
	newCache.AppendMessages(11, getMessages(10))
	newCache.AppendMessages(21, getMessages(10))
	newCache.AppendMessages(31, getMessages(10))
	if len(newCache.cache) != 4 {
		t.Errorf("Unexpected cache length before truncation: %v", len(newCache.cache))
	}

	newCache.Truncate(27)
	if len(newCache.cache) != 2 {
		t.Errorf("Unexpected cache length after truncation 27 %v", len(newCache.cache))
	}

}

func TestCacheBump(t *testing.T) {
	newCache := CreateCache(4)
	newCache.AppendMessages(1, getMessages(10))
	newCache.AppendMessages(11, getMessages(10))
	newCache.AppendMessages(21, getMessages(10))
	newCache.AppendMessages(31, getMessages(10))
	if len(newCache.cache) != 4 {
		t.Errorf("Unexpected cache length before hitting capacity: %v", len(newCache.cache))
	}
	newCache.AppendMessages(41, getMessages(10))
	if len(newCache.cache) != 4 {
		t.Errorf("Unexpected cache length after hitting capacity: %v", len(newCache.cache))
	}
	// Test that the right entries have been removed.
	if newCache.cache[0].firstIndex != 11 {
		t.Errorf("Unexpected first cache entry: %v.", newCache.cache[0])
	}
	if newCache.cache[1].firstIndex != 21 {
		t.Errorf("Unexpected second cache entry: %v.", newCache.cache[1])
	}
	if newCache.cache[2].firstIndex != 31 {
		t.Errorf("Unexpected third cache entry: %v.", newCache.cache[2])
	}
	if newCache.cache[3].firstIndex != 41 {
		t.Errorf("Unexpected fourth cache entry: %v.", newCache.cache[3])
	}
}
