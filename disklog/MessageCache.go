package disklog

import (
	//"log"
	"code.google.com/p/forestbus.server/model"
)

type CacheEntry struct {
	firstIndex int64
	cache      model.Messages
}

type CacheEntryInfo struct {
	FirstIndex int64
	CacheSize  int
}

/*
MessageCache provides DiskLogStorage cache facilities.

The cache is populated by the data passed to appends being kept around for a while and used
for gets when it can be.  This significantly reduces the amount of file seeking and reading.

Note that locks are not required - all read / write locking has been done by the caller.
*/
type MessageCache struct {
	cache  []*CacheEntry
	hits   int
	misses int
}

/*
MessageCacheInfo is used by ExpVars to present key stats about the MessageCache state.
*/
type MessageCacheInfo struct {
	CacheSlotsUsed int
	CacheEntryInfo []CacheEntryInfo
	TotalCacheSize int
	Hits           int
	Misses         int
}

// CreateCache generates the MessageCache for a give number of message batches.
func CreateCache(capacity int) *MessageCache {
	msgCache := &MessageCache{}
	msgCache.cache = make([]*CacheEntry, 0, capacity)
	return msgCache
}

/*
AppendMessages caches the messages for a while, bumping out old ones as requied.
*/
func (msgCache *MessageCache) AppendMessages(index int64, msgs model.Messages) {
	if cap(msgCache.cache) == len(msgCache.cache) {
		// Bump the oldest cached messages
		copy(msgCache.cache, msgCache.cache[1:])
		msgCache.cache = msgCache.cache[:len(msgCache.cache)-1]
	}
	msgCache.cache = append(msgCache.cache, &CacheEntry{firstIndex: index, cache: msgs})
}

/*
GetMessages returns messages from the cache from the given index.
*/
func (msgCache *MessageCache) GetMessages(index int64, count int64) model.Messages {
	for _, cacheE := range msgCache.cache {
		maxIndex := cacheE.firstIndex + int64(cacheE.cache.GetCount())
		//log.Printf("CACHE: index %v, maxIndex %v, firstIndex: %v\n", index, maxIndex, cacheE.firstIndex)
		if index >= cacheE.firstIndex && index < maxIndex {
			// We've found some messages - return them.
			localIndex := index - cacheE.firstIndex
			cacheLength := int64(cacheE.cache.GetCount())
			toReturn := cacheLength
			if (toReturn - localIndex) > count {
				toReturn = localIndex + count
			}
			msgCache.hits++
			results, _ := cacheE.cache.Slice(int(localIndex), int(toReturn))
			return results
		}
	}
	msgCache.misses++
	return model.EMPTY_MESSAGES
}

/*
Truncate is used to trim the cache in case the underlying commit log has been truncated.
*/
func (msgCache *MessageCache) Truncate(index int64) {
	for i := len(msgCache.cache) - 1; i >= 0; i-- {
		if (msgCache.cache[i].firstIndex + int64(msgCache.cache[i].cache.GetCount())) > index {
			// This cache entry contains messages beyond our truncate point - remove
			msgCache.cache = msgCache.cache[:i]
		}
	}
}

func (msgCache *MessageCache) ExpVar() interface{} {
	stats := &MessageCacheInfo{}
	stats.CacheSlotsUsed = len(msgCache.cache)
	stats.CacheEntryInfo = make([]CacheEntryInfo, stats.CacheSlotsUsed)
	var totalSize int
	for i, cacheEntry := range msgCache.cache {
		stats.CacheEntryInfo[i].FirstIndex = cacheEntry.firstIndex
		stats.CacheEntryInfo[i].CacheSize = cacheEntry.cache.GetCount()
		totalSize += stats.CacheEntryInfo[i].CacheSize
	}
	stats.TotalCacheSize = totalSize
	stats.Hits = msgCache.hits
	stats.Misses = msgCache.misses
	return stats
}
