/*
The model package provides the interface for interacting with a stream of messages internally within Forest Bus.  The messages are retained as a slice of bytes to enable cheap marshalling as part of the rpc calls and easy writing to underlying files.
*/
package model

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"log"
)

// MESSAGE_OVERHEAD is the total number of bytes used by the header of a message in version 0 of the message format.
const MESSAGE_OVERHEAD = 1 + 4 + 8 + 4

// MESSAGE_VERSION_OFFSET is the location within the header of the byte used to indicate the version of the message format.
const MESSAGE_VERSION_OFFEST = 0

// MESSAGE_LENGTH_OFFSET is the location within the header of the 4 bytes used to record the length of the message.
const MESSAGE_LENGTH_OFFSET = MESSAGE_VERSION_OFFEST + 1

// MESSAGE_TERM_OFFSET is the location within the header of the 8 bytes used to record the election term of the message.
const MESSAGE_TERM_OFFSET = MESSAGE_LENGTH_OFFSET + 4

// MESSAGE_CRC_OFFSET is the location within the header of the 4 bytes used to record the CRC of the message payload.
const MESSAGE_CRC_OFFSET = MESSAGE_TERM_OFFSET + 8

// ERR_UNSUPPORTED_MESSAGE_FORMAT is thrown if a message format is encountered that this code is not familiar with.
var ERR_UNSUPPORTED_MESSAGE_FORMAT = errors.New("Unsupported message format")

// ERR_CRC_MISMATCH is thrown if the message payload's CRC doesn't match that recorded in the header.
var ERR_CRC_MISMATCH = errors.New("CRC check on message payload failed.")

// EMPTY_MESSAGES represts an empty set of messages.
var EMPTY_MESSAGES = Messages{}

// Messages consists of the count of messages and a slice of bytes.
type Messages struct {
	Count int
	Data  []byte
}

/*
MessagesFromReader reads messages from the io.Reader and returns the resulting Message objects, along with the number of bytes actually read and any error.

The targetBytes parameter is the initial number of bytes that the function should attempt to retrieve from the Reader while looking for messages.  If the given number of bytes doesn't include a full message, additional bytes will be read in an attempt to retrieve at least one full message.

Bytes that have been read that are for part of message will be discarded, but still reflected in the returned readByteCount which always contains the number of bytes actually read from the stream.  The Disklog.Segment logic uses this to keep track of where in a file the current file pointer is up to.
*/
func MessagesFromReader(r io.Reader, targetBytes int) (result Messages, readByteCount int, err error) {
	result = Messages{}
	if targetBytes < MESSAGE_OVERHEAD {
		targetBytes = MESSAGE_OVERHEAD + 1024
	}
	// Keep the original buffer around in case we need it later (0 message scenario)
	buffer := make([]byte, targetBytes)
	result.Data = buffer
	// Read as much data as possible.
	readByteCount, err = io.ReadFull(r, result.Data)
	if err != nil && err != io.ErrUnexpectedEOF {
		return result, readByteCount, err
	}
	// How much did we get?
	//log.Printf("Read: data length: %v (readByteCount: %v)\n", len(result.Data), readByteCount)
	result.Data = result.Data[:readByteCount]
	//log.Printf("Data len: %v\n", len(result.Data))
	countErr := result.countMessages(true)
	if countErr != nil {
		return result, readByteCount, countErr
	}
	//log.Printf("Data len: %v\n", len(result.Data))
	if result.Count < 1 && readByteCount >= MESSAGE_OVERHEAD {
		// We got no message - figure out how much we have to read to get at least one.
		log.Printf("INFO: Having to read additional data in order to retrieve at least one message (%v bytes requested, %v read, data len %v)\n", targetBytes, readByteCount, len(result.Data))
		length := int32(binary.LittleEndian.Uint32(buffer[MESSAGE_LENGTH_OFFSET:]))
		newTarget := int(length) + MESSAGE_OVERHEAD
		remainderToRead := newTarget - readByteCount
		if remainderToRead > 0 {
			newData := make([]byte, newTarget)
			copy(newData, buffer[:readByteCount])
			newByteCount, err := io.ReadFull(r, newData[readByteCount:])
			readByteCount += newByteCount
			result.Data = newData
			if err != nil {
				// Suppress the error - cause by us trying to overread.
				return result, readByteCount, nil
			}
			countErr = result.countMessages(true)
			if countErr != nil {
				return result, readByteCount, countErr
			}
			if result.Count > 0 {
				log.Printf("Additional read to retrieve message worked - message retrieved.\n")
			}
		}
	}

	return result, readByteCount, nil
}

// GetCount returns the number of whole messages represented by the data contained in this set of Messages.
func (msgs *Messages) GetCount() int {
	return msgs.Count
}

/*
ForEachMessage is a helper function that calls the provided action function once for each message contained in this Messages object.

The action function will recieve the message index (local to this set of messages, starting at 0) and the offset into the slice of bytes that this message header starts at.  The action function must return a boolean - true to continue iterating over the rest of the messages, or false to abort the iteration.
*/
func (msgs *Messages) ForEachMessage(action func(msgIndex int, msgOffset int) bool) error {
	dlen := len(msgs.Data) - MESSAGE_OVERHEAD
	localCount := 0
	for msgOffset := 0; msgOffset <= dlen; {
		if msgs.Data[msgOffset] != 0 {
			return ERR_UNSUPPORTED_MESSAGE_FORMAT
		}
		length := int32(binary.LittleEndian.Uint32(msgs.Data[msgOffset+MESSAGE_LENGTH_OFFSET:]))
		if msgOffset+MESSAGE_OVERHEAD+int(length) <= len(msgs.Data) {
			if !action(localCount, msgOffset) {
				return nil
			}
		} else {
			// We have only a partial message - shrink data to match.
			msgs.Data = msgs.Data[:msgOffset]
			break
		}
		localCount++
		msgOffset += MESSAGE_OVERHEAD + int(length)
	}
	return nil
}

/*
countMessages is an internal function that counts how many messages are stored in the slice of bytes contained in this Messages object.  If crcCheck is true then the CRC value stored in the header will be compared to the CRC of the payload.

countMessages results in the Messages.Count field containing the number of complete messages in the Messages object.  If a CRC check is done and any messages fail CRC validation then the ERR_CRC_MISMATCH error will be returned.
*/
func (msgs *Messages) countMessages(crcCheck bool) error {
	msgs.Count = 0
	crcError := false
	err := msgs.ForEachMessage(func(forIndex int, offset int) bool {
		if crcCheck {
			length := int32(binary.LittleEndian.Uint32(msgs.Data[offset+MESSAGE_LENGTH_OFFSET:]))
			CRC := uint32(binary.LittleEndian.Uint32(msgs.Data[offset+MESSAGE_CRC_OFFSET:]))
			actualCRC := crc32.ChecksumIEEE(msgs.Data[offset+MESSAGE_OVERHEAD : offset+MESSAGE_OVERHEAD+int(length)])
			if CRC != actualCRC {
				crcError = true
			}
		}
		msgs.Count++
		return true
	})
	if crcError {
		return ERR_CRC_MISMATCH
	}
	return err
}

/*
GetMessageTerm returns the term of the message at the given local index (starting at zero) within this set of Messages.
*/
func (msgs *Messages) GetMessageTerm(index int) (int64, error) {
	var term int64
	err := msgs.ForEachMessage(func(forIndex int, offset int) bool {
		term = int64(binary.LittleEndian.Uint64(msgs.Data[offset+MESSAGE_TERM_OFFSET:]))
		if forIndex == index {
			return false
		}
		return true
	})
	return term, err
}

/*
SetMessageTerm sets the term of all messages in this set of Messages to the given value.
*/
func (msgs *Messages) SetMessageTerm(newTerm int64) error {
	err := msgs.ForEachMessage(func(forIndex int, offset int) bool {
		binary.LittleEndian.PutUint64(msgs.Data[offset+MESSAGE_TERM_OFFSET:], uint64(newTerm))
		return true
	})
	return err
}

/*
Write outputs all valid messages to the given io.Writer.  The number of bytes written is returned along with any errors.
*/
func (msgs *Messages) Write(w io.Writer) (int, error) {
	written, err := w.Write(msgs.Data)
	if err != nil {
		return 0, err
	}

	return written, nil
}

/*
Slice creates a new Messages object containing a subset of the messages in this Messages.  The underlying byte slice is shared between the two instances.
*/
func (msgs *Messages) Slice(fromMessageIndex int, toMessageIndex int) (Messages, error) {
	var dataSliceStart int
	var endOfSlice int
	result := Messages{}
	if toMessageIndex == fromMessageIndex {
		return result, nil
	}

	err := msgs.ForEachMessage(func(forIndex int, offset int) bool {
		length := int32(binary.LittleEndian.Uint32(msgs.Data[offset+MESSAGE_LENGTH_OFFSET:]))
		endOfSlice = offset + int(length) + MESSAGE_OVERHEAD
		if forIndex == fromMessageIndex {
			dataSliceStart = offset
		}
		if forIndex >= (toMessageIndex - 1) {
			return false
		}
		return true
	})
	if err != nil {
		return result, err
	}

	result.Data = msgs.Data[dataSliceStart:endOfSlice]
	err = result.countMessages(false)
	return result, err
}

/*
Join creates a new instance of Messages that contains both the original Messages content and the given extraMessages as well.  This operation requires copying of both the original and the extraMessages underlying byte data.
*/
func (msgs *Messages) Join(extraMessages Messages) (Messages, error) {
	newMessages := Messages{}
	newLength := len(msgs.Data) + len(extraMessages.Data)
	newMessages.Data = make([]byte, newLength)
	//log.Printf("Copying %v into new array", msgs.Data)
	copy(newMessages.Data, msgs.Data)
	copy(newMessages.Data[len(msgs.Data):], extraMessages.Data)
	err := newMessages.countMessages(false)
	return newMessages, err
}

/*
Payloads returns a slice of byte slices that only contains the payloads of the messages.

The byte data itself is shared between the Messages instance and the resulting Payloads slices.
*/
func (msgs *Messages) Payloads() ([][]byte, error) {
	results := make([][]byte, msgs.Count)
	err := msgs.ForEachMessage(func(forIndex int, offset int) bool {
		length := int32(binary.LittleEndian.Uint32(msgs.Data[offset+MESSAGE_LENGTH_OFFSET:]))
		//log.Printf("Found message %v long\n", length)
		results[forIndex] = msgs.Data[offset+MESSAGE_OVERHEAD : offset+MESSAGE_OVERHEAD+int(length)]
		return true
	})
	return results, err
}

// RawData provides the underlying raw data of the Messages.
func (msgs *Messages) RawData() []byte {
	return msgs.Data
}

/*
MessagesFromClientData takes the provided slice of payload data and creates the Forest Bus Messages representation of this data.
*/
func MessagesFromClientData(data [][]byte) Messages {
	messagesSize := len(data) * MESSAGE_OVERHEAD
	for _, msgData := range data {
		messagesSize += len(msgData)
	}
	result := Messages{}
	result.Data = make([]byte, messagesSize)
	// Turn each payload into a valid stream of messages
	var offset int
	for _, msgData := range data {
		// Record the version of the format
		result.Data[offset] = 0
		binary.LittleEndian.PutUint32(result.Data[offset+MESSAGE_LENGTH_OFFSET:], uint32(len(msgData)))
		// Placeholder for the Term
		binary.LittleEndian.PutUint64(result.Data[offset+MESSAGE_TERM_OFFSET:], 0)
		// The CRC
		binary.LittleEndian.PutUint32(result.Data[offset+MESSAGE_CRC_OFFSET:], crc32.ChecksumIEEE(msgData))
		//log.Printf("Copying msgData %v to %v\n", msgData, offset+MESSAGE_OVERHEAD)
		copy(result.Data[offset+MESSAGE_OVERHEAD:], msgData)
		offset += MESSAGE_OVERHEAD + len(msgData)
	}
	// Do a count / validation on the results
	result.countMessages(false)
	return result
}

/*
ReadSingleMessage is a utility function that reads one message from the given io.Reader.
*/
func ReadSingleMessage(r io.Reader) (length int32, term int64, crc uint32, payload []byte, err error) {
	// Header
	headerBytes := make([]byte, MESSAGE_OVERHEAD)
	_, err = io.ReadFull(r, headerBytes)
	if err != nil {
		return 0, 0, 0, nil, err
	}

	version := headerBytes[0]
	if version != 0 {
		return 0, 0, 0, nil, ERR_UNSUPPORTED_MESSAGE_FORMAT
	}
	Length := int32(binary.LittleEndian.Uint32(headerBytes[MESSAGE_LENGTH_OFFSET:]))
	Term := int64(binary.LittleEndian.Uint64(headerBytes[MESSAGE_TERM_OFFSET:]))
	CRC := uint32(binary.LittleEndian.Uint32(headerBytes[MESSAGE_CRC_OFFSET:]))

	Payload := make([]byte, Length)
	read, err := io.ReadFull(r, Payload)
	if err != nil {
		return 0, 0, 0, nil, err
	}

	if read != int(Length) {
		return 0, 0, 0, nil, errors.New("Not enough data found for message length.")
	}

	return Length, Term, CRC, Payload, nil
}

/*
ReadHeaderGetLength returns the length of the message from the header starting at the current position of the io.Reader.  The full MESSAGE_OVERHEAD bytes will be read from the io.Reader.
*/
func ReadHeaderGetLength(r io.Reader) (int, error) {
	// Header
	headerBytes := make([]byte, MESSAGE_OVERHEAD)
	_, err := io.ReadFull(r, headerBytes)
	if err != nil {
		return 0, err
	}

	version := headerBytes[0]
	if version != 0 {
		return 0, ERR_UNSUPPORTED_MESSAGE_FORMAT
	}
	return int(binary.LittleEndian.Uint32(headerBytes[1:])), nil
}

/*
ParseHeader takes the given slice of bytes and returns the individual fields that form the header.
*/
func ParseHeader(headerBytes []byte) (version byte, length int32, term int64, crc uint32) {
	version = headerBytes[0]
	length = int32(binary.LittleEndian.Uint32(headerBytes[MESSAGE_LENGTH_OFFSET:]))
	term = int64(binary.LittleEndian.Uint64(headerBytes[MESSAGE_TERM_OFFSET:]))
	crc = uint32(binary.LittleEndian.Uint32(headerBytes[MESSAGE_CRC_OFFSET:]))

	return version, length, term, crc
}

// GetMessageContent parses the bytes of a Message and returns just the Payload
func GetMessageContent(r io.Reader) ([]byte, error) {
	_, _, _, Payload, err := ReadSingleMessage(r)
	if err != nil {
		return nil, err
	}

	return Payload, nil
}
