package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"sort"
)

const topicAuthOpsUnset = int32(-2147483648)

func apiVersionMapKeysSorted() []uint16 {
	keys := make([]uint16, 0, len(versionsMap))
	for k := range versionsMap {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}

func validVersion(req RequestInfo) bool {
	key := uint16(req.request_api_key)
	version := uint16(req.request_api_version)
	if _, ok := versionsMap[key]; !ok {
		return false
	}
	vrange := versionsMap[key]
	if version < vrange.min_version || version > vrange.max_version {
		return false
	}
	return true
}

func buildResponse(req RequestInfo) *Response {
	res := &Response{}
	res.api_key = uint16(req.request_api_key)
	res.request_api_version = uint16(req.request_api_version)
	res.correlation_id = uint32(req.correlation_id)
	res.throttle_time_ms = uint32(0)
	res.tag_buffer = uint8(0)

	switch res.api_key {
	case uint16(0):
		res.valid_version = validVersion(req)
		res.produceTopics = append([]ProduceTopicIn(nil), req.produceTopics...)
		if !res.valid_version {
			res.error_code = uint16(35)
		} else {
			res.error_code = uint16(0)
		}
	case uint16(1):
		res.valid_version = validVersion(req)
		if !res.valid_version {
			res.error_code = uint16(35)
		} else {
			res.error_code = uint16(0)
		}
		res.fetchTopics = append([]FetchTopicInput(nil), req.fetchTopics...)
		res.fetch_session_id = req.fetch_session_id
	case uint16(18):
		res.valid_version = validVersion(req)
		if !res.valid_version {
			res.error_code = uint16(35)
		} else {
			res.error_code = uint16(0)
		}
	case uint16(75):
		res.topics = append([]Topic(nil), req.topics...)
		for i := range res.topics {
			name := string(res.topics[i].topic_name)
			res.topics[i].strname = name
			if _, ok := metadataMap[name]; !ok {
				res.topics[i].error_code = uint16(3)
				res.topics[i].topic_id = [16]byte{}
			} else {
				res.topics[i].error_code = uint16(0)
				res.topics[i].topic_id = metadataMap[name].topic_id
			}
			res.topics[i].is_internal = false
			res.topics[i].topic_authorized_operations = topicAuthOpsUnset
		}
		sort.Slice(res.topics, func(i, j int) bool {
			return res.topics[i].strname < res.topics[j].strname
		})
	}
	return res
}

func sendResponse(res *Response, con net.Conn) error {
	buf := new(bytes.Buffer)
	switch res.api_key {
	case uint16(0):
		writeProduceResponse(res, buf)
	case uint16(1):
		writeFetchResponse(res, buf) 
	case uint16(18):
		writeApiVersionsResponse(res, buf)
	case uint16(75):
		if err := writeDescribeTopicPartitionsResponse(res, buf); err != nil {
			return err
		}
	default:
		return fmt.Errorf("invalid version type")
	}

	_, err := con.Write(buf.Bytes())
	if err != nil {
		return fmt.Errorf("failed Write")
	}
	return nil
}

func writeApiVersionsResponse(res *Response, buf *bytes.Buffer) {
	_ = binary.Write(buf, binary.BigEndian, uint32(0))
	_ = binary.Write(buf, binary.BigEndian, int32(res.correlation_id))
	_ = binary.Write(buf, binary.BigEndian, res.error_code)

	keys := apiVersionMapKeysSorted()
	if res.error_code != 0 {
		buf.WriteByte(1) // empty COMPACT_ARRAY (0 elements)
	} else {
		buf.WriteByte(byte(len(keys) + 1))
		for _, key := range keys {
			val := versionsMap[key]
			_ = binary.Write(buf, binary.BigEndian, key)
			_ = binary.Write(buf, binary.BigEndian, val.min_version)
			_ = binary.Write(buf, binary.BigEndian, val.max_version)
			buf.WriteByte(0) // TAG_BUFFER for this ApiVersion entry
		}
	}

	_ = binary.Write(buf, binary.BigEndian, int32(res.throttle_time_ms))
	buf.WriteByte(0) // top-level TAG_BUFFER

	res.message_size = uint32(buf.Len() - 4)
	binary.BigEndian.PutUint32(buf.Bytes()[0:4], res.message_size)
}

func writeDescribeTopicPartitionsResponse(res *Response, buf *bytes.Buffer) error {
	binary.Write(buf, binary.BigEndian, uint32(0))
	binary.Write(buf, binary.BigEndian, int32(res.correlation_id))
	writeUvarintBuf(buf, 0)
	_ = binary.Write(buf, binary.BigEndian, int32(res.throttle_time_ms))

	writeUvarintBuf(buf, uint32(len(res.topics)+1))
	for _, t := range res.topics {
		_ = binary.Write(buf, binary.BigEndian, t.error_code)
		writeCompactNullableStringBuf(buf, t.topic_name)
		_, _ = buf.Write(t.topic_id[:])
		if t.is_internal {
			buf.WriteByte(1)
		} else {
			buf.WriteByte(0)
		}

		md, hasTopic := metadataMap[t.strname]
		var plist []Partitions
		if hasTopic && t.error_code == 0 {
			plist = md.partitions
		}
		writeUvarintBuf(buf, uint32(len(plist)+1))
		for _, p := range plist {
			_ = binary.Write(buf, binary.BigEndian, p.error_code)
			_ = binary.Write(buf, binary.BigEndian, int32(p.partition_index))
			_ = binary.Write(buf, binary.BigEndian, int32(p.leader_id))
			_ = binary.Write(buf, binary.BigEndian, int32(p.leader_epoch))
			writeCompactInt32ArrayBuf(buf, p.replica_nodes)
			writeCompactInt32ArrayBuf(buf, p.isr_nodes)
			writeUvarintBuf(buf, 0)
			writeUvarintBuf(buf, 0)
			writeCompactInt32ArrayBuf(buf, []uint32{})
			writeUvarintBuf(buf, 0)
		}

		_ = binary.Write(buf, binary.BigEndian, t.topic_authorized_operations)
		writeUvarintBuf(buf, 0)
	}

	// Null NextCursor: INT8 -1 (0xff). CodeCrafters expects this, not UNSIGNED_VARINT 0.
	buf.WriteByte(0xff)
	// Root-level TAG_BUFFER (UNSIGNED_VARINT count of tagged fields).
	writeUvarintBuf(buf, 0)

	res.message_size = uint32(buf.Len() - 4)
	binary.BigEndian.PutUint32(buf.Bytes()[0:4], res.message_size)
	return nil
}


const (
	errUnknownTopicID          = uint16(100)
	errUnknownTopicOrPartition = uint16(3)
)

func writeFetchPartitionResponse(buf *bytes.Buffer, apiVer uint16, partitionIndex int32, partErr uint16, highWM, lastStable, logStart int64, recordsBlob []byte) {
	_ = binary.Write(buf, binary.BigEndian, partitionIndex)
	_ = binary.Write(buf, binary.BigEndian, int16(partErr))
	_ = binary.Write(buf, binary.BigEndian, highWM)
	if apiVer >= 4 {
		_ = binary.Write(buf, binary.BigEndian, lastStable)
	}
	if apiVer >= 5 {
		_ = binary.Write(buf, binary.BigEndian, logStart)
	}
	if apiVer >= 4 {
		writeUvarintBuf(buf, 0) // null AbortedTransactions
	}
	if apiVer >= 11 {
		_ = binary.Write(buf, binary.BigEndian, int32(-1)) // PreferredReadReplica
	}
	if apiVer >= 12 {
		if partErr != 0 {
			writeUvarintBuf(buf, 0) // null Records
		} else if len(recordsBlob) == 0 {
			writeUvarintBuf(buf, 1) // COMPACT_RECORD_SIZE: 0 bytes of batch data
		} else {
			writeUvarintBuf(buf, uint32(len(recordsBlob)+1))
			_, _ = buf.Write(recordsBlob)
		}
		writeUvarintBuf(buf, 0) // partition-level TAG_BUFFER
	} else {
		writeUvarintBuf(buf, 0) // null Records (pre-flex)
	}
}

func writeFetchResponse(res *Response, buf *bytes.Buffer) {
	_ = binary.Write(buf, binary.BigEndian, uint32(0))
	_ = binary.Write(buf, binary.BigEndian, int32(res.correlation_id))
	writeUvarintBuf(buf, 0) // response header TAG_BUFFER (flexible)
	_ = binary.Write(buf, binary.BigEndian, int32(res.throttle_time_ms))
	_ = binary.Write(buf, binary.BigEndian, int16(res.error_code))
	_ = binary.Write(buf, binary.BigEndian, res.fetch_session_id)

	apiVer := res.request_api_version
	topicsOut := res.fetchTopics
	if !res.valid_version {
		topicsOut = nil
	}

	writeUvarintBuf(buf, uint32(len(topicsOut)+1))
	for _, ft := range topicsOut {
		if apiVer >= 13 {
			_, _ = buf.Write(ft.TopicID[:])
		} else {
			writeCompactStringBuf(buf, ft.TopicName)
		}
		writeUvarintBuf(buf, uint32(len(ft.Partitions)+1))
		var tm TopicMetadata
		var topicOK bool
		if apiVer >= 13 {
			tm, topicOK = uuidToMetadata[ft.TopicID]
		} else {
			tm, topicOK = metadataMap[string(ft.TopicName)]
		}
		for _, p := range ft.Partitions {
			var partErr uint16
			var hw, lso, logStart int64
			var recordsBlob []byte
			switch {
			case !topicOK:
				partErr = errUnknownTopicID
				hw, lso, logStart = -1, -1, -1
			case !metadataHasPartition(tm, p.PartitionIndex):
				partErr = errUnknownTopicOrPartition
				hw, lso, logStart = -1, -1, -1
			default:
				blob, err := readPartitionLogRecordBytes(tm.name, p.PartitionIndex)
				if err != nil {
					partErr = 0
					hw, lso, logStart = 0, 0, 0
					recordsBlob = nil
				} else {
					recordsBlob = blob
					partErr = 0
					hw, lso, logStart = partitionFetchOffsets(blob)
				}
			}
			writeFetchPartitionResponse(buf, apiVer, p.PartitionIndex, partErr, hw, lso, logStart, recordsBlob)
		}
		if apiVer >= 12 {
			writeUvarintBuf(buf, 0) // topic-level TAG_BUFFER
		}
	}
	if apiVer >= 12 {
		writeUvarintBuf(buf, 0) // top-level TAG_BUFFER (e.g. NodeEndpoints on v16+)
	}

	res.message_size = uint32(buf.Len() - 4)
	binary.BigEndian.PutUint32(buf.Bytes()[0:4], res.message_size)
}


func writeProducePartitionData(buf *bytes.Buffer, apiVer uint16, partitionIndex int32, errCode int16, baseOffset, logAppendTimeMs, logStartOffset int64) {
	_ = binary.Write(buf, binary.BigEndian, partitionIndex)
	_ = binary.Write(buf, binary.BigEndian, errCode)
	_ = binary.Write(buf, binary.BigEndian, baseOffset)
	if apiVer >= 2 {
		_ = binary.Write(buf, binary.BigEndian, logAppendTimeMs)
	}
	if apiVer >= 5 {
		_ = binary.Write(buf, binary.BigEndian, logStartOffset)
	}
	if apiVer >= 8 {
		writeUvarintBuf(buf, 1) // empty RecordErrors COMPACT_ARRAY
		writeCompactNullableStringBuf(buf, nil)
	}
	writeUvarintBuf(buf, 0) // partition-level TAG_BUFFER
}

func writeProduceResponse(res *Response, buf *bytes.Buffer) {
	_ = binary.Write(buf, binary.BigEndian, uint32(0))
	_ = binary.Write(buf, binary.BigEndian, int32(res.correlation_id))
	writeUvarintBuf(buf, 0) // response header TAG_BUFFER (v1 header)

	apiVer := res.request_api_version
	topicsOut := res.produceTopics
	if !res.valid_version {
		topicsOut = nil
	}

	writeUvarintBuf(buf, uint32(len(topicsOut)+1))
	for _, pt := range topicsOut {
		if apiVer >= 13 {
			_, _ = buf.Write(pt.TopicID[:])
		} else {
			writeCompactStringBuf(buf, pt.Name)
		}
		writeUvarintBuf(buf, uint32(len(pt.Partitions)+1))
		for _, p := range pt.Partitions {
			var errCode int16
			var baseOff, logAppend, logStart int64
			tm, valid := produceTopicPartitionValid(pt, p.Index, apiVer)
			switch {
			case !valid:
				errCode = int16(errUnknownTopicOrPartition)
				baseOff, logAppend, logStart = -1, -1, -1
			case len(p.Records) == 0:
				errCode = int16(errUnknownTopicOrPartition)
				baseOff, logAppend, logStart = -1, -1, -1
			default:
				baseOff = nextPartitionBaseOffset(tm.name, p.Index)
				if err := appendPartitionLogBatch(tm.name, p.Index, p.Records); err != nil {
					errCode = int16(errUnknownTopicOrPartition)
					baseOff, logAppend, logStart = -1, -1, -1
				} else {
					errCode = 0
					logAppend = -1
					logStart = 0
				}
			}
			writeProducePartitionData(buf, apiVer, p.Index, errCode, baseOff, logAppend, logStart)
		}
		writeUvarintBuf(buf, 0) // topic-level TAG_BUFFER
	}

	_ = binary.Write(buf, binary.BigEndian, int32(res.throttle_time_ms))
	writeUvarintBuf(buf, 0) // top-level TAG_BUFFER

	res.message_size = uint32(buf.Len() - 4)
	binary.BigEndian.PutUint32(buf.Bytes()[0:4], res.message_size)
}