package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)


func buildRequestInformation(msg []byte) (RequestInfo, error) {
	info := RequestInfo{}
	r := bytes.NewReader(msg)

	if err := binary.Read(r, binary.BigEndian, &info.request_api_key); err != nil {
		return info, err
	}
	if err := binary.Read(r, binary.BigEndian, &info.request_api_version); err != nil {
		return info, err
	}
	if err := binary.Read(r, binary.BigEndian, &info.correlation_id); err != nil {
		return info, err
	}

	var clientIDLen int16
	if err := binary.Read(r, binary.BigEndian, &clientIDLen); err != nil {
		return info, err
	}
	if clientIDLen < -1 {
		return info, fmt.Errorf("invalid client id length %d", clientIDLen)
	}
	if clientIDLen > 0 {
		if _, err := io.ReadFull(r, make([]byte, clientIDLen)); err != nil {
			return info, err
		}
	}

	// Request header v2: TAGGED_FIELDS (often 0 tags → one byte 0x00)
	if err := skipTagBuffer(r); err != nil {
		return info, err
	}

	switch info.request_api_key {
	case 0:
		if err := parseProduceRequest(r, &info); err != nil {
			return info, err
		}
		return info, nil
	case 1:
		if err := parseFetchRequest(r, &info); err != nil {
			return info, err
		}
		return info, nil
	case 18:
		if info.request_api_version >= 3 {
			if _, err := readCompactNullableStringBytes(r); err != nil {
				return info, err
			}
			if _, err := readCompactNullableStringBytes(r); err != nil {
				return info, err
			}
		}
	case 75:
		parseDescribeTopicPartitionsRequest(r, &info)
	}

	return info, nil
}

// parseProduceRequest parses flexible Produce (v9+): TransactionalId, Acks, Timeout, TopicData.
func parseProduceRequest(r *bytes.Reader, info *RequestInfo) error {
	ver := info.request_api_version
	if ver < 9 {
		return nil
	}
	if _, err := readCompactNullableStringBytes(r); err != nil {
		return err
	}
	var acks int16
	if err := binary.Read(r, binary.BigEndian, &acks); err != nil {
		return err
	}
	var timeoutMs int32
	if err := binary.Read(r, binary.BigEndian, &timeoutMs); err != nil {
		return err
	}
	tn, err := readUvarint(r)
	if err != nil {
		return err
	}
	topicCount := int(tn) - 1
	if topicCount < 0 {
		return fmt.Errorf("invalid produce topic array length")
	}
	for i := 0; i < topicCount; i++ {
		var pt ProduceTopicIn
		if ver >= 13 {
			if _, err := io.ReadFull(r, pt.TopicID[:]); err != nil {
				return err
			}
		} else {
			name, err := readCompactStringBytes(r)
			if err != nil {
				return err
			}
			pt.Name = name
		}
		pn, err := readUvarint(r)
		if err != nil {
			return err
		}
		pCount := int(pn) - 1
		if pCount < 0 {
			return fmt.Errorf("invalid produce partition array length")
		}
		var parts []ProducePartitionIn
		for j := 0; j < pCount; j++ {
			var idx int32
			if err := binary.Read(r, binary.BigEndian, &idx); err != nil {
				return err
			}
			rec, err := readCompactRecordPayload(r)
			if err != nil {
				return err
			}
			if err := skipTagBuffer(r); err != nil {
				return err
			}
			parts = append(parts, ProducePartitionIn{Index: idx, Records: rec})
		}
		if err := skipTagBuffer(r); err != nil {
			return err
		}
		pt.Partitions = parts
		info.produceTopics = append(info.produceTopics, pt)
	}
	return skipTagBuffer(r)
}

func readFetchPartition(r *bytes.Reader, ver int16) (int32, error) {
	var partition int32
	if err := binary.Read(r, binary.BigEndian, &partition); err != nil {
		return 0, err
	}
	if ver >= 9 {
		var leaderEpoch int32
		if err := binary.Read(r, binary.BigEndian, &leaderEpoch); err != nil {
			return 0, err
		}
	}
	var fetchOffset int64
	if err := binary.Read(r, binary.BigEndian, &fetchOffset); err != nil {
		return 0, err
	}
	if ver >= 12 {
		var lastFetchedEpoch int32
		if err := binary.Read(r, binary.BigEndian, &lastFetchedEpoch); err != nil {
			return 0, err
		}
	}
	if ver >= 5 {
		var logStartOffset int64
		if err := binary.Read(r, binary.BigEndian, &logStartOffset); err != nil {
			return 0, err
		}
	}
	var partitionMaxBytes int32
	if err := binary.Read(r, binary.BigEndian, &partitionMaxBytes); err != nil {
		return 0, err
	}
	if ver >= 12 {
		if err := skipTagBuffer(r); err != nil {
			return 0, err
		}
	}
	return partition, nil
}

func readFetchTopic(r *bytes.Reader, ver int16) (FetchTopicInput, error) {
	var ft FetchTopicInput
	var err error
	if ver >= 13 {
		if _, err = io.ReadFull(r, ft.TopicID[:]); err != nil {
			return ft, err
		}
	} else {
		ft.TopicName, err = readCompactStringBytes(r)
		if err != nil {
			return ft, err
		}
		if tm, ok := metadataMap[string(ft.TopicName)]; ok {
			ft.TopicID = tm.topic_id
		}
	}
	n, err := readUvarint(r)
	if err != nil {
		return ft, err
	}
	pcount := int(n) - 1
	if pcount < 0 {
		return ft, fmt.Errorf("invalid fetch partition array length")
	}
	for i := 0; i < pcount; i++ {
		pidx, err := readFetchPartition(r, ver)
		if err != nil {
			return ft, err
		}
		ft.Partitions = append(ft.Partitions, FetchPartitionInput{PartitionIndex: pidx})
	}
	if ver >= 12 {
		if err := skipTagBuffer(r); err != nil {
			return ft, err
		}
	}
	return ft, nil
}

func skipForgottenTopicsData(r *bytes.Reader, ver int16) error {
	if ver < 7 {
		return nil
	}
	n, err := readUvarint(r)
	if err != nil {
		return err
	}
	count := int(n) - 1
	for i := 0; i < count; i++ {
		if ver >= 13 {
			var u [16]byte
			if _, err := io.ReadFull(r, u[:]); err != nil {
				return err
			}
		} else {
			if _, err := readCompactStringBytes(r); err != nil {
				return err
			}
		}
		pn, err := readUvarint(r)
		if err != nil {
			return err
		}
		pc := int(pn) - 1
		for j := 0; j < pc; j++ {
			var p int32
			if err := binary.Read(r, binary.BigEndian, &p); err != nil {
				return err
			}
		}
	}
	return nil
}

func parseFetchRequest(r *bytes.Reader, info *RequestInfo) error {
	ver := info.request_api_version
	// ReplicaId is on the wire for Fetch v0–14; v15+ uses tagged ReplicaState instead.
	if ver <= 14 {
		var replicaID int32
		if err := binary.Read(r, binary.BigEndian, &replicaID); err != nil {
			return err
		}
	}
	var maxWaitMs, minBytes int32
	if err := binary.Read(r, binary.BigEndian, &maxWaitMs); err != nil {
		return err
	}
	if err := binary.Read(r, binary.BigEndian, &minBytes); err != nil {
		return err
	}
	if ver >= 3 {
		var maxBytes int32
		if err := binary.Read(r, binary.BigEndian, &maxBytes); err != nil {
			return err
		}
	}
	if ver >= 4 {
		var isolation int8
		if err := binary.Read(r, binary.BigEndian, &isolation); err != nil {
			return err
		}
	}
	if ver >= 7 {
		var sessionEpoch int32
		if err := binary.Read(r, binary.BigEndian, &info.fetch_session_id); err != nil {
			return err
		}
		if err := binary.Read(r, binary.BigEndian, &sessionEpoch); err != nil {
			return err
		}
	}
	tn, err := readUvarint(r)
	if err != nil {
		return err
	}
	tcount := int(tn) - 1
	if tcount < 0 {
		return fmt.Errorf("invalid fetch topic array length")
	}
	for i := 0; i < tcount; i++ {
		ft, err := readFetchTopic(r, ver)
		if err != nil {
			return err
		}
		info.fetchTopics = append(info.fetchTopics, ft)
	}
	if err := skipForgottenTopicsData(r, ver); err != nil {
		return err
	}
	if ver >= 11 {
		if _, err := readCompactStringBytes(r); err != nil {
			return err
		}
	}
	if ver >= 12 {
		return skipTagBuffer(r)
	}
	return nil
}

func parseDescribeTopicPartitionsRequest(r *bytes.Reader, info *RequestInfo) {
	arrayLen, _ := readUvarint(r)
	topicCount := int(arrayLen) - 1
	if topicCount < 0 {
		return
	}
	for i := 0; i < topicCount; i++ {
		nameLen, err := readUvarint(r)
		if err != nil || nameLen == 0 {
			return
		}
		name := make([]byte, nameLen-1)
		if _, err := io.ReadFull(r, name); err != nil {
			return
		}
		_ = skipTagBuffer(r)
		info.topics = append(info.topics, Topic{
			topic_name: name,
			strname:    string(name),
		})
	}
}
