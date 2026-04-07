package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const (
	kraftLogsDir   = "/tmp/kraft-combined-logs"
	segmentLogName = "00000000000000000000.log"
)

// pendingPartition buffers partition records that appear before their topic record (no extra topic map).
type pendingPartition struct {
	topicUUID [16]byte
	part      Partitions
}

var pendingPartitions []pendingPartition

func readLogs() error {
	pendingPartitions = nil
	data, err := os.ReadFile("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log")
	if err != nil {
		return err
	}

	r := bytes.NewReader(data)
	for r.Len() >= 12 {
		var baseOffset int64
		var batchLength int32
		if err := binary.Read(r, binary.BigEndian, &baseOffset); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if err := binary.Read(r, binary.BigEndian, &batchLength); err != nil {
			return err
		}
		if batchLength <= 0 {
			return fmt.Errorf("invalid batch length %d", batchLength)
		}

		batchData := make([]byte, batchLength)
		if _, err := io.ReadFull(r, batchData); err != nil {
			return err
		}
		br := bytes.NewReader(batchData)

		// Skip fixed batch header (45 bytes) up to record count.
		if _, err := br.Seek(45, io.SeekCurrent); err != nil {
			return err
		}

		var numRecords int32
		if err := binary.Read(br, binary.BigEndian, &numRecords); err != nil {
			return err
		}

		for i := int32(0); i < numRecords; i++ {
			recLen, err := readSvarint(br)
			if err != nil || recLen <= 0 {
				break
			}

			recData := make([]byte, recLen)
			if _, err := io.ReadFull(br, recData); err != nil {
				return err
			}
			rr := bytes.NewReader(recData)

			if _, err := rr.ReadByte(); err != nil {
				return err
			}
			if _, err := readSvarint(rr); err != nil {
				return err
			}
			if _, err := readSvarint(rr); err != nil {
				return err
			}

			keyLen, err := readSvarint(rr)
			if err != nil {
				return err
			}
			if keyLen > 0 {
				if _, err := rr.Seek(keyLen, io.SeekCurrent); err != nil {
					return err
				}
			}

			valueLen, err := readSvarint(rr)
			if err != nil {
				return err
			}
			if valueLen <= 0 {
				continue
			}

			valueData := make([]byte, valueLen)
			if _, err := io.ReadFull(rr, valueData); err != nil {
				return err
			}
			if err := decodeMetadataRecordValue(bytes.NewReader(valueData)); err != nil {
				return err
			}
		}
	}
	return nil
}

// partitionLogPath returns the path to the first segment log for a topic partition.
func partitionLogPath(topicName string, partitionIndex int32) string {
	dir := fmt.Sprintf("%s-%d", topicName, partitionIndex)
	return filepath.Join(kraftLogsDir, dir, segmentLogName)
}

// readPartitionLogRecordBytes returns the Fetch "Records" payload: concatenated on-disk
// batches (baseOffset + batchLength + batchPayload for each), matching Kafka log layout.
func readPartitionLogRecordBytes(topicName string, partitionIndex int32) ([]byte, error) {
	path := partitionLogPath(topicName, partitionIndex)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	r := bytes.NewReader(data)
	var out []byte
	for r.Len() >= 12 {
		var baseOffset int64
		var batchLength int32
		if err := binary.Read(r, binary.BigEndian, &baseOffset); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		if err := binary.Read(r, binary.BigEndian, &batchLength); err != nil {
			return nil, err
		}
		if batchLength <= 0 {
			return nil, fmt.Errorf("invalid batch length %d", batchLength)
		}
		payload := make([]byte, batchLength)
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil, err
		}
		var batchBuf bytes.Buffer
		_ = binary.Write(&batchBuf, binary.BigEndian, baseOffset)
		_ = binary.Write(&batchBuf, binary.BigEndian, batchLength)
		batchBuf.Write(payload)
		out = append(out, batchBuf.Bytes()...)
	}
	return out, nil
}

func metadataHasPartition(tm TopicMetadata, partitionIndex int32) bool {
	for _, p := range tm.partitions {
		if int32(p.partition_index) == partitionIndex {
			return true
		}
	}
	return false
}

func findPartitionMetadata(tm TopicMetadata, partitionIndex int32) *Partitions {
	for i := range tm.partitions {
		if int32(tm.partitions[i].partition_index) == partitionIndex {
			return &tm.partitions[i]
		}
	}
	return nil
}

func partitionHasLogDirectory(p *Partitions) bool {
	return p != nil && p.directory_id != [16]byte{}
}

// topicMetadataFromCluster returns metadata only if the topic exists with a non-zero
// topic id from a TOPIC_RECORD (cluster metadata log).
func topicMetadataFromCluster(pt ProduceTopicIn, apiVer uint16) (TopicMetadata, bool) {
	if apiVer >= 13 {
		tm, ok := uuidToMetadata[pt.TopicID]
		return tm, ok
	}
	tm, ok := metadataMap[string(pt.Name)]
	if !ok || tm.topic_id == ([16]byte{}) {
		return TopicMetadata{}, false
	}
	return tm, true
}

// produceTopicPartitionValid returns cluster metadata when topic + partition + log dir exist.
func produceTopicPartitionValid(pt ProduceTopicIn, partitionIndex int32, apiVer uint16) (TopicMetadata, bool) {
	tm, ok := topicMetadataFromCluster(pt, apiVer)
	if !ok {
		return TopicMetadata{}, false
	}
	par := findPartitionMetadata(tm, partitionIndex)
	if !partitionHasLogDirectory(par) {
		return TopicMetadata{}, false
	}
	return tm, true
}

// validateProduceAgainstClusterMetadata checks TOPIC_RECORD + PARTITION_RECORD linkage:
// topic id, partition index, and at least one partition log directory UUID (KRaft).
func validateProduceAgainstClusterMetadata(pt ProduceTopicIn, partitionIndex int32, apiVer uint16) bool {
	_, ok := produceTopicPartitionValid(pt, partitionIndex, apiVer)
	return ok
}

// nextPartitionBaseOffset returns the offset of the next record to be written (0 for empty/missing log).
func nextPartitionBaseOffset(topicName string, partitionIndex int32) int64 {
	blob, err := readPartitionLogRecordBytes(topicName, partitionIndex)
	if err != nil || len(blob) == 0 {
		return 0
	}
	hw, _, _ := partitionFetchOffsets(blob)
	return hw
}

// appendPartitionLogBatch appends RecordBatch bytes to the partition segment (Kafka log format).
func appendPartitionLogBatch(topicName string, partitionIndex int32, batch []byte) error {
	if len(batch) == 0 {
		return fmt.Errorf("empty record batch")
	}
	dir := filepath.Join(kraftLogsDir, fmt.Sprintf("%s-%d", topicName, partitionIndex))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	path := partitionLogPath(topicName, partitionIndex)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write(batch)
	return err
}

// partitionFetchOffsets derives high watermark, last stable offset, and log start from raw
// Fetch record bytes (concatenated on-disk batches). Uses LastOffsetDelta in each batch payload.
func partitionFetchOffsets(blob []byte) (hw, lso, logStart int64) {
	if len(blob) == 0 {
		return 0, 0, 0
	}
	r := bytes.NewReader(blob)
	logStart = 0
	var maxLast int64 = -1
	for r.Len() >= 12 {
		var baseOffset int64
		var batchLength int32
		if err := binary.Read(r, binary.BigEndian, &baseOffset); err != nil {
			break
		}
		if err := binary.Read(r, binary.BigEndian, &batchLength); err != nil {
			break
		}
		if batchLength < 15 {
			break
		}
		payload := make([]byte, batchLength)
		if _, err := io.ReadFull(r, payload); err != nil {
			break
		}
		lastDelta := int32(binary.BigEndian.Uint32(payload[11:15]))
		last := baseOffset + int64(lastDelta)
		if last > maxLast {
			maxLast = last
		}
	}
	if maxLast < 0 {
		return 0, 0, 0
	}
	next := maxLast + 1
	return next, next, 0
}

func int32SliceToUint32(s []int32) []uint32 {
	out := make([]uint32, len(s))
	for i, v := range s {
		out[i] = uint32(v)
	}
	return out
}

func syncUUIDMetadata(topicName string) {
	tm, ok := metadataMap[topicName]
	if !ok {
		return
	}
	uuidToMetadata[tm.topic_id] = tm
}

func absorbPendingForTopic(uuid [16]byte, topicName string) {
	var kept []pendingPartition
	for _, item := range pendingPartitions {
		if item.topicUUID == uuid {
			tm := metadataMap[topicName]
			tm.partitions = append(tm.partitions, item.part)
			metadataMap[topicName] = tm
			syncUUIDMetadata(topicName)
		} else {
			kept = append(kept, item)
		}
	}
	pendingPartitions = kept
}

func attachPartitionOrQueue(uuid [16]byte, p Partitions) {
	if tm, ok := uuidToMetadata[uuid]; ok {
		name := tm.name
		tm.partitions = append(tm.partitions, p)
		metadataMap[name] = tm
		uuidToMetadata[uuid] = tm
		return
	}
	pendingPartitions = append(pendingPartitions, pendingPartition{topicUUID: uuid, part: p})
}

func decodeMetadataRecordValue(vr *bytes.Reader) error {
	if _, err := vr.ReadByte(); err != nil {
		return err
	}
	recType, err := vr.ReadByte()
	if err != nil {
		return err
	}
	recVersion, err := vr.ReadByte()
	if err != nil {
		return err
	}

	switch recType {
	case 2:
		nameLen, err := readUvarint(vr)
		if err != nil {
			return err
		}
		if nameLen == 0 {
			return fmt.Errorf("topic name: unexpected null")
		}
		nameBytes := make([]byte, nameLen-1)
		if _, err := io.ReadFull(vr, nameBytes); err != nil {
			return err
		}
		name := string(nameBytes)

		var uuid [16]byte
		if _, err := io.ReadFull(vr, uuid[:]); err != nil {
			return err
		}

		if _, ok := metadataMap[name]; !ok {
			tm := TopicMetadata{
				name:       name,
				topic_id:   uuid,
				partitions: []Partitions{},
			}
			metadataMap[name] = tm
			uuidToMetadata[uuid] = tm
		} else {
			tm := metadataMap[name]
			oldID := tm.topic_id
			tm.topic_id = uuid
			tm.name = name
			metadataMap[name] = tm
			if oldID != uuid {
				delete(uuidToMetadata, oldID)
			}
			uuidToMetadata[uuid] = tm
		}
		absorbPendingForTopic(uuid, name)
		return nil

	case 3:
		var partitionID int32
		if err := binary.Read(vr, binary.BigEndian, &partitionID); err != nil {
			return err
		}

		var topicUUID [16]byte
		if _, err := io.ReadFull(vr, topicUUID[:]); err != nil {
			return err
		}

		replicas, err := readInt32CompactArray(vr)
		if err != nil {
			return err
		}
		isr, err := readInt32CompactArray(vr)
		if err != nil {
			return err
		}
		if _, err := readInt32CompactArray(vr); err != nil {
			return err
		}
		if _, err := readInt32CompactArray(vr); err != nil {
			return err
		}

		var leader, leaderEpoch, partitionEpoch int32
		if err := binary.Read(vr, binary.BigEndian, &leader); err != nil {
			return err
		}
		if err := binary.Read(vr, binary.BigEndian, &leaderEpoch); err != nil {
			return err
		}
		if err := binary.Read(vr, binary.BigEndian, &partitionEpoch); err != nil {
			return err
		}
		_ = partitionEpoch

		var dirID [16]byte
		if recVersion >= 1 {
			n, err := readUvarint(vr)
			if err != nil {
				return err
			}
			if n == 0 {
				return fmt.Errorf("partition directories: unexpected null array")
			}
			ndir := int(n) - 1
			for j := 0; j < ndir; j++ {
				var u [16]byte
				if _, err := io.ReadFull(vr, u[:]); err != nil {
					return err
				}
				if j == 0 {
					dirID = u
				}
			}
		}

		replicaNodes := int32SliceToUint32(replicas)
		isrNodes := int32SliceToUint32(isr)

		p := Partitions{
			error_code:      0,
			partition_index: uint32(partitionID),
			leader_id:       uint32(leader),
			leader_epoch:    uint32(leaderEpoch),
			replica_length:  uint8(len(replicaNodes) + 1),
			replica_nodes:   replicaNodes,
			isr_length:      uint8(len(isrNodes) + 1),
			isr_nodes:       isrNodes,
			tag_buffer:      0,
			directory_id:    dirID,
		}

		attachPartitionOrQueue(topicUUID, p)
		return nil

	default:
		return nil
	}
}
