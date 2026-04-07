package main

var versionsMap = map[uint16]VersionInfo{
	0: {
		min_version: 0,
		max_version: 11,
		tag_buffer:  0,
	},
	1: {
		min_version: 0,
		max_version: 16,
		tag_buffer:  0,
	},
	18: {
		min_version: 0,
		max_version: 4,
		tag_buffer:  0,
	},
	75: {
		min_version: 0,
		max_version: 0,
		tag_buffer:  0,
	},
}

var metadataMap = make(map[string]TopicMetadata)
var uuidToMetadata = make(map[[16]byte]TopicMetadata)

type VersionInfo struct {
	min_version uint16
	max_version uint16
	tag_buffer  uint8
}

type TopicMetadata struct {
	name       string
	topic_id   [16]byte
	partitions []Partitions
}

type Topic struct {
	strname                     string
	topic_id                    [16]byte
	topic_name                  []byte
	error_code                  uint16
	is_internal                 bool
	metadata                    TopicMetadata
	topic_authorized_operations int32
}

type Partitions struct {
	error_code       uint16
	partition_index  uint32
	leader_id        uint32
	leader_epoch     uint32
	replica_length   uint8
	replica_nodes    []uint32
	isr_length       uint8
	isr_nodes        []uint32
	eligible_leader_replicas []byte
	last_known_elr           []byte
	offline_repicas          []byte
	tag_buffer               uint8
	
	directory_id [16]byte
}


type ProducePartitionIn struct {
	Index   int32
	Records []byte 
}


type ProduceTopicIn struct {
	Name       []byte 
	TopicID    [16]byte 
	Partitions []ProducePartitionIn
}


type FetchPartitionInput struct {
	PartitionIndex int32
}

// FetchTopicInput is one topic (and its partitions) from a Fetch request.
type FetchTopicInput struct {
	TopicID    [16]byte
	TopicName  []byte 
	Partitions []FetchPartitionInput
}

type RequestInfo struct {
	request_api_key     int16
	request_api_version int16
	correlation_id      int32
	topics              []Topic
	fetchTopics         []FetchTopicInput
	fetch_session_id    int32 
	produceTopics       []ProduceTopicIn
}

type Response struct {
	message_size          uint32
	correlation_id        uint32
	error_code            uint16
	api_key               uint16
	request_api_version   uint16
	throttle_time_ms      uint32
	tag_buffer            uint8
	valid_version         bool
	topics                []Topic
	partitions            []Partitions
	fetchTopics           []FetchTopicInput
	fetch_session_id      int32
	produceTopics         []ProduceTopicIn
}
