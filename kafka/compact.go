package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

// readUvarint reads an unsigned variable-length integer (COMPACT_ARRAY / COMPACT_STRING lengths).
func readUvarint(r io.ByteReader) (uint64, error) {
	var x uint64
	var s uint
	for i := 0; i < 10; i++ {
		b, err := r.ReadByte()
		if err != nil {
			return 0, err
		}
		if b < 0x80 {
			return x | uint64(b)<<s, nil
		}
		x |= uint64(b&0x7f) << s
		s += 7
	}
	return 0, fmt.Errorf("uvarint too long")
}

// readSvarint reads a signed variable-length integer (zigzag; record lengths, deltas, etc.).
func readSvarint(r io.ByteReader) (int64, error) {
	uv, err := readUvarint(r)
	if err != nil {
		return 0, err
	}
	return int64((uv >> 1) ^ -(uv & 1)), nil
}

// skipTagBuffer skips a TAGGED_FIELDS section (unsigned varint count, then per-tag size + payload).
func skipTagBuffer(r *bytes.Reader) error {
	n, err := readUvarint(r)
	if err != nil {
		return err
	}
	for i := uint64(0); i < n; i++ {
		if _, err := readUvarint(r); err != nil {
			return err
		}
		size, err := readUvarint(r)
		if err != nil {
			return err
		}
		if _, err := r.Seek(int64(size), io.SeekCurrent); err != nil {
			return err
		}
	}
	return nil
}

// readInt32CompactArray reads a COMPACT_ARRAY of INT32 (big-endian).
func readInt32CompactArray(r *bytes.Reader) ([]int32, error) {
	n, err := readUvarint(r)
	if err != nil {
		return nil, err
	}
	count := int(n) - 1
	if count < 0 {
		return nil, fmt.Errorf("invalid compact array length %d", n)
	}
	arr := make([]int32, count)
	for i := range arr {
		if err := binary.Read(r, binary.BigEndian, &arr[i]); err != nil {
			return nil, err
		}
	}
	return arr, nil
}

func readCompactStringBytes(r *bytes.Reader) ([]byte, error) {
	n, err := readUvarint(r)
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, fmt.Errorf("compact string: unexpected null")
	}
	l := int(n) - 1
	b := make([]byte, l)
	if _, err := io.ReadFull(r, b); err != nil {
		return nil, err
	}
	return b, nil
}

func readCompactNullableStringBytes(r *bytes.Reader) ([]byte, error) {
	n, err := readUvarint(r)
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, nil
	}
	l := int(n) - 1
	b := make([]byte, l)
	if _, err := io.ReadFull(r, b); err != nil {
		return nil, err
	}
	return b, nil
}

func writeUvarintBuf(buf *bytes.Buffer, v uint32) {
	for v >= 0x80 {
		buf.WriteByte(byte(v) | 0x80)
		v >>= 7
	}
	buf.WriteByte(byte(v))
}

func writeCompactStringBuf(buf *bytes.Buffer, s []byte) {
	writeUvarintBuf(buf, uint32(len(s)+1))
	buf.Write(s)
}

func writeCompactNullableStringBuf(buf *bytes.Buffer, s []byte) {
	if s == nil {
		writeUvarintBuf(buf, 0)
		return
	}
	writeCompactStringBuf(buf, s)
}

func writeCompactInt32ArrayBuf(buf *bytes.Buffer, vals []uint32) {
	writeUvarintBuf(buf, uint32(len(vals)+1))
	for _, v := range vals {
		_ = binary.Write(buf, binary.BigEndian, int32(v))
	}
}

// skipCompactRecordPayload skips a nullable COMPACT_RECORDS blob (Fetch/Produce).
func skipCompactRecordPayload(r *bytes.Reader) error {
	n, err := readUvarint(r)
	if err != nil {
		return err
	}
	var payloadLen uint64
	switch {
	case n == 0:
		return nil // null
	case n == 1:
		payloadLen = 0
	default:
		payloadLen = n - 1
	}
	if payloadLen > uint64(r.Len()) {
		return fmt.Errorf("compact records length exceeds buffer")
	}
	_, err = r.Seek(int64(payloadLen), io.SeekCurrent)
	return err
}

// readCompactRecordPayload reads a nullable COMPACT_RECORDS blob (Produce/Fetch).
func readCompactRecordPayload(r *bytes.Reader) ([]byte, error) {
	n, err := readUvarint(r)
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, nil
	}
	payloadLen := n - 1
	if payloadLen > uint64(r.Len()) {
		return nil, fmt.Errorf("compact records length exceeds buffer")
	}
	if payloadLen == 0 {
		return nil, nil
	}
	buf := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}


