package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

const maxKafkaFrameBytes = 16 * 1024 * 1024


func readRequestPayload(con net.Conn) ([]byte, error) {
	var msgSize int32
	if err := binary.Read(con, binary.BigEndian, &msgSize); err != nil {
		return nil, err
	}
	if msgSize < 0 || int64(msgSize) > int64(maxKafkaFrameBytes) {
		return nil, fmt.Errorf("invalid message size %d", msgSize)
	}
	msg := make([]byte, msgSize)
	if _, err := io.ReadFull(con, msg); err != nil {
		return nil, err
	}
	return msg, nil
}

func handle(con net.Conn) {
	defer con.Close()
	for {
		msg, err := readRequestPayload(con)
		if err != nil {
			if err != io.EOF && err != io.ErrUnexpectedEOF {
				fmt.Println("read frame:", err)
			}
			return
		}
		req, err := buildRequestInformation(msg)
		if err != nil {
			fmt.Println("parse request:", err)
			return
		}
		res := buildResponse(req)
		if err := sendResponse(res, con); err != nil {
			fmt.Println(err)
		}
	}
}

func startServer() error {
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		return fmt.Errorf("Failed to bind to port 9092")
	}

	defer l.Close()

	if err := readLogs(); err != nil {
		return fmt.Errorf("failed to read logs")
	}
	for {
		con, err := l.Accept()
		if err != nil {
			return fmt.Errorf("Error accepting connection")
		}

		go handle(con)
	}
}
