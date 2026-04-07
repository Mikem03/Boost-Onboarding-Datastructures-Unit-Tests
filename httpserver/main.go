package main

import (
	"bytes"
	"compress/gzip"
	"encoding/hex"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"
)

// Ensures gofmt doesn't remove the "net" and "os" imports above (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

type requestInformation struct {
	headers     map[string]string
	requestType string
	URI         string
	version     string
	body        string
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// TODO: Uncomment the code below to pass the first stage
	//
	l, err := net.Listen("tcp", "0.0.0.0:4221")
	if err != nil {
		fmt.Println("Failed to bind to port 4221")
		os.Exit(1)
	}
	defer l.Close()
	//
	for {
		con, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handle(con)
	}
}
func handle(con net.Conn) {
	defer con.Close()
	for {
		err := con.SetReadDeadline(time.Now().Add(5 * time.Second))
		if err != nil {
			fmt.Println("failed time")
		}
		buffer := make([]byte, 1024)
		n, err := con.Read(buffer)
		if err != nil {
			fmt.Println("failed to read")
			return
		}
		request := buildRequestInformation(buffer, n)
		buildResponse(request, con)
		if _, ok := request.headers["Connection"]; ok && request.headers["Connection"] == "close" {
			con.Close()
		}
	}
}

// buildRequestInformation parses and builds request information
func buildRequestInformation(buffer []byte, length int) requestInformation {
	info := requestInformation{
		headers: make(map[string]string),
	}
	request := strings.Split(string(buffer[:length]), "\r\n")
	parts := strings.Split(request[0], " ")
	info.requestType = parts[0]
	info.URI = parts[1]
	info.version = parts[2]
	info.body = request[len(request)-1]
	// dont need the empty space/line that gets made by the split on /r/n
	for _, part := range request[1 : len(request)-2] {
		s := strings.Split(part, ": ")
		info.headers[s[0]] = s[1]
	}
	return info
}

// builds HTTP responses based on the type of the request
func buildResponse(req requestInformation, con net.Conn) {
	var response []byte
	var err error

	switch req.requestType {
	case "GET":
		response, err = handleGET(req)
	case "POST":
		response, err = handlePOST(req)
	default:
		response = []byte("HTTP/1.1 404 Not Found\r\n\r\n")
	}
	if err != nil {
		log.Fatal(err)
		con.Write([]byte("HTTP/1.1 404 Not Found\r\n\r\n"))
		return
	}
	con.Write(response)
}

func handleGET(req requestInformation) (response []byte, err error) {
	failedResponse := "HTTP/1.1 404 Not Found\r\n\r\n"

	if strings.Contains(req.URI, "/echo/") {
		if _, ok := req.headers["Accept-Encoding"]; ok {
			response = encodingResponse(req)
		} else if req.headers["Connection"] == "close" {
			str := strings.Split(req.URI, "/echo/")[1]
			response = []byte(fmt.Sprintf("%v 200 OK\r\nContent-Type: text/plain\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s", req.version, len(str), str))
		} else {
			s := strings.Split(req.URI, "/echo/")
			str := s[1]
			fmt.Println("Inside Else")
			response = []byte(fmt.Sprintf("%v 200 OK\r\nContent-Type: text/plain\r\nContent-Length: %d\r\n\r\n%s", req.version, len(str), str))
		}

	} else if strings.Contains(req.URI, "/files/") {
		s := strings.Split(req.URI, "/files/")
		str := s[1]
		absPath := fmt.Sprintf("/tmp/data/codecrafters.io/http-server-tester/%s", str)
		fileContent, err := os.ReadFile(absPath)
		if err != nil {
			fmt.Println("read file failed")
			return []byte(failedResponse), nil
		}
		fmt.Println("Made it")
		fileLength := len(fileContent)
		response = []byte(fmt.Sprintf("%v 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: %d\r\n\r\n%v", req.version, fileLength, string(fileContent)))

	} else if req.URI == "/" {
		return []byte("HTTP/1.1 200 OK\r\n\r\n"), nil

	} else if strings.Contains(req.URI, "/user-agent") {
		userAgent := req.headers["User-Agent"]
		response = []byte(fmt.Sprintf("%v 200 OK\r\nContent-type: text/plain\r\nContent-length: %d\r\n\r\n%v", req.version, len(userAgent), userAgent))

	} else {
		response = []byte(failedResponse)
	}

	fmt.Println("Response:", response, "EndResponse")

	return response, nil
}

// strings.Split("/files/", lines[4])[1]
func handlePOST(req requestInformation) (response []byte, err error) {
	s := strings.Split(req.URI, "/files/")
	fileName := s[1]
	absPath := fmt.Sprintf("/tmp/data/codecrafters.io/http-server-tester/%s", fileName)
	data := []byte(req.body)
	err = os.WriteFile(absPath, data, 0666)
	if err != nil {
		log.Fatal(err)
		return []byte("HTTP/1.1 404 Not Found\r\n\r\n"), err
	}
	return []byte("HTTP/1.1 201 Created\r\n\r\n"), nil
}

func encodingResponse(req requestInformation) (response []byte) {
	if strings.Contains(req.headers["Accept-Encoding"], "gzip") {
		str := strings.Split(req.URI, "/echo/")[1]
		fmt.Println("str line:", str)
		var buf bytes.Buffer
		data := []byte(str)
		hexData := hex.EncodeToString(data)
		binaryData, err := hex.DecodeString(hexData)
		if err != nil {
			fmt.Println("failed hex decode")
		}

		gz := gzip.NewWriter(&buf)
		if _, err := gz.Write(binaryData); err != nil {
			fmt.Println("failed gz write")
		}
		if err := gz.Close(); err != nil {
			fmt.Println("failed close")
		}
		size := buf.Len()
		compressedFile := buf.Bytes()
		headers := fmt.Sprintf("%v 200 OK\r\nContent-Encoding: gzip\r\nContent-Type: text/plain\r\nContent-Length: %d\r\n\r\n", req.version, size)
		response = append([]byte(headers), compressedFile...)
	} else {
		response = []byte(fmt.Sprintf("%v 200 OK\r\nContent-Type: txt/plain\r\n\r\n", req.version))
	}
	return response
}
