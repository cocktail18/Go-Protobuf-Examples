package main

import (
	"ProtobufTest"
	"bytes"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/golang/protobuf/proto"
)

func main() {
	fmt.Printf("Started ProtoBuf Server")
	c := make(chan *ProtobufTest.TestMessage)
	go func() {
		for {
			message := <-c
			fmt.Printf("message %v, %v, %v", *message.ClientId, *message.ClientName, *message.Description)
		}
	}()
	//Listen to the TCP port
	listener, err := net.Listen("tcp", "127.0.0.1:2110")
	checkError(err)
	for {
		if conn, err := listener.Accept(); err == nil {
			//If err is nil then that means that data is available for us so we take up this data and pass it to a new goroutine
			go handleProtoClient(conn, c)
		} else {
			continue
		}
	}
}

func handleProtoClient(conn net.Conn, c chan *ProtobufTest.TestMessage) {
	fmt.Println("Connection established")
	//Close the connection when the function exits
	defer conn.Close()

	BYTES_SIZE := 1024
	var (
		buffer           = bytes.NewBuffer(make([]byte, 0, BYTES_SIZE))
		bytes            = make([]byte, 10)
		isHead      bool = true
		contentSize uint64
		content     = make([]byte, BYTES_SIZE)
	)
	for {
		readLen, err := conn.Read(bytes)
		if err != nil {
			log.Println("Error reading", err.Error())
			return
		}
		_, err = buffer.Write(bytes[0:readLen])
		if err != nil {
			log.Println("Error writing to buffer", err.Error())
			return
		}

		for {
			if isHead {
				length, n := proto.DecodeVarint(buffer.Bytes())
				if length == 0 || n == 0 { // not enougth
					break
				} else {
					_, err := buffer.Read(make([]byte, n))
					if err != nil {
						fmt.Println("Error reading", err.Error())
						return
					}
					contentSize = length
					isHead = false
				}
			}
			if !isHead {
				if uint64(buffer.Len()) >= contentSize {
					_, err := buffer.Read(content[:contentSize])
					if err != nil {
						fmt.Println("Error reading", err.Error())
						return
					}
					model := &ProtobufTest.TestMessage{}
					err = model.XXX_Unmarshal(content[:contentSize])
					if err != nil {
						fmt.Println(err.Error())
						return
					}
					c <- model
					isHead = true
				} else {
					break
				}
			}
		}
	}

}

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}
