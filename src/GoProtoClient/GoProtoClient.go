package main

import (
	"ProtobufTest"
	"bytes"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"

	"github.com/golang/protobuf/proto"
)

type Headers []string

const CLIENT_NAME = "GoClient"
const CLIENT_ID = 2
const CLIENT_DESCRIPTION = "This is a Go Protobuf client!!"

func main() {
	filename := flag.String("f", "CSVV.csv", "Enter the filename to read from")
	dest := flag.String("d", "127.0.0.1:2110", "Enter the destnation socket address")
	flag.Parse()
	data, err := retrieveDataFromFile(filename)
	checkError(err)
	fmt.Println("data length, ", len(data))
	bytesWriter := bytes.Buffer{}
	bytesWriter.Write(data)
	sendDataToDest(bytesWriter.Bytes(), dest)
}

func writeMsg(msg []byte, out io.Writer) (int, error) {
	bodyLen := len(msg)
	total := 0
	n, err := out.Write(proto.EncodeVarint(uint64(bodyLen)))
	if err != nil {
		return 0, err
	}
	fmt.Println("header", n, "body", bodyLen)
	total += n
	n, err = out.Write(msg)
	total += n
	return total, err
}

func retrieveDataFromFile(fname *string) ([]byte, error) {
	file, err := os.Open(*fname)
	checkError(err)
	defer file.Close()

	csvreader := csv.NewReader(file)
	var hdrs Headers
	hdrs, err = csvreader.Read()
	checkError(err)
	ITEMIDINDEX := hdrs.getHeaderIndex("itemid")
	ITEMNAMEINDEX := hdrs.getHeaderIndex("itemname")
	ITEMVALUEINDEX := hdrs.getHeaderIndex("itemvalue")
	ITEMTYPEINDEX := hdrs.getHeaderIndex("itemType")

	ProtoMessage := new(ProtobufTest.TestMessage)
	ProtoMessage.ClientName = proto.String(CLIENT_NAME)
	ProtoMessage.ClientId = proto.Int32(CLIENT_ID)
	ProtoMessage.Description = proto.String(CLIENT_DESCRIPTION)

	//loop through the records
	for {
		record, err := csvreader.Read()
		if err != io.EOF {
			checkError(err)
		} else {

			break
		}
		//Populate items
		testMessageItem := new(ProtobufTest.TestMessage_MsgItem)
		itemid, err := strconv.Atoi(record[ITEMIDINDEX])
		checkError(err)
		testMessageItem.Id = proto.Int32(int32(itemid))
		testMessageItem.ItemName = &record[ITEMNAMEINDEX]
		itemvalue, err := strconv.Atoi(record[ITEMVALUEINDEX])
		checkError(err)
		testMessageItem.ItemValue = proto.Int32(int32(itemvalue))
		itemtype, err := strconv.Atoi(record[ITEMTYPEINDEX])
		checkError(err)
		iType := ProtobufTest.TestMessage_ItemType(itemtype)
		testMessageItem.ItemType = &iType

		ProtoMessage.Messageitems = append(ProtoMessage.Messageitems, testMessageItem)

		fmt.Println(record)
	}

	//fmt.Println(ProtoMessage.Messageitems)
	buffer, err := proto.Marshal(ProtoMessage)
	if err != nil {
		return nil, err
	}
	proto.EncodeVarint(uint64(len(buffer)))
	return buffer, nil
}

func sendDataToDest(data []byte, dst *string) {
	conn, err := net.Dial("tcp", *dst)
	checkError(err)

	n, err := writeMsg(data, conn)
	checkError(err)
	fmt.Println("Sent " + strconv.Itoa(n) + " bytes")

	n, err = writeMsg(data, conn)
	checkError(err)
	fmt.Println("Sent " + strconv.Itoa(n) + " bytes")
}

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}

func (h Headers) getHeaderIndex(headername string) int {
	if len(headername) >= 2 {
		for index, s := range h {
			if s == headername {
				return index
			}
		}
	}
	return -1
}
