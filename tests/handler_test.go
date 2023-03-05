package tests

import (
	"bufio"
	"os"
	"testing"
	"time"

	"github.com/BitTraceProject/BitTrace-Resolver/server"
	"github.com/BitTraceProject/BitTrace-Types/pkg/config"
	"github.com/BitTraceProject/BitTrace-Types/pkg/protocol"
)

var (
	dataLeftSeq int64 = 0
)

func TestResolverHandler(t *testing.T) {
	var testCases = []protocol.ReceiverDataPackage{
		genDataPackage("2023-01-14", 5),
		genDataPackage("2023-01-14", 15),
		genDataPackage("2023-01-14", 30),
		genDataPackage("2023-01-14", 20),
		genDataPackage("2023-01-14", 60),
	}

	resolverHandler := server.NewDefaultResolverHandler("e", "a", config.DatabaseConfig{})
	for _, dp := range testCases {
		resolverHandler.OnReceive(dp)
	}
	time.Sleep(3 * time.Minute)
}

func genDataPackage(day string, n int64) protocol.ReceiverDataPackage {
	l, r := dataLeftSeq, dataLeftSeq+n
	return protocol.ReceiverDataPackage{
		Day:         day,
		LeftSeq:     l,
		RightSeq:    r,
		DataPackage: genSnapshotData(n),
		EOF:         false,
	}
}

func genSnapshotData(n int64) [][]byte {
	f, err := os.OpenFile("./test.txt", os.O_RDWR, os.ModePerm)
	if err != nil {
		panic(err)
	}
	r := bufio.NewReader(f)
	for i := int64(0); i < dataLeftSeq; i++ {
		_, err := r.ReadBytes('\n')
		if err != nil {
			break
		}
	}

	dataset := [][]byte{}
	for i := int64(0); i < n; i++ {
		data, err := r.ReadBytes('\n')
		if err != nil {
			break
		}
		dataset = append(dataset, data[:len(data)-1])
	}

	dataLeftSeq += n
	if n > int64(len(dataset)) {
		panic("data not enough")
	}
	return dataset
}
