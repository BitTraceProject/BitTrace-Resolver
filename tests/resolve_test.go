package tests

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/BitTraceProject/BitTrace-Resolver/common"
	"github.com/BitTraceProject/BitTrace-Types/pkg/protocol"
	"github.com/BitTraceProject/BitTrace-Types/pkg/structure"
)

func TestResolverHandler(t *testing.T) {
	var testCases = []protocol.ReceiverDataPackage{
		{
			Day:      "day1",
			LeftSeq:  0,
			RightSeq: 2,
			DataPackage: [][]byte{
				genSnapshotData("s1", structure.SnapshotTypeInit, 1),
				genSnapshotData("s2", structure.SnapshotTypeInit, 2),
			},
			EOF: false,
		},
		{
			Day:      "day1",
			LeftSeq:  2,
			RightSeq: 4,
			DataPackage: [][]byte{
				genSnapshotData("s2", structure.SnapshotTypeFinal, 5),
				genSnapshotData("s3", structure.SnapshotTypeInit, 3),
			},
			EOF: false,
		},
		{
			Day:      "day1",
			LeftSeq:  4,
			RightSeq: 6,
			DataPackage: [][]byte{
				genSnapshotData("s1", structure.SnapshotTypeFinal, 4),
				genSnapshotData("s4", structure.SnapshotTypeInit, 6),
			},
			EOF: true,
		},
		{
			Day:      "day2",
			LeftSeq:  0,
			RightSeq: 2,
			DataPackage: [][]byte{
				genSnapshotData("s5", structure.SnapshotTypeInit, 8),
				genSnapshotData("s4", structure.SnapshotTypeFinal, 7),
			},
			EOF: false,
		},
		{
			Day:      "day2",
			LeftSeq:  2,
			RightSeq: 4,
			DataPackage: [][]byte{
				genSnapshotData("s3", structure.SnapshotTypeFinal, 9),
				genSnapshotData("s5", structure.SnapshotTypeFinal, 12),
			},
			EOF: false,
		},
		{
			Day:      "day2",
			LeftSeq:  4,
			RightSeq: 6,
			DataPackage: [][]byte{
				genSnapshotData("s6", structure.SnapshotTypeInit, 10),
				genSnapshotData("s6", structure.SnapshotTypeFinal, 11),
			},
			EOF: true,
		},
	}

	resolverHandler := common.NewDefaultResolverHandler("")
	for _, dp := range testCases {
		resolverHandler.OnReceive(dp)
	}
	time.Sleep(3 * time.Second)
}

func genSnapshotData(id string, t structure.SnapshotType, timestamp int64) []byte {
	s := structure.Snapshot{
		ID:                id,
		TargetChainID:     "",
		TargetChainHeight: 0,
		Type:              t,
		Timestamp:         structure.Timestamp(timestamp),
		RevisionList:      nil,
	}
	data, _ := json.Marshal(s)
	return data
}
