package common

import (
	"encoding/json"
	"fmt"
	"log"
	"net/rpc"
	"net/rpc/jsonrpc"
	"sort"

	"github.com/BitTraceProject/BitTrace-Types/pkg/common"
	"github.com/BitTraceProject/BitTrace-Types/pkg/constants"
	"github.com/BitTraceProject/BitTrace-Types/pkg/protocol"
	"github.com/BitTraceProject/BitTrace-Types/pkg/structure"

	"github.com/huandu/skiplist"
)

// ResolverHandler 完成所有 snapshot 的重排序和处理工作，
// resolver 保证处理是串行的，handler 无需考虑并发问题，
// 以 data package 为单位进行重排序，
// 以 snapshot pair 为单位进行处理，
// 处理结果写入 collector。
// - dp 间，根据 day 和 seq 排序（http 和 rpc 乱序，通过 map 维护滑动窗口解决）；
// - dp 内，根据 timestamp 排序（logger 本身乱序，通过 sort 解决）；
// - dp 间，根据 timestamp 排序，两两组装 dp（logger 本身乱序，且乱序之处恰好处于两个 dp 间，通过 skiplist 解决）;
// - 流式读取 snapshot，组装成对；
// - 处理每一对 snapshot，异步写入 collector；
// - 实现下面的三个方法，然后在 resolver 处调用 OnReceive 方法即可；
type (
	ResolverHandler interface {
		// OnReceive 接收到 dp，实现各种重排序任务，然后将排序好的 snapshot 组装成 pair，写入 stream
		OnReceive(dp protocol.ReceiverDataPackage)
		// OnResolve 从 stream 读取 snapshot pair，进行处理工作，并写入 collector
		OnResolve(snapshotPair [2]structure.Snapshot)
	}
	DefaultResolverHandler struct {
		dataPackageMap *dataPackageMap

		snapshotPairMap *snapshotPairMap

		snapshotStream chan [2]*structure.Snapshot

		collectorWriterServerAddr string
		collectorWriterClient     *rpc.Client
	}
	dataPackageMap struct {
		firstLSeq int64
		dataMap   map[int64][]*structure.Snapshot // next data lseq = lseq+len(data)
		eof       bool
	}
	snapshotPair struct {
		snapshotID    string
		initSnapshot  *structure.Snapshot
		finalSnapshot *structure.Snapshot
		final         bool
	}
	snapshotPairMap struct {
		pairSkipList *skiplist.SkipList // timestamp:id，通过 timestamp 维护 id 有序，支持循环读取 timestamp 最小的 id
		pairMap      map[string]*snapshotPair
	}
)

func NewDefaultResolverHandler(collectorWriterServerAddr string) *DefaultResolverHandler {
	h := &DefaultResolverHandler{
		dataPackageMap:            newDataPackageMap(),
		snapshotPairMap:           newSnapshotPairMap(),
		snapshotStream:            make(chan [2]*structure.Snapshot, constants.RESOLVER_SNAPSHOTPAIR_STREAM_SIZE),
		collectorWriterServerAddr: collectorWriterServerAddr,
	}
	go h.daemon() // 前台重排序，后台按序处理和写入 collector
	return h
}

func (h *DefaultResolverHandler) OnReceive(dp protocol.ReceiverDataPackage) {
	snapshotList, eof := h.dataPackageMap.PutDataPackage(dp)
	if eof {
		// 切换 day
		h.dataPackageMap = newDataPackageMap()
	}
	// newData 需要用来生成 stream
	snapshotPairList := h.snapshotPairMap.PutSnapshot(snapshotList...)
	for _, snapshotPair := range snapshotPairList {
		h.snapshotStream <- snapshotPair
	}
}

// TODO OnResolve
func (h *DefaultResolverHandler) OnResolve(snapshotPair [2]*structure.Snapshot) {
	fmt.Printf("[OnResolve]pair:[%v]\n", snapshotPair)
}

func (h *DefaultResolverHandler) CallCollectorWriterServer(collectorWriterServerAddr string, serviceMethod string, args any, reply any) error {
	// 由于本身 rpc 连接是无状态的，因此这里不必加锁就行
	return common.ExecuteFunctionByRetry(func() error {
		var err error
		if h.collectorWriterClient == nil {
			h.collectorWriterClient, err = jsonrpc.Dial("tcp", collectorWriterServerAddr)
			if err != nil {
				return err
			}
		}
		err = h.collectorWriterClient.Call(serviceMethod, args, reply)
		if err != nil {
			h.collectorWriterClient = nil
		}
		return err
	})
}

func (h *DefaultResolverHandler) daemon() {
	for {
		select {
		case snapshotPair, ok := <-h.snapshotStream:
			if ok {
				h.OnResolve(snapshotPair)
			}
		}
	}
}

func newDataPackageMap() *dataPackageMap {
	return &dataPackageMap{
		firstLSeq: 0,
		dataMap:   map[int64][]*structure.Snapshot{},
		eof:       false,
	}
}

// PutDataPackage put data in this day and return the new data that sorted(between dp) and eof status
func (m *dataPackageMap) PutDataPackage(dp protocol.ReceiverDataPackage) ([]*structure.Snapshot, bool) {
	if dp.EOF {
		m.eof = true
	}
	// 1 内部排序后，直接放入 data map
	snapshotList := m.sortRawSnapshotByTimestamp(dp.DataPackage)
	m.dataMap[dp.LeftSeq] = snapshotList

	// 2 循环判断 first 对应的 下一个 dp 是否存在，来判断是否返回 first dp data
	var data = make([]*structure.Snapshot, 0, len(m.dataMap[m.firstLSeq]))
	for {
		firstRSeq := m.firstLSeq + int64(len(m.dataMap[m.firstLSeq]))
		if _, ok := m.dataMap[firstRSeq]; ok {
			newData := m.dataMap[m.firstLSeq]
			// 设置新的 firstLSeq
			delete(m.dataMap, m.firstLSeq)
			m.firstLSeq = firstRSeq
			data = append(data, newData...)
		} else {
			break
		}
	}
	// 3 如果 eof 且剩下最后一个，则直接将最后一个也返回（假设乱序不会发生在两个 day 之间）
	if m.eof && len(m.dataMap) == 1 {
		data = append(data, m.dataMap[m.firstLSeq]...)
		return data, true
	}
	return data, false
}

func (m *dataPackageMap) sortRawSnapshotByTimestamp(data [][]byte) []*structure.Snapshot {
	snapshotList := make([]*structure.Snapshot, len(data))
	for i := range data {
		snapshotBytes, err := Base64StdDecode(data[i])
		if err != nil {
			log.Printf("[Base64StdDecode]base64 decode err:%v", err)
			continue
		}
		var snapshot structure.Snapshot
		err = json.Unmarshal(snapshotBytes, &snapshot)
		if err != nil {
			log.Printf("[sortSnapshotByTimestamp]json err:%v", err)
			continue
		}
		snapshotList[i] = &snapshot
	}
	return m.sortSnapshotByTimestamp(snapshotList)
}

func (m *dataPackageMap) sortSnapshotByTimestamp(snapshotList []*structure.Snapshot) []*structure.Snapshot {
	sort.Slice(snapshotList, func(i, j int) bool {
		return snapshotList[i].Timestamp < snapshotList[j].Timestamp
	})
	return snapshotList
}

func newSnapshotPairMap() *snapshotPairMap {
	return &snapshotPairMap{
		pairSkipList: skiplist.New(skiplist.String),
		pairMap:      map[string]*snapshotPair{},
	}
}

func (m *snapshotPairMap) PutSnapshot(snapshotList ...*structure.Snapshot) [][2]*structure.Snapshot {
	for _, snapshot := range snapshotList {
		if _, ok := m.pairMap[snapshot.ID]; !ok {
			m.pairSkipList.Set(snapshot.ID, snapshot.ID)
			m.pairMap[snapshot.ID] = &snapshotPair{
				snapshotID: snapshot.ID,
				final:      false,
			}
		}
		if snapshot.Type == structure.SnapshotTypeInit {
			m.pairMap[snapshot.ID].initSnapshot = snapshot
		} else {
			m.pairMap[snapshot.ID].finalSnapshot = snapshot
			// 不可能出现 final 在 init 前的情况
			m.pairMap[snapshot.ID].final = true
		}
	}
	snapshotPairList := [][2]*structure.Snapshot{}
	for {
		if m.pairSkipList.Front() == nil {
			break
		}
		id := m.pairSkipList.Front().Value.(string)
		if m.pairMap[id].final {
			snapshotPairList = append(snapshotPairList, [2]*structure.Snapshot{m.pairMap[id].initSnapshot, m.pairMap[id].finalSnapshot})
			m.RemoveSnapshotPair(id)
		} else {
			break
		}
	}
	return snapshotPairList
}

func (m *snapshotPairMap) RemoveSnapshotPair(id string) (*snapshotPair, bool) {
	if pair, ok := m.pairMap[id]; ok {
		delete(m.pairMap, id)
		m.pairSkipList.Remove(pair.snapshotID)
		return pair, ok
	}
	return nil, false
}
