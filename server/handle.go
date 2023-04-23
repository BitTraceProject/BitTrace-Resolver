package server

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/rpc"
	"net/rpc/jsonrpc"
	"sort"

	"github.com/BitTraceProject/BitTrace-Types/pkg/common"
	"github.com/BitTraceProject/BitTrace-Types/pkg/config"
	"github.com/BitTraceProject/BitTrace-Types/pkg/constants"
	"github.com/BitTraceProject/BitTrace-Types/pkg/database"
	"github.com/BitTraceProject/BitTrace-Types/pkg/logger"
	"github.com/BitTraceProject/BitTrace-Types/pkg/metric"
	"github.com/BitTraceProject/BitTrace-Types/pkg/protocol"
	"github.com/BitTraceProject/BitTrace-Types/pkg/structure"

	"gorm.io/gorm"
)

// resolver handler

var _ ResolverHandler = &DefaultResolverHandler{}

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
type (
	ResolverHandler interface {
		// OnReceive 接收到 dp，实现各种重排序任务，然后将排序好的 snapshot 组装成 pair，写入 stream
		OnReceive(dp protocol.ReceiverDataPackage)
		// OnResolve 从 stream 读取 snapshot pair，进行处理工作，并写入 collector
		OnResolve(snapshotPairList []*snapshotPair)
	}
	DefaultResolverHandler struct {
		dataPackageMap *dataPackageMap

		snapshotPairMap *snapshotPairMap

		snapshotStream chan []*snapshotPair

		dbConf       config.DatabaseConfig
		masterDBInst *gorm.DB

		exporterTag   string
		exporterTable *protocol.ExporterTable

		metaServerAddr string
		metaClient     *rpc.Client

		resolverHandlerLogger logger.Logger
	}
	dataPackageMap struct {
		exporterTag string

		firstLSeq int64
		dataMap   map[int64][]*structure.Snapshot // next data lseq = lseq+len(data)
		eof       bool

		resolverHandlerLogger logger.Logger
	}
	snapshotPair struct {
		snapshotID string
		// pair: init+final
		initSnapshot  *structure.Snapshot
		finalSnapshot *structure.Snapshot
		// sync
		syncSnapshot *structure.Snapshot
	}
	snapshotPairMap struct {
		exporterTag string

		pairMap map[string]*snapshotPair

		resolverHandlerLogger logger.Logger
	}
)

func NewDefaultResolverHandler(exporterTag, metaServerAddr string, dbConf config.DatabaseConfig) *DefaultResolverHandler {
	resolverHandlerLogger := logger.GetLogger(fmt.Sprintf("bittrace_resolver_handler_%s", exporterTag))
	h := &DefaultResolverHandler{
		dataPackageMap:        newDataPackageMap(exporterTag, resolverHandlerLogger),
		snapshotPairMap:       newSnapshotPairMap(exporterTag, resolverHandlerLogger),
		snapshotStream:        make(chan []*snapshotPair, constants.RESOLVER_SNAPSHOTPAIR_STREAM_SIZE),
		exporterTag:           exporterTag,
		dbConf:                dbConf,
		metaServerAddr:        metaServerAddr,
		resolverHandlerLogger: resolverHandlerLogger,
	}
	go h.daemon() // 前台重排序，后台按序处理和写入 collector
	return h
}

func (h *DefaultResolverHandler) OnReceive(dp protocol.ReceiverDataPackage) {
	h.resolverHandlerLogger.Info("[OnReceive]receive data pakcage:%s,%d,%d,%v,%d", dp.Day, dp.LeftSeq, dp.RightSeq, dp.EOF, len(dp.DataPackage))
	allSnapshotList, eof := h.dataPackageMap.PutDataPackage(dp)
	h.resolverHandlerLogger.Info("[OnReceive]put data pakcage finished, snapshot list length:%d,eof:%v", len(allSnapshotList), eof)
	if eof {
		// 切换 day
		h.dataPackageMap = newDataPackageMap(h.exporterTag, h.resolverHandlerLogger)
	}

	// 这里对 snapshot list 过滤一下，把 sync 的拿出来直接传入 OnResolve 方法
	syncSnapshotPairList := make([]*snapshotPair, 0)
	othersSnapshotPairList := make([]*snapshotPair, 0)
	for _, snapshot := range allSnapshotList {
		h.resolverHandlerLogger.Info("[OnReceive]process snapshot:%+v", snapshot)
		if snapshot.Type == structure.SnapshotTypeUnknown {
			h.resolverHandlerLogger.Warn("[OnReceive]unknown type snapshot:%+v", snapshot)
			continue
		}
		if snapshot.Type == structure.SnapshotTypeSync {
			syncSnapshotPair := &snapshotPair{
				snapshotID:    snapshot.ID,
				initSnapshot:  nil,
				finalSnapshot: nil,

				syncSnapshot: snapshot,
			}
			syncSnapshotPairList = append(syncSnapshotPairList, syncSnapshotPair)
		} else {
			if snapshotPairFinal := h.snapshotPairMap.PutSnapshotData(snapshot); snapshotPairFinal != nil {
				othersSnapshotPairList = append(othersSnapshotPairList, snapshotPairFinal)
			}

			// metric
			if snapshot.Type == structure.SnapshotTypeInit {
				metric.MetricLogSnapshotStage(metric.MetricSnapshotStage{
					Tag:        h.exporterTag,
					SnapshotID: snapshot.ID,
					Timestamp:  common.FromNow().String(),
					Stage:      metric.StageTypeInit,
				})
			} else if snapshot.Type == structure.SnapshotTypeFinal {
				metric.MetricLogSnapshotStage(metric.MetricSnapshotStage{
					Tag:        h.exporterTag,
					SnapshotID: snapshot.ID,
					Timestamp:  common.FromNow().String(),
					Stage:      metric.StageTypeFinal,
				})
			}
		}
	}

	// 先处理 sync，这个顺序不强制要求， sync 和 data 混搭其实对于 resolver 处理来说没有影响
	h.snapshotStream <- syncSnapshotPairList
	h.resolverHandlerLogger.Info("[OnReceive]put sync snapshot finished, snapshot pair list length:%d", len(syncSnapshotPairList))
	// 再处理 data
	h.snapshotStream <- othersSnapshotPairList
	h.resolverHandlerLogger.Info("[OnReceive]put others snapshot finished, snapshot pair list length:%d", len(othersSnapshotPairList))
}

func (h *DefaultResolverHandler) OnResolve(snapshotPairList []*snapshotPair) {
	for i, pair := range snapshotPairList {
		h.resolverHandlerLogger.Info("[OnResolve]%d-pair:[%v]\n", i, *pair)
		if pair.syncSnapshot != nil {
			syncSnapshot := pair.syncSnapshot
			snapshotSync := database.TableSnapshotSync{
				SnapshotID:        syncSnapshot.ID,
				TargetChainID:     syncSnapshot.TargetChainID,
				TargetChainHeight: syncSnapshot.TargetChainHeight,
				SyncTimestamp:     syncSnapshot.Timestamp.String(),
			}

			syncState := syncSnapshot.State
			stateSync := database.TableState{
				SnapshotID:      syncSnapshot.ID,
				SnapshotType:    structure.SnapshotTypeSync,
				BestBlockHash:   syncState.Hash,
				Height:          syncState.Height,
				Bits:            syncState.Bits,
				BlockSize:       syncState.BlockSize,
				BlockWeight:     syncState.BlockWeight,
				NumTxns:         syncState.NumTxns,
				TotalTxns:       syncState.TotalTxns,
				MedianTimestamp: syncState.MedianTimestamp.String(),
			}

			sqlList := []string{
				database.SqlInsertSnapshotSync(h.exporterTable.TableNameSnapshotSync, snapshotSync),
				database.SqlInsertState(h.exporterTable.TableNameState, stateSync),
			}
			var err error
			h.masterDBInst, err = database.TryExecPipelineSql(h.masterDBInst, sqlList, h.dbConf)
			if err != nil {
				h.resolverHandlerLogger.Error("[OnResolve]call database exec pipeline error:%v,%v", err, sqlList)

				// metric
				metric.MetricLogModuleError(metric.MetricModuleError{
					Tag:       h.exporterTag,
					Timestamp: common.FromNow().String(),
					Module:    metric.ModuleTypeResolverHandler,
				})
				continue
			}
		} else if pair.initSnapshot != nil && pair.finalSnapshot != nil {
			initSnapshot := pair.initSnapshot
			finalSnapshot := pair.finalSnapshot
			isOrphan := 0
			if initSnapshot.IsOrphan {
				isOrphan = 1
			}
			snapshotData := database.TableSnapshotData{
				SnapshotID:        initSnapshot.ID,
				TargetChainID:     initSnapshot.TargetChainID,
				TargetChainHeight: initSnapshot.TargetChainHeight,
				BlockHash:         initSnapshot.BlockHash,
				InitTimestamp:     initSnapshot.Timestamp.String(),
				IsOrphan:          isOrphan,
				FinalTimestamp:    finalSnapshot.Timestamp.String(),
			}

			initState := pair.initSnapshot.State
			finalState := pair.finalSnapshot.State
			stateInit := database.TableState{
				SnapshotID:      initSnapshot.ID,
				SnapshotType:    structure.SnapshotTypeInit,
				BestBlockHash:   initState.Hash,
				Height:          initState.Height,
				Bits:            initState.Bits,
				BlockSize:       initState.BlockSize,
				BlockWeight:     initState.BlockWeight,
				NumTxns:         initState.NumTxns,
				TotalTxns:       initState.TotalTxns,
				MedianTimestamp: initState.MedianTimestamp.String(),
			}
			stateFinal := database.TableState{
				SnapshotID:      initSnapshot.ID,
				SnapshotType:    structure.SnapshotTypeFinal,
				BestBlockHash:   finalState.Hash,
				Height:          finalState.Height,
				Bits:            finalState.Bits,
				BlockSize:       finalState.BlockSize,
				BlockWeight:     finalState.BlockWeight,
				NumTxns:         finalState.NumTxns,
				TotalTxns:       finalState.TotalTxns,
				MedianTimestamp: finalState.MedianTimestamp.String(),
			}

			sqlList := []string{
				database.SqlInsertSnapshotData(h.exporterTable.TableNameSnapshotData, snapshotData),
				database.SqlInsertState(h.exporterTable.TableNameState, stateInit, stateFinal),
			}
			revisionList := make([]database.TableRevision, 0, len(pair.finalSnapshot.RevisionList))
			for _, r := range pair.finalSnapshot.RevisionList {
				initData := common.StructToJsonStr(r.InitData)
				finalData := common.StructToJsonStr(r.FinalData)
				revision := database.TableRevision{
					SnapshotID:     initSnapshot.ID,
					RevisionType:   int(r.Type),
					InitTimestamp:  r.InitTimestamp.String(),
					FinalTimestamp: r.FinalTimestamp.String(),
					InitData:       initData,
					FinalData:      finalData,
				}
				revisionList = append(revisionList, revision)
			}
			sqlList = append(sqlList, database.SqlInsertRevision(h.exporterTable.TableNameRevision, revisionList...))

			eventOrphanList := make([]database.TableEventOrphan, 0, len(pair.finalSnapshot.EventOrphanList))
			for _, e := range pair.finalSnapshot.EventOrphanList {
				connectMainChain := 0
				if e.ConnectMainChain {
					connectMainChain = 1
				}
				eventOrphan := database.TableEventOrphan{
					SnapshotID:            e.SnapshotID,
					EventTypeOrphan:       int(e.Type),
					OrphanParentBlockHash: e.OrphanParentBlockHash,
					OrphanBlockHash:       e.OrphanBlockHash,
					ConnectMainChain:      connectMainChain,
					EventOrphanTimestamp:  e.Timestamp.String(),
				}
				eventOrphanList = append(eventOrphanList, eventOrphan)
			}
			sqlList = append(sqlList, database.SqlInsertOrphanEvent(h.exporterTable.TableNameEventOrphan, eventOrphanList...))

			var err error
			h.masterDBInst, err = database.TryExecPipelineSql(h.masterDBInst, sqlList, h.dbConf)
			if err != nil {
				h.resolverHandlerLogger.Error("[OnResolve]call database exec pipeline error:%v,%v", err, sqlList)

				// metric
				metric.MetricLogSnapshotStage(metric.MetricSnapshotStage{
					Tag:        h.exporterTag,
					SnapshotID: pair.initSnapshot.ID,
					Timestamp:  common.FromNow().String(),
					Stage:      metric.StageTypeError,
				})
				metric.MetricLogModuleError(metric.MetricModuleError{
					Tag:       h.exporterTag,
					Timestamp: common.FromNow().String(),
					Module:    metric.ModuleTypeResolverHandler,
				})
				continue
			}

			// metric
			metric.MetricLogSnapshotStage(metric.MetricSnapshotStage{
				Tag:        h.exporterTag,
				SnapshotID: pair.initSnapshot.ID,
				Timestamp:  common.FromNow().String(),
				Stage:      metric.StageTypePair,
			})
		} else {
			continue // 不会发生这种情况
		}
	}
}

func (h *DefaultResolverHandler) tryFetchTable() error {
	exporterInfoKey := common.GenExporterInfoKey(h.exporterTag)
	getTableArgs := &protocol.MetaGetExporterTableArgs{Key: exporterInfoKey}
	var getTableReply protocol.MetaGetExporterTableReply
	err := h.CallMetaServer("MetaServerAPI.GetExporterTable", getTableArgs, &getTableReply)
	if err != nil {
		return fmt.Errorf("[tryFetchTable]call meta get exporter infoerror:%v", err)
	}
	if !getTableReply.OK {
		return errors.New("[tryFetchTable]call meta get exporter info not ok")
	}
	if !getTableReply.HasJoin {
		return errors.New("[tryFetchTable]call meta get exporter info has not join")
	}
	h.exporterTable = &getTableReply.Table
	return nil
}

func (h *DefaultResolverHandler) daemon() {
	for {
		select {
		case snapshotPairList, ok := <-h.snapshotStream:
			if ok {
				if h.exporterTable == nil {
					if err := h.tryFetchTable(); err != nil {
						h.snapshotStream <- snapshotPairList
						h.resolverHandlerLogger.Error("[daemon]try fetch table error:%v", err)

						// metric
						metric.MetricLogModuleError(metric.MetricModuleError{
							Tag:       h.exporterTag,
							Timestamp: common.FromNow().String(),
							Module:    metric.ModuleTypeResolverHandler,
						})
						continue
					}
				}
				if h.exporterTable != nil {
					h.OnResolve(snapshotPairList)
				}
			}
		}
	}
}

func (h *DefaultResolverHandler) CallMetaServer(serviceMethod string, args any, reply any) error {
	// 由于本身 rpc 连接是无状态的，因此这里不必加锁就行
	return common.ExecuteFunctionByRetry(func() error {
		var err error
		if h.metaClient == nil {
			h.metaClient, err = jsonrpc.Dial("tcp", h.metaServerAddr)
			if err != nil {
				return err
			}
		}
		err = h.metaClient.Call(serviceMethod, args, reply)
		if err != nil {
			h.metaClient = nil
		}
		return err
	})
}

func newDataPackageMap(exporterTag string, resolverHandlerLogger logger.Logger) *dataPackageMap {
	return &dataPackageMap{
		exporterTag:           exporterTag,
		firstLSeq:             0,
		dataMap:               map[int64][]*structure.Snapshot{},
		eof:                   false,
		resolverHandlerLogger: resolverHandlerLogger,
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
		dataI, err := base64.StdEncoding.DecodeString(string(data[i]))
		if err != nil {
			m.resolverHandlerLogger.Error("[sortRawSnapshotByTimestamp]base64 decode err:%v", err)

			// metric
			metric.MetricLogModuleError(metric.MetricModuleError{
				Tag:       m.exporterTag,
				Timestamp: common.FromNow().String(),
				Module:    metric.ModuleTypeResolverHandler,
			})
			continue
		}
		var snapshot structure.Snapshot
		err = json.Unmarshal(dataI, &snapshot)
		if err != nil {
			m.resolverHandlerLogger.Error("[sortRawSnapshotByTimestamp]json unmarshal err:%v,data:%s", err, data[i])

			// metric
			metric.MetricLogModuleError(metric.MetricModuleError{
				Tag:       m.exporterTag,
				Timestamp: common.FromNow().String(),
				Module:    metric.ModuleTypeResolverHandler,
			})
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

func newSnapshotPairMap(exporterTag string, resolverHandlerLogger logger.Logger) *snapshotPairMap {
	return &snapshotPairMap{
		exporterTag:           exporterTag,
		pairMap:               map[string]*snapshotPair{},
		resolverHandlerLogger: resolverHandlerLogger,
	}
}

func (m *snapshotPairMap) PutSnapshotData(snapshot *structure.Snapshot) *snapshotPair {
	if _, ok := m.pairMap[snapshot.ID]; !ok {
		m.pairMap[snapshot.ID] = &snapshotPair{
			snapshotID: snapshot.ID,
		}
	}
	if snapshot.Type == structure.SnapshotTypeInit {
		m.pairMap[snapshot.ID].initSnapshot = snapshot
		if m.pairMap[snapshot.ID].finalSnapshot != nil {
			return m.pairMap[snapshot.ID]
		}
	} else if snapshot.Type == structure.SnapshotTypeFinal {
		m.pairMap[snapshot.ID].finalSnapshot = snapshot
		if m.pairMap[snapshot.ID].initSnapshot != nil {
			return m.pairMap[snapshot.ID]
		}
	}
	return nil
}
