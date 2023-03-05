package server

import (
	"encoding/json"
	"fmt"
	"net/rpc"
	"net/rpc/jsonrpc"
	"time"

	"github.com/BitTraceProject/BitTrace-Types/pkg/common"
	"github.com/BitTraceProject/BitTrace-Types/pkg/config"
	"github.com/BitTraceProject/BitTrace-Types/pkg/constants"
	"github.com/BitTraceProject/BitTrace-Types/pkg/logger"
	"github.com/BitTraceProject/BitTrace-Types/pkg/metric"
	"github.com/BitTraceProject/BitTrace-Types/pkg/protocol"
)

type (
	ResolverServer struct {
		resolverTag string // resolver tag
		exporterTag string // 对应的 exporter tag，从 mq 消费消息

		conf config.ResolverConfig

		metaClient *rpc.Client
		mqClient   *rpc.Client

		hasShutdown  bool
		lazyShutdown bool

		stopCh chan bool

		resolverHandler ResolverHandler

		resolverLogger logger.Logger
	}
)

func NewResolverServer(conf config.ResolverConfig, resolverTag, exporterTag string, resolverHandler ResolverHandler) *ResolverServer {
	s := &ResolverServer{
		resolverTag:     resolverTag,
		exporterTag:     exporterTag,
		conf:            conf,
		stopCh:          make(chan bool, 1),
		resolverHandler: resolverHandler,
		resolverLogger:  logger.GetLogger(fmt.Sprintf("bittrace_resolver_%s", exporterTag)),
	}
	go s.Start()
	return s
}

func (s *ResolverServer) Start() {
	// 定时轮询消费 mq，然后根据 mq 返回的 has next 选择立即消费 mq，还是继续定时轮询（每一次轮询结束重置定时，保证消费是串行的）
	timer := time.NewTicker(constants.RESOLVER_CONSUME_MQ_INTERVAL) // 使用 timer 而非 ticker 保证消费是串行的
	hasNextChan := make(chan bool, 1)
	for {
		select {
		case <-timer.C:
			// 轮询 mq
			msg, hasNext, ok := s.consume()
			if ok {
				// 处理并存储
				s.resolve(msg)
			}
			if hasNext {
				// 判断 hasNext
				hasNextChan <- true
			} else {
				// 重置 timer
				timer.Reset(constants.RESOLVER_CONSUME_MQ_INTERVAL)
			}
		case <-hasNextChan:
			// 轮询 mq
			msg, hasNext, ok := s.consume()
			if ok {
				// 处理并存储
				s.resolve(msg)
			}
			if hasNext {
				// 判断 hasNext
				hasNextChan <- true
			} else {
				// 重置 timer
				timer.Reset(constants.RESOLVER_CONSUME_MQ_INTERVAL)
			}
		case <-s.stopCh:
			return // 停止
		} // select
	}
}

func (s *ResolverServer) Shutdown(lazyShutdown bool) {
	if !lazyShutdown {
		// 直接调用 mq 的 clear 方法
		clearMessageArgs := &protocol.MqClearMessageArgs{Tag: s.resolverTag}
		var clearMessageReply protocol.MqClearMessageReply
		err := s.CallMqServer("MqServerAPI.ClearMessage", clearMessageArgs, &clearMessageReply)
		if err != nil {
			s.resolverLogger.Error("[Shutdown]MqServerAPI.ClearMessage error:%v", err)

			// metric
			metric.MetricLogModuleError(metric.MetricModuleError{
				Tag:       s.exporterTag,
				Timestamp: common.FromNow().String(),
				Module:    metric.ModuleTypeResolver,
			})
		}
		s.stopCh <- true
		close(s.stopCh)
		s.hasShutdown = true
		s.resolverLogger.Info("[Shutdown][%s]resolver has been shutdown", s.exporterTag)
		return
	}
	// 否则设置全局标记位，使得下一次 hasNext 为 false 时直接退出 (consume 处)
	s.lazyShutdown = true
	s.resolverLogger.Info("[Shutdown][%s]resolver will be shutdown lazy", s.exporterTag)
	// 并且，更新 exporter status 为 lazyquit
	exporterInfoKey := common.GenExporterInfoKey(s.exporterTag)
	updateStatusArgs := &protocol.MetaUpdateExporterStatusArgs{Key: exporterInfoKey, StatusCode: protocol.StatusLazyQuit, QuitTimestamp: common.FromNow()}
	var updateStatusReply protocol.MetaUpdateExporterStatusReply
	err := s.CallMetaServer("MetaServerAPI.UpdateExporterStatus", updateStatusArgs, &updateStatusReply)
	if err != nil {
		s.resolverLogger.Error("[Shutdown]call meta update exporter status error:%v", err)

		// metric
		metric.MetricLogModuleError(metric.MetricModuleError{
			Tag:       s.exporterTag,
			Timestamp: common.FromNow().String(),
			Module:    metric.ModuleTypeResolver,
		})
	}
	if !updateStatusReply.OK {
		s.resolverLogger.Error("[Shutdown]call meta update exporter status not ok")
	}
}

// consume resolver 重启之后无法 consume 了
func (s *ResolverServer) consume() (protocol.MqMessage, bool, bool) {
	if s.hasShutdown {
		return protocol.MqMessage{}, false, false
	}
	filterMessageArgs := &protocol.MqFilterMessageArgs{Tag: s.resolverTag}
	var filterMessageReply protocol.MqFilterMessageReply
	err := s.CallMqServer("MqServerAPI.FilterMessage", filterMessageArgs, &filterMessageReply)
	if err != nil {
		s.resolverLogger.Error("[consume]MqServerAPI.FilterMessage error:%v", err)

		// metric
		metric.MetricLogModuleError(metric.MetricModuleError{
			Tag:       s.exporterTag,
			Timestamp: common.FromNow().String(),
			Module:    metric.ModuleTypeResolver,
		})
		return protocol.MqMessage{}, false, false
	}
	if !filterMessageReply.OK {
		s.resolverLogger.Error("[consume]MqServerAPI.FilterMessage not ok")
		return protocol.MqMessage{}, false, false
	}
	msg, hasNext := filterMessageReply.Message, filterMessageReply.HasNext

	// 支持 lazy shutdown
	if s.lazyShutdown && !hasNext {
		s.resolverLogger.Info("[consume][%s]the lazy-shutdown resolver has consume all message and will shutdown now", s.exporterTag)
		s.Shutdown(false)
		// delete exporter info
		exporterInfoKey := common.GenExporterInfoKey(s.exporterTag)
		delInfoArgs := &protocol.MetaDeleteExporterInfoArgs{Key: exporterInfoKey}
		var delInfoReply protocol.MetaDeleteExporterInfoReply
		err = s.CallMetaServer("MetaServerAPI.DeleteExporterInfo", delInfoArgs, &delInfoReply)
		if err != nil {
			s.resolverLogger.Error("[consume]call meta del exporter info error:%v", err)

			// metric
			metric.MetricLogModuleError(metric.MetricModuleError{
				Tag:       s.exporterTag,
				Timestamp: common.FromNow().String(),
				Module:    metric.ModuleTypeResolver,
			})
		}
		if !delInfoReply.OK {
			s.resolverLogger.Error("[consume]call meta del exporter info not ok")
		}
	}
	return msg, hasNext, true
}

func (s *ResolverServer) resolve(message protocol.MqMessage) {
	if s.hasShutdown {
		return
	}
	tag, data := message.Tag, message.Msg
	if tag != s.resolverTag {
		return
	}
	var dataPackage protocol.ReceiverDataPackage
	err := json.Unmarshal(data, &dataPackage)
	if err != nil {
		s.resolverLogger.Error("[resolve]err:%v", err)

		// metric
		metric.MetricLogModuleError(metric.MetricModuleError{
			Tag:       s.exporterTag,
			Timestamp: common.FromNow().String(),
			Module:    metric.ModuleTypeResolver,
		})
	}

	s.resolverHandler.OnReceive(dataPackage)
}

func (s *ResolverServer) CallMetaServer(serviceMethod string, args any, reply any) error {
	// 由于本身 rpc 连接是无状态的，因此这里不必加锁就行
	return common.ExecuteFunctionByRetry(func() error {
		var err error
		if s.metaClient == nil {
			s.metaClient, err = jsonrpc.Dial("tcp", s.conf.MetaServerAddr)
			if err != nil {
				return err
			}
		}
		err = s.metaClient.Call(serviceMethod, args, reply)
		if err != nil {
			s.metaClient = nil
		}
		return err
	})
}

func (s *ResolverServer) CallMqServer(serviceMethod string, args any, reply any) error {
	// 由于本身 rpc 连接是无状态的，因此这里不必加锁就行
	return common.ExecuteFunctionByRetry(func() error {
		var err error
		if s.mqClient == nil {
			s.mqClient, err = jsonrpc.Dial("tcp", s.conf.MqServerAddr)
			if err != nil {
				return err
			}
		}
		err = s.mqClient.Call(serviceMethod, args, reply)
		if err != nil {
			s.mqClient = nil
		}
		return err
	})
}
