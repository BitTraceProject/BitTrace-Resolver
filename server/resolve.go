package server

import (
	"encoding/json"
	"log"
	"net/rpc"
	"net/rpc/jsonrpc"
	"time"

	resolver_common "github.com/BitTraceProject/BitTrace-Resolver/common"
	"github.com/BitTraceProject/BitTrace-Types/pkg/common"
	"github.com/BitTraceProject/BitTrace-Types/pkg/constants"
	"github.com/BitTraceProject/BitTrace-Types/pkg/protocol"
)

type (
	ResolverServer struct {
		resolverTag string // resolver tag
		exporterTag string // 对应的 exporter tag，从 mq 消费消息

		mqServerAddr string
		mqClient     *rpc.Client

		hasShutdown  bool
		lazyShutdown bool

		stopCh chan bool

		resolverHandler resolver_common.ResolverHandler
	}
)

func NewResolverServer(mqServerAddr string, resolverTag, exporterTag string, resolverHandler resolver_common.ResolverHandler) *ResolverServer {
	s := &ResolverServer{
		resolverTag:     resolverTag,
		exporterTag:     exporterTag,
		mqServerAddr:    mqServerAddr,
		stopCh:          make(chan bool, 1),
		resolverHandler: resolverHandler,
	}
	go s.Start()
	return s
}

func (s *ResolverServer) Start() {
	// 定时轮询消费 mq，然后根据 mq 返回的 has next 选择立即消费 mq，还是继续定时轮询（每一次轮询结束重置定时，保证消费是串行的）
	timer := time.NewTicker(constants.MQ_CONSUME_INTERVAL) // 使用 timer 而非 ticker 保证消费是串行的
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
				timer.Reset(constants.MQ_CONSUME_INTERVAL)
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
				timer.Reset(constants.MQ_CONSUME_INTERVAL)
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
			log.Printf("[Shutdown]MqServerAPI.ClearMessage error:%v", err)
		}
		s.stopCh <- true
		close(s.stopCh)
		s.hasShutdown = true
		return
	}
	// 否则设置全局标记位，使得下一次 hasNext 为 false 时直接退出 (consume 处)
	s.lazyShutdown = true
}

func (s *ResolverServer) consume() (protocol.MqMessage, bool, bool) {
	if s.hasShutdown {
		return protocol.MqMessage{}, false, false
	}
	filterMessageArgs := &protocol.MqFilterMessageArgs{Tag: s.resolverTag}
	var filterMessageReply protocol.MqFilterMessageReply
	err := s.CallMqServer("MqServerAPI.FilterMessage", filterMessageArgs, &filterMessageReply)
	if err != nil {
		log.Printf("[consume]MqServerAPI.FilterMessage error:%v", err)
		return protocol.MqMessage{}, false, false
	}
	if !filterMessageReply.OK {
		log.Println("[consume]MqServerAPI.FilterMessage not ok")
		return protocol.MqMessage{}, false, false
	}
	msg, hasNext := filterMessageReply.Message, filterMessageReply.HasNext

	// 支持 lazy shutdown
	if s.lazyShutdown && !hasNext {
		s.Shutdown(false)
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
		log.Printf("[resolve]err:%v", err)
	}

	//log.Printf("resolve:%s,tag:%s,message:%+v", s.resolverTag, tag, dataPackage)

	s.resolverHandler.OnReceive(dataPackage)
}

func (s *ResolverServer) CallMqServer(serviceMethod string, args any, reply any) error {
	// 由于本身 rpc 连接是无状态的，因此这里不必加锁就行
	return common.ExecuteFunctionByRetry(func() error {
		var err error
		if s.mqClient == nil {
			s.mqClient, err = jsonrpc.Dial("tcp", s.mqServerAddr)
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
