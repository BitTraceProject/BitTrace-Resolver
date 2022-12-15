package server

import (
	"encoding/json"
	"fmt"
	"github.com/BitTraceProject/BitTrace-Types/pkg/protocol"
	"log"
	"net/rpc"
	"net/rpc/jsonrpc"
	"time"

	"github.com/BitTraceProject/BitTrace-Types/pkg/config"
	"github.com/BitTraceProject/BitTrace-Types/pkg/constants"
)

type ResolverServer struct {
	resolverTag string // resolver tag
	exporterTag string // 对应的 exporter tag，从 mq 消费消息
	conf        *config.ResolverConfig

	mqClient              *rpc.Client
	collectorWriterClient *rpc.Client

	hasShutdown  bool
	lazyShutdown bool

	stopCh chan bool
}

func NewResolverServer(conf *config.ResolverConfig, resolverTag, exporterTag string) *ResolverServer {
	s := &ResolverServer{
		resolverTag: resolverTag,
		exporterTag: exporterTag,
		conf:        conf,
		stopCh:      make(chan bool, 1),
	}
	err := s.initClient()
	if err != nil {
		panic(fmt.Errorf("[NewResolverServer]err:%v", err))
	}
	go s.Start()
	return s
}

func (s *ResolverServer) initClient() error {
	// TODO 一旦某个 client 发生异常可能导致瘫痪，所以要有足够的兜底方式，随时刷新 client
	// 后面确认一下
	mqClient, err := jsonrpc.Dial("tcp", s.conf.MqServerAddr)
	if err != nil {
		return err
	}
	s.mqClient = mqClient

	collectorWriterClient, err := jsonrpc.Dial("tcp", s.conf.CollectorWriterServerAddr)
	if err != nil {
		return err
	}
	s.collectorWriterClient = collectorWriterClient

	return nil
}

func (s *ResolverServer) Start() {
	// 定时轮询消费 mq，然后根据 mq 返回的 has next 选择立即消费 mq，还是继续定时轮询（每一次轮询结束重置定时，保证消费是串行的）
	timer := time.NewTicker(constants.MQ_CONSUME_INTERVAL) // 使用 timer 而非 ticker 保证消费是串行的
	hasNextChan := make(chan bool, 1)
	for {
		select {
		case <-timer.C:
			log.Println("C")
			// 轮询 mq
			msg, hasNext, ok := s.consume()
			log.Println(hasNext)
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
		err := s.mqClient.Call("MqServerAPI.ClearMessage", clearMessageArgs, &clearMessageReply)
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
	err := s.mqClient.Call("MqServerAPI.FilterMessage", filterMessageArgs, &filterMessageReply)
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

// resolve TODO 处理消息，存入 collector
func (s *ResolverServer) resolve(message protocol.MqMessage) {
	if s.hasShutdown {
		return
	}
	tag, data := message.Tag, message.Msg
	var dataPackage protocol.ReceiverDataPackage
	err := json.Unmarshal(data, &dataPackage)
	if err != nil {
		log.Printf("[resolve]err:%v", err)
	}
	log.Printf("resolve:%s,tag:%s,message:%+v", s.resolverTag, tag, dataPackage)
}
