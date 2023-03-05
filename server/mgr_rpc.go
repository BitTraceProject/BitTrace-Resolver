package server

import (
	"fmt"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"sync"

	"github.com/BitTraceProject/BitTrace-Types/pkg/common"
	"github.com/BitTraceProject/BitTrace-Types/pkg/config"
	"github.com/BitTraceProject/BitTrace-Types/pkg/logger"
	"github.com/BitTraceProject/BitTrace-Types/pkg/metric"
	"github.com/BitTraceProject/BitTrace-Types/pkg/protocol"
)

// resolver mgr rpc 服务: 根据 exporter tag 分配 resolver 并启动并返回 resolver tag

type (
	ResolverMgrServer struct {
		address string
		conf    config.ResolverMgrConfig
	}
	// ResolverMgrServerAPI 目前只提供同步的接口
	ResolverMgrServerAPI struct {
		sync.RWMutex

		resolverConf config.ResolverConfig
		resolvers    map[string]*ResolverServer

		metaClient *rpc.Client
	}
)

var (
	mgrLogger logger.Logger
)

func init() {
	mgrLogger = logger.GetLogger("bittrace_resolver_mgr")
}

func NewResolverMgrServer(addr string, conf config.ResolverMgrConfig) *ResolverMgrServer {
	return &ResolverMgrServer{
		address: addr,
		conf:    conf,
	}
}

func NewResolveMgrServerAPI(conf config.ResolverConfig) *ResolverMgrServerAPI {
	api := &ResolverMgrServerAPI{
		resolverConf: conf,
		resolvers:    map[string]*ResolverServer{},
	}
	api.initResolver()
	return api
}

func (s *ResolverMgrServer) Address() string {
	return s.address
}

func (s *ResolverMgrServer) Run() error {
	// 注册 rpc 服务
	api := NewResolveMgrServerAPI(s.conf.ResolverConfig)
	err := rpc.Register(api)
	if err != nil {
		return fmt.Errorf("[Run]fatal error:%v", err)
	}

	l, err := net.Listen("tcp", s.address)
	if err != nil {
		return fmt.Errorf("[Run]fatal error:%v", err)
	}

	for {
		conn, err := l.Accept() // 接收客户端连接请求
		if err != nil {
			continue
		}

		go func(conn net.Conn) { // 并发处理客户端请求
			jsonrpc.ServeConn(conn)
		}(conn)
	}
}

func (api *ResolverMgrServerAPI) initResolver() {
	getAllInfoArgs := &protocol.MetaGetAllExporterInfoArgs{}
	var getAllInfoReply protocol.MetaGetAllExporterInfoReply
	err := api.CallMetaServer("MetaServerAPI.GetAllExporterInfo", getAllInfoArgs, &getAllInfoReply)
	if err != nil {
		mgrLogger.Error("[initResolver]call meta get all exporter info error:%v", err)

		// metric
		metric.MetricLogModuleError(metric.MetricModuleError{
			Tag:       "[initResolver]call meta get all exporter info error",
			Timestamp: common.FromNow().String(),
			Module:    metric.ModuleTypeResolverMgr,
		})
	}
	if !getAllInfoReply.OK {
		mgrLogger.Error("[initResolver]call meta get all exporter info not ok")
	}
	mgrLogger.Info("[initResolver]there has %d resolver need to init", len(getAllInfoReply.ExporterInfo))
	for key, _ := range getAllInfoReply.ExporterInfo {
		exporterTag := common.ParseExporterTagFromExporterInfoKey(key)
		if _, ok := api.resolvers[exporterTag]; !ok {
			mgrLogger.Info("[%s]resolver do not exist, init", exporterTag)
			startArgs := &protocol.ResolverStartArgs{ExporterTag: exporterTag}
			var startReply protocol.ResolverStartReply
			err := api.Start(startArgs, &startReply)
			if err != nil {
				mgrLogger.Error("[initResolver]start resolver[%s] error:%v", exporterTag, err)

				// metric
				metric.MetricLogModuleError(metric.MetricModuleError{
					Tag:       exporterTag,
					Timestamp: common.FromNow().String(),
					Module:    metric.ModuleTypeResolverMgr,
				})
				continue
			}
		} else {
			mgrLogger.Info("[initResolver][%s]resolver has existed, pass", exporterTag)
		}
	}
}

func (api *ResolverMgrServerAPI) Start(args *protocol.ResolverStartArgs, reply *protocol.ResolverStartReply) error {
	// 根据 exporter tag 分配并启动 resolver，如果已存在则返回已存在的 resolver tag
	exporterTag := args.ExporterTag
	api.RLock()
	resolver, ok := api.resolvers[exporterTag]
	api.RUnlock()
	if !ok { // 不存在分配一个新的 resolver
		mgrLogger.Info("[Start][%s]resolver do not exist, start", exporterTag)
		api.Lock()
		if _, ok := api.resolvers[exporterTag]; !ok {
			resolverTag := common.GenResolverTag(exporterTag)
			resolverHandler := NewDefaultResolverHandler(exporterTag, api.resolverConf.MetaServerAddr, api.resolverConf.DatabaseConfig)
			resolver = NewResolverServer(api.resolverConf, resolverTag, exporterTag, resolverHandler)
			api.resolvers[exporterTag] = resolver
		}
		api.Unlock()
	} else {
		mgrLogger.Info("[Start][%s]resolver has existed, pass", exporterTag)
	}
	// 返回对应 resolver tag
	reply.OK = true
	reply.ResolverTag = resolver.resolverTag

	return nil
}

func (api *ResolverMgrServerAPI) Shutdown(args *protocol.ResolverShutdownArgs, reply *protocol.ResolverShutdownReply) error {
	// 根据 exporter tag 关停 resolver
	exporterTag := args.ExporterTag
	api.RLock()
	resolver, ok := api.resolvers[exporterTag]
	api.RUnlock()
	if ok { // 存在则关停
		mgrLogger.Info("[Shutdown][%s]resolver existed, will be shutdown", exporterTag)
		api.Lock()
		defer api.Unlock()
		if _, ok := api.resolvers[exporterTag]; ok {
			resolver.Shutdown(args.LazyShutdown)
			delete(api.resolvers, exporterTag)
		}
	} else {
		mgrLogger.Info("[Shutdown][%s]resolver do not exist, pass", exporterTag)
	}
	reply.OK = ok

	return nil
}

func (api *ResolverMgrServerAPI) CallMetaServer(serviceMethod string, args any, reply any) error {
	// 由于本身 rpc 连接是无状态的，因此这里不必加锁就行
	return common.ExecuteFunctionByRetry(func() error {
		var err error
		if api.metaClient == nil {
			api.metaClient, err = jsonrpc.Dial("tcp", api.resolverConf.MetaServerAddr)
			if err != nil {
				return err
			}
		}
		err = api.metaClient.Call(serviceMethod, args, reply)
		if err != nil {
			api.metaClient = nil
		}
		return err
	})
}
