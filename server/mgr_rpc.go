package server

import (
	"fmt"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"sync"

	"github.com/BitTraceProject/BitTrace-Resolver/common"
	"github.com/BitTraceProject/BitTrace-Types/pkg/config"
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

		resolverConf *config.ResolverConfig
		resolvers    map[string]*ResolverServer
	}
)

func NewResolverMgrServer(addr string, conf config.ResolverMgrConfig) *ResolverMgrServer {
	return &ResolverMgrServer{
		address: addr,
		conf:    conf,
	}
}

func NewResolveMgrServerAPI(conf *config.ResolverConfig) *ResolverMgrServerAPI {
	return &ResolverMgrServerAPI{
		resolverConf: conf,
		resolvers:    map[string]*ResolverServer{},
	}
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

func (api *ResolverMgrServerAPI) Start(args *protocol.ResolverStartArgs, reply *protocol.ResolverStartReply) error {
	// 根据 exporter tag 分配并启动 resolver，如果已存在则返回已存在的 resolver tag
	exporterTag := args.ExporterTag
	api.RLock()
	resolver, ok := api.resolvers[exporterTag]
	api.RUnlock()
	if !ok { // 不存在分配一个新的 resolver
		api.Lock()
		if _, ok := api.resolvers[exporterTag]; !ok {
			resolverTag := common.GenResolverTag(exporterTag)
			resolver = NewResolverServer(api.resolverConf, resolverTag, exporterTag)
			api.resolvers[exporterTag] = resolver
		}
		api.Unlock()
	}
	// 返回对应 resolver tag
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
		api.Lock()
		if _, ok := api.resolvers[exporterTag]; ok {
			resolver.Shutdown(args.LazyShutdown)
			delete(api.resolvers, exporterTag)
		}
		api.Unlock()
	}
	reply.OK = ok

	return nil
}
