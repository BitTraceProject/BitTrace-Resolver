package main

import (
	"github.com/BitTraceProject/BitTrace-Resolver/server"
	"github.com/BitTraceProject/BitTrace-Types/pkg/config"
)

func main() {
	conf := config.ResolverMgrConfig{
		MetaServerAddr: "127.0.0.1:8081", // 没用到
		ResolverConfig: &config.ResolverConfig{
			MqServerAddr:              "127.0.0.1:8082",
			CollectorWriterServerAddr: "127.0.0.1:8084",
		},
	}
	s := server.NewResolverMgrServer("127.0.0.1:8083", conf)
	s.Run()
}
