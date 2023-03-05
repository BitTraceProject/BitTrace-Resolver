package main

import (
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"

	"github.com/BitTraceProject/BitTrace-Resolver/server"
	"github.com/BitTraceProject/BitTrace-Types/pkg/common"
	"github.com/BitTraceProject/BitTrace-Types/pkg/config"
)

func main() {
	envModule, err := common.LookupEnv("MODULE")
	if err != nil {
		panic(err)
	}

	// 开启pprof
	go func() {
		err := http.ListenAndServe(":9093", nil)
		if err != nil {
			panic(err)
		}
	}()

	selectModule(envModule)
}

func selectModule(module string) {
	switch module {
	case "mgr":
		runMgr()
	default:
		panic(fmt.Sprintf("error: unknown module:%s", module))
	}
}

func runMgr() {
	conf := config.ResolverMgrConfig{
		MetaServerAddr: "meta.receiver.bittrace.proj:8082",
		ResolverConfig: config.ResolverConfig{
			MqServerAddr:   "mq.receiver.bittrace.proj:8081",
			MetaServerAddr: "meta.receiver.bittrace.proj:8082",
			DatabaseConfig: config.DatabaseConfig{
				Address:  "master.collector.bittrace.proj:33061",
				Username: "admin",
				Password: "admin",
			},
		},
	}
	s := server.NewResolverMgrServer(":8083", conf)

	log.Println("running mgr")
	s.Run()
}
