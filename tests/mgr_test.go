package tests

import (
	"net/rpc/jsonrpc"
	"strconv"
	"testing"

	"github.com/BitTraceProject/BitTrace-Resolver/server"
	"github.com/BitTraceProject/BitTrace-Types/pkg/config"
	"github.com/BitTraceProject/BitTrace-Types/pkg/protocol"
)

func TestResolverMgrServer(t *testing.T) {
	conf := config.ResolverMgrConfig{
		MetaServerAddr: "127.0.0.1:8081",
		ResolverConfig: config.ResolverConfig{
			MetaServerAddr: "127.0.0.1:8081",
			MqServerAddr:   "127.0.0.1:8082",
			DatabaseConfig: config.DatabaseConfig{
				Address:  "localhost:33061",
				Username: "admin",
				Password: "admin",
			},
		},
	}
	s := server.NewResolverMgrServer("127.0.0.1:8083", conf)
	err := s.Run()
	if err != nil {
		t.Fatal(err)
	}
}

func TestResolverMgrClientResolverStart(t *testing.T) {
	c, err := jsonrpc.Dial("tcp", "127.0.0.1:8083")
	if err != nil {
		t.Fatal("dialing:", err)
	}
	defer c.Close()

	for i := 0; i < 3; i++ {
		args := &protocol.ResolverStartArgs{ExporterTag: "exporter-" + strconv.Itoa(i)}
		var reply protocol.ResolverStartReply
		err = c.Call("ResolverMgrServerAPI.Start", args, &reply)
		if err != nil {
			t.Fatal("ResolverMgrServerAPI error:", err)
		}
		t.Log(reply.ResolverTag)
	}
}

func TestResolverMgrClientResolverShutdown(t *testing.T) {
	c, err := jsonrpc.Dial("tcp", "127.0.0.1:8083")
	if err != nil {
		t.Fatal("dialing:", err)
	}
	defer c.Close()

	for i := 0; i < 3; i++ {
		args := &protocol.ResolverShutdownArgs{ExporterTag: "exporter-" + strconv.Itoa(i), LazyShutdown: true}
		var reply protocol.ResolverShutdownReply
		err = c.Call("ResolverMgrServerAPI.Shutdown", args, &reply)
		if err != nil {
			t.Fatal("ResolverMgrServerAPI error:", err)
		}
		t.Log(reply.OK)
	}
}
