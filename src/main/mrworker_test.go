package main

import (
	"6.5840/mr"
	"fmt"
	"log"
	"plugin"
	"testing"
)

func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []mr.KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}

func TestMap(t *testing.T) {
	mapf, _ := loadPlugin("wc.so")
	taskID := 0
	inputFiles := []string{"pg-being_ernest.txt"}
	nReduce := 3
	outputFiles := mr.MapTask(mapf, taskID, inputFiles, nReduce)
	for _, filename := range outputFiles {
		fmt.Println(filename)
	}
}
