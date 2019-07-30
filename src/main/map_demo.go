package main

import (
	"mapreduce"
	"fmt"
)

func main() {
	kvs := make([]mapreduce.KeyValue, 0)
	hello := mapreduce.KeyValue{"Hello", "1"}
	world := mapreduce.KeyValue{"World", "1"}
	kvs = append(kvs, hello, world)

	dict := make(map[string][]mapreduce.KeyValue)
	dict["1"] = kvs
	dict["1"] = append(dict["1"], mapreduce.KeyValue{"Go", "1"})
	dict["2"] = kvs

	for r := range dict {
		fmt.Println(dict[r])
	}
}
