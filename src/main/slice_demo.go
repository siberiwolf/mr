package main

import (
	"fmt"
	"mapreduce"
	"encoding/json"
	"os"
	"io/ioutil"
	"bytes"
)

func main() {
	kvs := make([]mapreduce.KeyValue, 0)
	hello := mapreduce.KeyValue{"Hello", "1"}
	world := mapreduce.KeyValue{"World", "1"}
	kvs = append(kvs, hello, world)
	//fmt.Println(kvs)

	file, err_open := os.OpenFile("aaa", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err_open != nil {
		fmt.Printf("Open file failed %s", err_open)
	}
	enc := json.NewEncoder(file)
	enc.Encode(&kvs)
	file.Close()

	contents, err_read := ioutil.ReadFile("aaa")
	if err_read != nil {
		fmt.Printf("Read file failed %s", err_read)
	}

	temp := make([]mapreduce.KeyValue, 0)
	dec := json.NewDecoder(bytes.NewReader(contents))
	dec.Decode(&temp)
	fmt.Println(temp)

}
