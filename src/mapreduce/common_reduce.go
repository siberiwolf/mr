package mapreduce

import (
	"io/ioutil"
	"fmt"
	"encoding/json"
	"bytes"
	"sort"
	"os"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int,       // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).

	kvs := make([]KeyValue, 0)
	for i := 0; i < nMap; i++ {
		// read intermediate file
		filename := fmt.Sprintf("%s%d%d", jobName, i, reduceTask)
		fmt.Printf("Read intermediate file %s\n", filename)
		contents, err := ioutil.ReadFile(filename)
		if err != nil {
			fmt.Printf("Reduce task %d read file %s failed %s\n", reduceTask, filename, err)
		}
		temp := make([]KeyValue, 0)
		dec := json.NewDecoder(bytes.NewReader(contents))
		dec.Decode(&temp)
		fmt.Printf("Temp list is %s\n", temp)
		kvs = append(kvs, temp...)
	}

	fmt.Printf("Kvs is %s\n", kvs)

	// sort key value pairs
	sort.Slice(kvs, func(i, j int) bool {
		return kvs[i].Key < kvs[j].Key
	})
	fmt.Printf("Sorted kvs is %s\n", kvs)

	// call reduceF
	k := kvs[0].Key
	vs := make([]string, 0)
	var result string
	for i := 0; i < len(kvs); i++ {
		if k == kvs[i].Key {
			vs = append(vs, kvs[i].Value)
		} else {
			result = fmt.Sprintf("%s%s", result, reduceF(k, vs))
			fmt.Printf("Reduce k %s values %s result %s\n", k, vs, result)
			k = kvs[i].Key
			vs = make([]string, 0)
			vs = append(vs, kvs[i].Value)
		}
	}
	result = fmt.Sprintf("%s%s", result, reduceF(k, vs))
	fmt.Printf("Reduce k %s values %s result %s\n", k, vs, result)
	fmt.Printf("Reduce result is %s\n", result)

	// write output
	file, err := os.OpenFile(outFile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		fmt.Printf("Write mr output failed reduce task %d\n", reduceTask)
	}
	fmt.Printf("Write reduce out file %s", outFile)
	enc := json.NewEncoder(file)
	enc.Encode(&result)
	file.Close()
}
