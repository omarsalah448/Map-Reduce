package mapreduce

import (
	"encoding/json"
	"hash/fnv"
	"os"
	"sort"
)

func runMapTask(
	jobName string, // The name of the whole mapreduce job
	mapTaskIndex int, // The index of the map task
	inputFile string, // The path to the input file assigned to this task
	nReduce int, // The number of reduce tasks that will be run
	mapFn func(file string, contents string) []KeyValue, // The user-defined map function
) {
	// ToDo: Write this function. See description in assignment.
	data, err := os.ReadFile(inputFile)
	checkError(err)
	output := mapFn(inputFile, string(data))
	// store the file names in an array of strings
	var filenames []string
	for i := 0; i < nReduce; i++ {
		// get the intermediate filenames
		filename := getIntermediateName(jobName, mapTaskIndex, i)
		filenames = append(filenames, filename)
		file, err := os.Create(filename)
		checkError(err)
		defer file.Close()
	}
	// add the (key, value) pairs to the files
	for _, kv := range output {
		filename := filenames[hash32(kv.Key)%uint32(nReduce)]
		file, err := os.OpenFile(filename, os.O_WRONLY|os.O_APPEND, 0644)
		checkError(err)
		encoder := json.NewEncoder(file)
		err = encoder.Encode(&kv)
		checkError(err)
		defer file.Close()
	}
}

func hash32(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func runReduceTask(
	jobName string, // the name of the whole MapReduce job
	reduceTaskIndex int, // the index of the reduce task
	nMap int, // the number of map tasks that were run
	reduceFn func(key string, values []string) string,
) {
	// ToDo: Write this function. See description in assignment.
	// store the file names in an array of strings
	var filenames []string
	for i := 0; i < nMap; i++ {
		// get the intermediate filenames
		filename := getIntermediateName(jobName, i, reduceTaskIndex)
		filenames = append(filenames, filename)
	}
	// create a dictionary for grouping
	dict := make(map[string][]string)
	for _, filename := range filenames {
		file, err := os.Open(filename)
		checkError(err)
		decoder := json.NewDecoder(file)
		var kv KeyValue
		for {
			err = decoder.Decode(&kv)
			if err != nil {
				break
			}
			dict[kv.Key] = append(dict[kv.Key], kv.Value)
		}
		defer file.Close()
	}
	outputFileName := getReduceOutName(jobName, reduceTaskIndex)
	// sort by key
	var keys []string
	for k := range dict {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	file, err := os.Create(outputFileName)
	checkError(err)
	defer file.Close()
	// apply the reduce stage
	for _, key := range keys {
		value := reduceFn(key, dict[key])
		file, err := os.OpenFile(outputFileName, os.O_WRONLY|os.O_APPEND, 0644)
		checkError(err)
		encoder := json.NewEncoder(file)
		kv := KeyValue{key, value}
		err = encoder.Encode(&kv)
		checkError(err)
		defer file.Close()
	}
}
