package main

import (
	"fmt"
	"mapreduce"
	"os"
	"strconv"
	"strings"
)

func mapFn(docName string, value string) []mapreduce.KeyValue {
	// TODO: write this function -- See description in assignment
	var output []mapreduce.KeyValue
	words := strings.Fields(value)
	for _, word := range words {
		if len(word) > 7 {
			output = append(output, mapreduce.KeyValue{Key: word, Value: "1"})
		}
	}
	return output
}

func reduceFn(key string, values []string) string {
	// TODO: write this function -- See description in assignment
	counter := 0
	for _, value := range values {
		num, _ := strconv.Atoi(value)
		counter += num
	}
	return strconv.Itoa(counter)
}

// Can be run in 3 ways:
// 1) Sequential (e.g., go run word_count.go master sequential papers)
// 2) Master (e.g., go run word_count.go master localhost_7777 papers &)
// 3) Worker (e.g., go run word_count.go master localhost_7777 papers &) // change 7778 when running other workers
func main() {
	if len(os.Args) < 4 {
		fmt.Printf("%s: see usage comments in file\n", os.Args[0])
	} else if os.Args[1] == "master" {
		var mr *mapreduce.Master
		if os.Args[2] == "sequential" {
			mr = mapreduce.Sequential("wcnt_seq", os.Args[3], 3, mapFn, reduceFn)
		} else {
			mr = mapreduce.Distributed("wcnt_dist", os.Args[3], 3, os.Args[2])
		}
		mr.Wait()
	} else if os.Args[1] == "worker" {
		mapreduce.RunWorker(os.Args[2], os.Args[3], mapFn, reduceFn, 100, true)
	} else {
		fmt.Printf("%s: see usage comments in file\n", os.Args[0])
	}
}
