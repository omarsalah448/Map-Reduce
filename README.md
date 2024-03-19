# Map-Reduce
This project provides an implementation of:
- A sequential map-reduce library that can support any arbitrary map and reduce functions. (Task 1)
- A word-count map-reduce application. (Task 2)
- A distributed implementation of map-reduce that is tolerant to worker failures. (Task 3)

- To test you can use open a terminal and use "go test -run TestSequentialSingle mapreduce/..." as shown in the figure
  ![image](https://github.com/omarsalah448/Map-Reduce/assets/108231831/72c6cf06-370b-4b1e-9c19-edd611aad94d)

-similary you can use "go test -run TestSequentialMany mapreduce/..." for testing of task 1

- For task two you can run "go run word_count.go master sequential papers" to run the word count using map reduce,the output is found in a file called "mrtmp.wcnt_seq"

- Similary for task 3 you can run:
 go test -run TestBasic mapreduce/...
 go test -run TestOneFailure mapreduce/...
 go test -run TestManyFailures mapreduce/...

- For more detailed explaination please refer to the assignment.pdf
