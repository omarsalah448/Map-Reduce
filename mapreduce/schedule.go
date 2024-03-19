package mapreduce

import (
	"sync"
)

// This function is a modification to the previous function, after I
// noticed that the previous function didn't always work
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var numOtherPhase int
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)     // number of map tasks
		numOtherPhase = mr.nReduce // number of reducers
	case reducePhase:
		ntasks = mr.nReduce           // number of reduce tasks
		numOtherPhase = len(mr.files) // number of map tasks
	}
	debug("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, numOtherPhase)
	// ToDo: Complete this function. See description in assignment.
	wg := sync.WaitGroup{}
	fAssignWorker := func(workerAddress string) {
		// worker is now free to work on another task
		mr.registerChannel <- workerAddress
	}
	// first we need to construct an array of job states
	var jobStates []jobState
	// at haven't completed any jobs yet
	for i := 0; i < ntasks; i++ {
		jobStates = append(jobStates, jobState{i, false})
	}
	wg.Add(len(jobStates))
	// looping over all the remaining jobs
	for i := 0; i < len(jobStates); i++ {
		// make it non blocking
		go func(jobStates []jobState, i int) {
			for {
				// construct the arguments for this specific job
				args := RunTaskArgs{
					JobName:       mr.jobName,
					File:          mr.files[i],
					Phase:         phase,
					TaskNumber:    i,
					NumOtherPhase: numOtherPhase,
				}
				// get a worker
				workerAddress := <-mr.registerChannel
				// assign the result of the call function to the state
				jobStates[i].finished = call(workerAddress, "Worker.RunTask", &args, &struct{}{})
				if jobStates[i].finished {
					// assign the worker
					go fAssignWorker(workerAddress)
					break
				}
			}
			wg.Done()
		}(jobStates, i)
	}
	wg.Wait()
	debug("Schedule: %v phase done\n", phase)
}

// func (mr *Master) schedule(phase jobPhase) {
// 	var ntasks int
// 	var numOtherPhase int
// 	switch phase {
// 	case mapPhase:
// 		ntasks = len(mr.files)     // number of map tasks
// 		numOtherPhase = mr.nReduce // number of reducers
// 	case reducePhase:
// 		ntasks = mr.nReduce           // number of reduce tasks
// 		numOtherPhase = len(mr.files) // number of map tasks
// 	}
// 	debug("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, numOtherPhase)
// 	// ToDo: Complete this function. See description in assignment.
// 	wg := sync.WaitGroup{}
// 	fAssignWorker := func(workerAddress string) {
// 		// worker is now free to work on another task
// 		mr.registerChannel <- workerAddress
// 	}
// 	// first we need to construct an array of job states
// 	var jobStates []jobState
// 	// at haven't completed any jobs yet
// 	for i := 0; i < ntasks; i++ {
// 		jobStates = append(jobStates, jobState{i, false})
// 	}
// 	// only get out when all jobs finished
// 	for len(jobStates) > 0 {
// 		wg.Add(len(jobStates))
// 		// looping over all the remaining jobs
// 		for _, state := range jobStates {
// 			// if a job is finished, remove it from the slice
// 			if state.finished {
// 				idx := findIndex(state, jobStates)
// 				if idx != -1 {
// 					jobStates = remove(jobStates, idx)
// 					wg.Done()
// 				}
// 			} else { // job is not finished, we need to assign it to a worker
// 				// make it non blocking
// 				go func(state jobState) {
// 					idx := findIndex(state, jobStates)
// 					if idx != -1 {
// 						// construct the arguments for this specific job
// 						args := RunTaskArgs{
// 							JobName:       mr.jobName,
// 							File:          mr.files[state.idx],
// 							Phase:         phase,
// 							TaskNumber:    state.idx,
// 							NumOtherPhase: numOtherPhase,
// 						}
// 						// get a worker
// 						workerAddress := <-mr.registerChannel
// 						// assign the result of the call function to the state
// 						jobStates[idx].finished = call(workerAddress, "Worker.RunTask", &args, &struct{}{})
// 						if jobStates[idx].finished {
// 							// assign the worker
// 							go fAssignWorker(workerAddress)
// 						}
// 					}
// 					wg.Done()
// 				}(state)
// 			}
// 		}
// 		// we don't want to enter the outer loop,
// 		// unless all elements of inner loop have been accessed
// 		wg.Wait()
// 	}
// 	debug("Schedule: %v phase done\n", phase)
// }

type jobState struct {
	idx      int
	finished bool
}

// // removes an element from slice
// func remove(s []jobState, i int) []jobState {
// 	if len(s) == 0 || i >= len(s) {
// 		return []jobState{}
// 	}
// 	s[i] = s[len(s)-1]
// 	return s[:len(s)-1]
// }

// // finds the index of an element in slice
// func findIndex(state jobState, jobStates []jobState) int {
// 	for i, _ := range jobStates {
// 		if state == jobStates[i] {
// 			return i
// 		}
// 	}
// 	return -1
// }
