package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	var (
		rsp *GetTaskRsp
		err error
	)

	for {
		// 1.Get task from coordinator,
		rsp, err = getTaskOrBlocked()
		if err != nil {
			fmt.Printf("Worker getTask fail")
			return
		}
		// fmt.Printf("getTaskOrBlocked rsp=%v\n", *rsp)
		// 2.execute computation by task type
		if rsp.TaskType == MAP_TASK {
			/** Map Task **/
			// 1.read input from file
			file, err := os.Open(rsp.InputFileName)
			if err != nil {
				log.Fatalf("MAP_TASK cannot open %v", rsp.InputFileName)
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", rsp.InputFileName)
			}
			file.Close()
			kvs := mapf(rsp.InputFileName, string(content))
			// 2.map kvs into their bucket file (shuffle)
			buckets := make([][]KeyValue, rsp.NReduce)
			for _, kv := range kvs {
				idx := ihash(kv.Key) % rsp.NReduce
				buckets[idx] = append(buckets[idx], kv)
			}
			// 3.write outputs into intermediate file
			for i := 0; i < rsp.NReduce; i++ {
				// create temporary output file
				f, err := os.CreateTemp("", "maptemp")
				if err != nil {
					log.Fatal(err)
				}
				// write down to file in JSON format
				enc := json.NewEncoder(f)
				if err = enc.Encode(buckets[i]); err != nil {
					log.Fatal(err)
				}
				// rename temporary file after completing written
				if err = os.Rename(f.Name(), fmt.Sprintf(INTERMEDIATE_FILE_FORMAT, rsp.TaskSeqNum, i)); err != nil {
					log.Fatal(err)
				}
				f.Close()
			}

		} else if rsp.TaskType == RDC_TASK {
			/** Reduce Task **/
			// 1.read input key-values from intermediate files
			input := []KeyValue{}
			for i := 0; i < rsp.NMap; i++ {
				filename := fmt.Sprintf(INTERMEDIATE_FILE_FORMAT, i, rsp.TaskSeqNum)
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("RDC_TASK cannot open %v", filename)
				}
				dec := json.NewDecoder(file)
				bucket := []KeyValue{}
				if err = dec.Decode(&bucket); err != nil {
					log.Fatal(err)
				}
				file.Close()
				input = append(input, bucket...)
			}
			// 2.sort input k/vs
			sort.Slice(input, func(i, j int) bool {
				return input[i].Key < input[j].Key
			})
			// 3.batch-process one by one and write down temporary file
			outputfile, err := os.CreateTemp("", "outputfile") // create temporary file
			if err != nil {
				log.Fatal(err)
			}
			idx := 0
			for idx < len(input) {
				key := input[idx].Key
				values := []string{}
				// merge equal k/v into one k/vs
				for ; idx < len(input) && key == input[idx].Key; idx++ {
					values = append(values, input[idx].Value)
				}
				output := reducef(key, values)
				fmt.Fprintf(outputfile, "%v %v\n", key, output)
			}
			// 4.rename output file and close it after completing correctly
			if err = os.Rename(outputfile.Name(), fmt.Sprintf(OUTPUT_FILE_FORMAT, rsp.TaskSeqNum)); err != nil {
				log.Fatal(err)
			}
			outputfile.Close()
		} else {
			fmt.Printf("Worker getTask unknown task type(%s)", rsp.TaskType)
			return
		}

		// notify coordinator that task is completed
		if err = handinTask(rsp.TaskType, rsp.TaskSeqNum); err != nil {
			fmt.Printf("Worker handinTask fail: %v", err)
			return
		}

		// check if all tasks in job is completed, if so, exits

	}
}

// get task from coordinator.
// if no assignable task, blocked in this function
// if getTask() fail, exit
func getTaskOrBlocked() (*GetTaskRsp, error) {
	for {
		rsp, err := getTask()
		if err != nil {
			// fmt.Println("Worker exit")
			os.Exit(0) // assume that master-exit express job is finished, so worker exit successfully
		}
		if rsp != nil && rsp.TaskSeqNum >= 0 {
			return rsp, err
		}
		// sleep a while
		time.Sleep(500 * time.Millisecond)
	}
}

func getTask() (*GetTaskRsp, error) {
	req := GetTaskReq{}
	rsp := GetTaskRsp{}
	if ok := call("Coordinator.GetTask", &req, &rsp); ok {
		return &rsp, nil
	} else {
		return nil, fmt.Errorf("GetTask error")
	}
}

func handinTask(taskType TaskTypeEnum, taskSeqNum int) error {
	req := HandinTaskReq{
		TaskType:   taskType,
		TaskSeqNum: taskSeqNum,
	}
	rsp := HandinTaskRsp{}
	if ok := call("Coordinator.HandinTask", &req, &rsp); ok {
		return nil
	} else {
		return fmt.Errorf("HandinTask error")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
