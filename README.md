# MapReduce

This project is an implementation of a distributed MapReduce system. The system consists of a coordinator process and worker processes. The coordinator hands out tasks to workers and manages task distribution and fault tolerance. Workers execute the Map and Reduce tasks, reading from and writing to files.

The implementation calculates word counts from .txt files.

## Overview of MapReduce

MapReduce is a programming model and processing technique developed by Google to simplify large-scale data processing on distributed systems. It abstracts the complexities of parallel computation, fault tolerance, data distribution, and load balancing. The model consists of two primary functions: `Map`, which processes input key-value pairs to generate intermediate key-value pairs, and `Reduce`, which merges all intermediate values associated with the same key to produce the final output. By dividing the task into these two stages, MapReduce enables efficient processing of vast amounts of data across large clusters of commodity hardware.

## Getting Started

### Prerequisites

- Go (version 1.15 or later)
- Git

### Cloning the Repository

```
git clone https://github.com/Jona-Han/MapReduce.git
cd src/main
```

### Running Sequential Word Count
```bash
$ go build -buildmode=plugin ../mrapps/wc.go
$ rm mr-out*
$ go run mrsequential.go wc.so pg*.txt
$ more mr-out-0
A 509
ABOUT 2
ACT 8
...
```
`mrsequential.go` leaves its output in the file `mr-out-0`. The input is from the text files named `pg-xxx.txt`.


### Running Distributed Word Count
```bash
$ go build -buildmode=plugin ../mrapps/wc.go
$ rm mr-out*
$ go run mrcoordinator.go pg-*.txt
```

The `pg-*.txt` arguments to `mrcoordinator.go` are the input files; each file corresponds to one "split", and is the input to one Map task.

In one or more other windows, run some workers:

```bash
$ go run mrworker.go wc.so
```

When the workers and coordinator have finished, look at the output in `mr-out-*`. When you've completed the lab, the sorted union of the output files should match the sequential output, like this:

```bash
$ cat mr-out-* | sort | more
A 509
ABOUT 2
ACT 8
...
```
