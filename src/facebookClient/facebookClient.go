package main

import (
  "bufio"
  "dlog"
  "encoding/json"
  "flag"
  "fmt"
  "genericsmrproto"
  "io/ioutil"
  "log"
  "masterproto"
  "math/rand"
  "net"
  "net/rpc"
  "runtime"
  "state"
  "time"
)

var masterAddr *string = flag.String("maddr", "", "Master address. Defaults to localhost")
var masterPort *int = flag.Int("mport", 7087, "Master port.  Defaults to 7077.")
var reqsNb *int = flag.Int("q", 5000, "Total number of requests. Defaults to 5000.")
var writes *int = flag.Int("w", 100, "Percentage of updates (likes). Defaults to 100%.")
var enhanced *bool = flag.Bool("e", false, "Use enhanced conflict detection. Defaults to false.")
var fastReads *int = flag.Int("fast", 0, "Percentage of total reads that are fast. Defaults to 0%.")
var rounds *int = flag.Int("r", 1, "Split the total number of requests into this many rounds, and do rounds sequentially. Defaults to 1.")
var procs *int = flag.Int("p", 2, "GOMAXPROCS. Defaults to 2")
var check = flag.Bool("check", false, "Check that every expected reply was received exactly once.")
var hotKey *int = flag.Int("c", 0, "Percentage of requests using the same key. Defaults to 0%")
var s = flag.Float64("s", 2, "Zipfian s parameter")
var v = flag.Float64("v", 1, "Zipfian v parameter")

var N int
var FOLDER = "vanilla/"

var successful []int
var rarray []int
var rsp []bool
var startTimes []time.Time
var latencies []time.Duration
var OKrsp []bool

type Statistics struct {
  ReqsNb int
  Writes int
  Rounds int
  Conflicts int
  LatenciesNano []int64
  LikeLatencies []int64
  ReadLatencies []int64
  FastReadLatencies []int64
}

func main() {
  flag.Parse()
  runtime.GOMAXPROCS(*procs)
  randObj := rand.New(rand.NewSource(42))
  zipf := rand.NewZipf(randObj, *s, *v, uint64(*reqsNb / *rounds))
  if *hotKey > 100 {
    log.Fatalf("Conflicts percentage must be between 0 and 100.\n")
  }

  master, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", *masterAddr, *masterPort))
  if err != nil {
    log.Fatalf("Error connecting to master\n")
  }

  rlReply := new(masterproto.GetReplicaListReply)
  err = master.Call("Master.GetReplicaList", new(masterproto.GetReplicaListArgs), rlReply)
  if err != nil {
    log.Fatalf("Error making the GetReplicaList RPC")
  }

  N = len(rlReply.ReplicaList)
  servers := make([]net.Conn, N)
  readers := make([]*bufio.Reader, N)
  writers := make([]*bufio.Writer, N)
  // This array contains the ids of the replicas to which 
  // the requests in 1 round will be sent
  rarray = make([]int, *reqsNb / *rounds)
  // This array contains which key the request will touch
  // The key is 42 for a conflict
  karray := make([]int64, *reqsNb / *rounds)
  // Operation array indicating what each request operation was
  ops := make([]state.Operation, *reqsNb / *rounds)
  // Number of requests sent to the corresponding replicas
  perReplicaCount := make([]int, N)
  test := make([]int, *reqsNb / *rounds)
  for i := 0; i < len(rarray); i++ {
    r := rand.Intn(N)
    rarray[i] = r
    // Increase the replica's request count, excluding the eps requests
    if i < *reqsNb / *rounds {
      perReplicaCount[r]++
    }

    if *hotKey >= 0 {
      r = rand.Intn(100)
      if r < *hotKey {
        karray[i] = 42
      } else {
        karray[i] = int64(43 + i)
      }
    } else {
      karray[i] = int64(zipf.Uint64())
      test[karray[i]]++
    }
  }

  if *hotKey >= 0 {
    fmt.Println("Uniform distribution:")
  } else {
    fmt.Println("Zipfian distribution:")
  }

  for i := 0; i < N; i++ {
    var err error
    servers[i], err = net.Dial("tcp", rlReply.ReplicaList[i])
    if err != nil {
      log.Printf("Error connecting to replica %d\n", i)
    }

    readers[i] = bufio.NewReader(servers[i])
    writers[i] = bufio.NewWriter(servers[i])
  }

  successful = make([]int, N)
  var id int32 = 0
  done := make(chan bool, N)
  args := genericsmrproto.Propose{id, state.Command{state.INCREMENT, 0, 0}, 0}
  before_total := time.Now()
  startTimes = make([]time.Time, *reqsNb)
  latencies = make([]time.Duration, *reqsNb)
  OKrsp = make([]bool, *reqsNb)
  for j := 0; j < *rounds; j++ {
    n := *reqsNb / *rounds
    if *check {
      rsp = make([]bool, n)
      for j := 0; j < n; j++ {
        rsp[j] = false
      }
    }

    for i := 0; i < N; i++ {
      go waitReplies(readers, i, perReplicaCount[i], done)
    }

    before := time.Now()
    for i := 0; i < n; i++ {
      dlog.Printf("Sending proposal %d\n", id)
      args.CommandId = id
      r := rand.Intn(100)
      if r < *writes {
        args.Command.Op = state.LIKE
      } else {
        r = rand.Intn(100)
        if r < *fastReads {
          args.Command.Op = state.FAST_READ
        } else {
          args.Command.Op = state.READ
        }
      }

      ops[i] = args.Command.Op
      args.Command.K = state.Key(karray[i])
      args.Command.V = state.Value(i)
      leader := rarray[i]
      writers[leader].WriteByte(genericsmrproto.PROPOSE)
      args.Marshal(writers[leader])
      startTimes[id] = time.Now()
      id++
      if i % 100 == 0 {
        for i := 0; i < N; i++ {
          writers[i].Flush()
        }
      }
    }

    for i := 0; i < N; i++ {
      writers[i].Flush()
    }

    err := false
    for i := 0; i < N; i++ {
      e := <-done
      err = e || err
    }

    after := time.Now()
    fmt.Printf("Round took %v\n", after.Sub(before))
    if *check {
      for j := 0; j < n; j++ {
        if !rsp[j] {
          fmt.Println("Didn't receive", j)
        }
      }
    }

    if err {
      N = N - 1
    }
  }

  after_total := time.Now()
  fmt.Printf("Test took %v\n", after_total.Sub(before_total))
  avg := 0.0
  latency_nanos := make([]int64, *reqsNb)
  var readLatencies []int64
  var fastReadLatencies []int64
  var likeLatencies []int64
  for i, latency := range latencies {
    avg += latency.Seconds()
    latency_nanos[i] = latency.Nanoseconds()
    switch ops[i] {
      case state.READ:
        readLatencies = append(readLatencies, latency_nanos[i])
        break
      case state.FAST_READ:
        fastReadLatencies = append(fastReadLatencies, latency_nanos[i])
        break
      case state.LIKE:
        likeLatencies = append(likeLatencies, latency_nanos[i])
        break
    }
  }

  fmt.Println()
  avg = avg * 1000.0 / float64(len(latencies))
  fmt.Printf("Average latency %fms\n", avg)
  stats := Statistics{
    ReqsNb: *reqsNb,
    Writes: *writes,
    Rounds: *rounds,
    Conflicts: *hotKey,
    LatenciesNano: latency_nanos,
    LikeLatencies: likeLatencies,
    ReadLatencies: readLatencies,
    FastReadLatencies: fastReadLatencies,
  }

  statsBytes, err := json.Marshal(stats)
  if err != nil {
    fmt.Println(err)
    return
  }

  var filename string
  if N == 3 {
    filename = fmt.Sprintf("%sthree_reps/stats_%dreq_%d%%writes_%d%%fastreads_%d%%conflicts", FOLDER, *reqsNb, *writes, *fastReads, *hotKey)
  } else {
    filename = fmt.Sprintf("%sfive_reps/stats_%dreq_%d%%writes_%d%%fastreads_%d%%conflicts", FOLDER, *reqsNb, *writes, *fastReads, *hotKey)
  }

  ioutil.WriteFile(filename, statsBytes, 0644)
  s := 0
  for _, succ := range successful {
    s += succ
  }

  fmt.Printf("Successful: %d\n", s)
  for _, client := range servers {
    if client != nil {
      client.Close()
    }
  }

  master.Close()
}

func waitReplies(readers []*bufio.Reader, leader int, n int, done chan bool) {
  e := false
  reply := new(genericsmrproto.ProposeReplyTS)
  for i := 0; i < n; i++ {
    if err := reply.Unmarshal(readers[leader]); err != nil {
      fmt.Println("Error when reading:", err)
      e = true
      continue
    }

    if *check {
      if rsp[reply.CommandId] {
        fmt.Println("Duplicate reply", reply.CommandId)
      }

      rsp[reply.CommandId] = true
    }

    if reply.OK != 0 {
      if !OKrsp[reply.CommandId] {
        OKrsp[reply.CommandId] = true
        latencies[reply.CommandId] = time.Now().Sub(startTimes[reply.CommandId])
      }

      successful[leader]++
    }
  }

  done <- e
}
