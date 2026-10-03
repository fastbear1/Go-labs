package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type Worker struct {
	channel chan int
	value   float64
}

type WorkerMap struct {
	workers map[int]Worker
	mu      *sync.RWMutex
}

var vm WorkerMap

func echoServer(ctx context.Context, c net.Conn) {
	select {
	case <-ctx.Done():
		break
	default:
		buf := make([]byte, 512)

		terr := c.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		checkerr(terr)
		nr, err := c.Read(buf)
		if err != nil {
			return
		}
		data := buf[0:nr]
		go ParseAndRun(ctx, c, data)
	}
}

func ParseAndRun(ctx context.Context, c net.Conn, data []byte) {
	var num, id int
	cmd := strings.Split(string(data), " ")
	fmt.Println("CMD: ", cmd, len(cmd))

	defer func() {
		if r := recover(); r != nil {
			fmt.Println("=================== Error occured: ", r)
			c.Write([]byte("Server error, status 502"))
		}
	}()

	if rand.Intn(20) == 1 {
		panic("Critical failure")
	}

	switch cmd[0] {
	case "worker":
		switch cmd[1] {
		case "add":
			num = 1
			if len(cmd) > 2 {
				v, err := strconv.Atoi(cmd[2])
				checkerr(err)
				num = v
			}
			idList := []byte{}
			for range num {
				vm.mu.Lock()
				id = len(vm.workers) + 1
				fmt.Println("Get worker id: ", id)
				quit := make(chan int)
				vm.workers[id] = Worker{channel: quit, value: 0}
				go runworker(ctx, quit, id)
				idList = append(idList, []byte(fmt.Sprintf("Client created: %d\n", id))...)
				fmt.Println("worker started")
				fmt.Println("send response with worker id")
				vm.mu.Unlock()
			}
			_, err := c.Write(idList)
			checkerr(err)
		case "list":
			n := 1
			vm.mu.Lock()
			fmt.Println("How many workers are runnig at the moment", len(vm.workers))
			var conMsg string = "# id calc\n"
			for k, v := range vm.workers {
				conMsg += fmt.Sprintf("%d  %d  %v\n", n, k, v)
				n++
			}
			vm.mu.Unlock()
			_, err := c.Write([]byte(conMsg))
			checkerr(err)
		case "stop":
			vm.mu.Lock()
			did, err := strconv.Atoi(cmd[2])
			checkerr(err)
			stopMes := fmt.Sprintf("worker with id %d not found", did)
			if wr, ok := vm.workers[did]; ok {
				stopMes = fmt.Sprintf("worker #%d deleted", did)
				wr.channel <- 0
				close(wr.channel)
			}
			vm.mu.Unlock()
			_, err = c.Write([]byte(stopMes))
		case "restart":
			vm.mu.Lock()
			rid, err := strconv.Atoi(cmd[2])
			checkerr(err)
			resMes := fmt.Sprintf("worker with id %d not found", rid)
			if wr, ok := vm.workers[rid]; ok {
				resMes = fmt.Sprintf("worker #%d restarted", rid)
				wr.channel <- 1
			}
			vm.mu.Unlock()
			_, err = c.Write([]byte(resMes))
		}
	default:
		_, err := c.Write(data)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func runworker(ctx context.Context, channel chan int, id int) {
	var calc float64 = 0
	running := true
	for running {
		select {
		case <-ctx.Done():
			fmt.Printf("Worker #%d exiting...\n", id)
			vm.mu.Lock()
			delete(vm.workers, id)
			vm.mu.Unlock()
			running = false
			break
		case com := <-channel:
			if com == 0 {
				fmt.Printf("Worker #%d stopping...\n", id)
				vm.mu.Lock()
				delete(vm.workers, id)
				vm.mu.Unlock()
				running = false
				break
			}
			if com == 1 {
				calc = 0
			}
		default:
			calc += 0.0000001
			vm.mu.Lock()
			wrk := vm.workers[id]
			wrk.value = calc
			vm.workers[id] = wrk
			vm.mu.Unlock()
			time.Sleep(200 * time.Millisecond)
		}
	}
}

func checkerr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func getMapLen() int {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	return len(vm.workers)
}

func main() {
	l, err := net.Listen("unix", "/tmp/echo.sock")
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Error occured: ", r)
			l.Close()
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	vm = WorkerMap{workers: make(map[int]Worker), mu: new(sync.RWMutex)}
	if err != nil {
		log.Fatal("listen error:", err)
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-stop
		fmt.Println("Exit signal: ")
		l.Close()
		cancel()
	}()

	for {
		fd, err := l.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				log.Println("Exiting....")
				for getMapLen() != 0 {
					time.Sleep(100 * time.Millisecond)
				}
				break
			} else {
				log.Fatal("accept error:", err)
			}
		}
		go echoServer(ctx, fd)
	}
}
