package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

const (
	BUF_SIZE     = 100
	HISTORY_FILE = ".race.client.history"
)

var (
	Running bool   = true
	CmdUp   []byte = []byte{27, 91, 65}
	CmdDown []byte = []byte{27, 91, 64}
)

type InterruptStdin struct {
	data string
}

func (ist InterruptStdin) Read(buf []byte) (n int, err error) {
	buf = append(buf, []byte(ist.data)...)

	return len(ist.data), nil
}

func getHistoryFile() (*os.File, error) {
	var f *os.File
	dirname, err := os.UserHomeDir()
	if err != nil {
		return f, err
	}
	f, err = os.OpenFile(fmt.Sprintf("%s/%s", dirname, HISTORY_FILE), os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		f, err = os.Create(fmt.Sprintf("%s/%s", dirname, HISTORY_FILE))
		if err != nil {
			return f, err
		}
	}
	return f, nil
}

type History struct {
	file    *os.File
	history []string
	line    int
}

func (h *History) getHistory() string {
	cmd := h.history[h.line]
	if h.line > 1 {
		h.line -= 1
	} else {
		h.line = len(h.history) - 1
	}
	return cmd
}

func (h *History) addHistory(cmd string) error {
	if cmd != "" {
		_, err := h.file.WriteString(cmd + "\n")
		if err != nil {
			return err
		}
	}
	return nil
}

func (h *History) loadHistory() error {
	scanner := bufio.NewScanner(h.file)
	for scanner.Scan() {
		h.history = append(h.history, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	h.line = len(h.history) - 1
	return nil
}

func main() {
	hf, err := getHistoryFile()
	if err != nil {
		log.Fatal(err)
	}
	defer hf.Close()
	h := History{
		file:    hf,
		history: make([]string, 100),
		line:    0,
	}
	err = h.loadHistory()
	if err != nil {
		log.Fatal(err)
	}

	reader := bufio.NewReader(os.Stdin)
	stop := make(chan os.Signal, 1)
	finish := make(chan int, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sigs := <-stop
		fmt.Println("Exit signal: ", sigs)
		finish <- 0
		StopReader := InterruptStdin{data: "0\n"}
		reader.Reset(StopReader)
		fmt.Println(reader.Buffered())
		reader.UnreadByte()
		fmt.Println(reader.Buffered())
		//fmt.Println(reader.UnreadByte())
	}()

	fmt.Println("Command line interactive")
	fmt.Println("---------------------")

	for Running {
		select {
		case <-finish:
			fmt.Println("Exiting...")
			Running = false
			break
		default:
			fmt.Print("> ")
			buf, _ := reader.ReadBytes('\n')

			cmd := strings.Split(string(buf[:len(buf)-1]), " ")
			switch cmd[0] {
			case "hi":
				fmt.Println("hello, Yourself")
			case "exit", "q":
				Running = false
			case "worker":
				//h.addHistory(string(buf))
				connect(buf)
			case "wa":
				if len(cmd) > 1 {
					scmd := fmt.Sprintf("worker add %s", cmd[1])
					connect([]byte(scmd))
				} else {
					connect([]byte("worker add"))
				}
			case "wl":
				connect([]byte("worker list"))
			case "ws":
				if len(cmd) > 1 {
					scmd := fmt.Sprintf("worker stop %s", cmd[1])
					connect([]byte(scmd))
				} else {
					fmt.Println("Use ws command with worker ID")
				}
			case "wr":
				if len(cmd) > 1 {
					scmd := fmt.Sprintf("worker restart %s", cmd[1])
					connect([]byte(scmd))
				} else {
					fmt.Println("Use wr command with worker ID")
				}
			default:
				fmt.Println(cmd)
			}
		}
	}
}

func connect(msg []byte) {
	server := "/tmp/echo.sock"
	con, err := net.Dial("unix", server)
	checkErr(err)
	defer con.Close()

	_, err = con.Write(msg)
	checkErr(err)

	reply := make([]byte, 1024)
	_, err = con.Read(reply)
	checkErr(err)
	fmt.Println(string(reply))
}

func checkErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
