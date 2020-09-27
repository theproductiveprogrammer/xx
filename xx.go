package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

/*    understand/
 * program version
 */
const VERSION = "1.0.0"

func main() {
	if len(os.Args) != 2 {
		showHelp()
		return
	}
	err := run(os.Args[1])
	if err != nil {
		log.Println(err)
	}
}

func showHelp() {
	fmt.Println("xx: eXecutes")
	fmt.Println(`usage:
  go run xx.go <kaf addr>
eg: go run xx.go 127.0.0.1:7749
version: ` + VERSION)
}

/*
 * key data types
 */
type StartMsg struct {
	num  uint32
	Src  string   `json:"src"`
	Exe  string   `json:"exe"`
	Dir  string   `json:"dir"`
	Args []string `json:"args"`
	Secs int      `json:"secs"`
}

type StatusMsg struct {
	When string `json:"when"`
	Ref  uint32 `json:"ref"`
	Pid  int    `json:"pid"`
	Exit int    `json:"exit"`
	Op   string `json:"op"`
}

type sendStatus struct {
	msg *StatusMsg
	res chan error
}

/*    way/
 * start the go routine to put status updates, handle
 * processing messages, and get kaf messages to process.
 */
func run(kaddr string) error {
	var pending []StartMsg

	c := make(chan sendStatus)
	go putKafMsgs(kaddr, c)

	processor := func(num uint32, msg []byte, err error) {
		pending, err = processMsg(num, msg, err, pending)
		if err != nil {
			log.Println(err)
		}
	}

	scheduler := func(err error, end bool) time.Duration {
		if end && len(pending) > 0 {
			handle(c, pending)
			pending = []StartMsg{}
		}
		return schedule(err, end)
	}

	return getKafMsgs(kaddr, processor, scheduler)
}

/*    way/
 * Show any errors and request to get the next messages
 * - quickly if there could be more, slower if we seem
 * to have reached the end.
 */
func schedule(err error, end bool) time.Duration {
	if err != nil {
		log.Println(err)
	}

	if end {
		return 7 * time.Second
	} else {
		return 200 * time.Millisecond
	}
}

/*    way/
 * Update the pending list with the message
 */
func processMsg(num uint32, msg []byte, err error, pending []StartMsg) ([]StartMsg, error) {
	if err != nil {
		return nil, err
	}

	mErr := func(err error) ([]StartMsg, error) {
		m := fmt.Sprintf(`failed msg: %d ("%s" %s)`,
			num, string(msg), err.Error())
		return nil, errors.New(m)
	}

	if isStartReq(msg) {

		var start StartMsg
		err := json.Unmarshal(msg, &start)
		if err != nil {
			return mErr(err)
		} else {
			start.num = num
			pending = append(pending, start)
			return pending, nil
		}

	}

	if isStatusReq(msg) {

		var status StatusMsg
		err := json.Unmarshal(msg, &status)
		if err != nil {
			return mErr(err)
		} else {
			for i := 0; i < len(pending); i++ {
				curr := pending[i]
				if curr.num == status.Ref {
					pending[i] = pending[len(pending)-1]
					pending = pending[:len(pending)-1]
				}
			}
			return pending, nil
		}
	}

	return mErr(errors.New("Did not understand message type"))
}

/*    way/
 * Guess that this is a start request when it contains
 * an "exe" field
 */
func isStartReq(msg []byte) bool {
	return bytes.Contains(msg, []byte(`"exe":`))
}

/*    way/
 * Guess that this is a start request when it contains
 * a "ref" field
 */
func isStatusReq(msg []byte) bool {
	return bytes.Contains(msg, []byte(`"ref":`))
}

/*    way/
 * start a goroutine for each pending request
 */
func handle(setStatus chan sendStatus, pending []StartMsg) {
	for i := 0; i < len(pending); i++ {
		go start(pending[i], setStatus)
	}
}

type Op struct {
	mux  sync.Mutex
	used int
	buf  []byte
}

/*    way/
 * write into a the Output buffer, rotating it around
 * as needed to keep the last few entries
 */
func (o *Op) Write(p []byte) (int, error) {
	o.mux.Lock()
	defer o.mux.Unlock()

	n := len(p)

	if n >= len(o.buf) {
		copy(o.buf, p[(n-len(o.buf)):])
		o.used = len(o.buf)
		return len(p), nil
	}

	if n+o.used <= len(o.buf) {
		copy(o.buf[o.used:], p)
		o.used += n
		return len(p), nil
	}

	shift := n + o.used - len(o.buf)
	copy(o.buf, o.buf[shift:])
	o.used -= shift
	copy(o.buf[o.used:], p)
	o.used += n

	return len(p), nil
}

/*    way/
 * empty the current buffer into a string
 */
func (o *Op) String() string {
	o.mux.Lock()
	defer o.mux.Unlock()
	r := string(o.buf[:o.used])
	o.used = 0
	return r
}

/*    way/
 * Start the given process and capture it's output,
 * sending the status ever 'sec' seconds and on exit
 */
func start(start StartMsg, setStatus chan sendStatus) {
	log.Println(fmt.Sprintf(`starting: "%s" [%d]`, start.Exe, start.num))

	op := Op{buf: make([]byte, 900)}

	cmd := exec.Command(start.Exe, start.Args...)
	cmd.Stdout = &op
	cmd.Stderr = &op
	if len(start.Dir) > 0 {
		cmd.Dir = start.Dir
	}

	var xit chan bool
	if start.Secs > 0 {
		xit = make(chan bool)
		go func() {
			s := time.Duration(start.Secs)
			ticker := time.NewTicker(s * time.Second)
			for {
				select {
				case <-xit:
					return
				case <-ticker.C:
					out := op.String()
					if len(out) > 0 {
						status := StatusMsg{
							When: time.Now().UTC().Format(time.RFC3339),
							Ref:  start.num,
							Pid:  cmd.Process.Pid,
							Op:   out,
						}
						res := make(chan error)
						setStatus <- sendStatus{&status, res}
						err := <-res
						if err != nil {
							log.Println(err)
						}
					}
				}
			}
		}()
	}

	err := cmd.Run()

	exit := cmd.ProcessState.ExitCode()
	if err != nil {
		op.Write([]byte(err.Error()))
		exit = -1
	}

	status := StatusMsg{
		When: time.Now().UTC().Format(time.RFC3339),
		Ref:  start.num,
		Exit: exit,
		Op:   op.String(),
	}

	if xit != nil {
		xit <- true
	}

	res := make(chan error)
	setStatus <- sendStatus{&status, res}
	err = <-res
	if err != nil {
		log.Println(err)
	} else {
		m := "done"
		if exit != 0 {
			m = fmt.Sprintf(`exited with code %d`, exit)
		}
		log.Println(fmt.Sprintf(`%s: "%s" [%d]`, m, start.Exe, start.num))
	}

}

/*    way/
 * post status requests to kaf to save them
 */
func putKafMsgs(kaddr string, c chan sendStatus) {
	if kaddr[len(kaddr)-1] != '/' {
		kaddr = kaddr + "/"
	}
	if !strings.HasPrefix(kaddr, "http") {
		kaddr = "http://" + kaddr
	}
	kaddr = kaddr + "put/xx"

	for {
		req := <-c
		data, err := json.Marshal(req.msg)
		if err != nil {
			req.res <- err
			continue
		}
		_, err = http.Post(kaddr,
			"application/json",
			bytes.NewReader(data))
		req.res <- err
	}

}

/*    way/
 * connect to the kaf address and request the latest
 * logs as per the scheduler, passing the data received
 * to the handler along with any errors.
 */
func getKafMsgs(kaddr string, h Handler, s Scheduler) error {
	if kaddr[len(kaddr)-1] != '/' {
		kaddr = kaddr + "/"
	}
	if !strings.HasPrefix(kaddr, "http") {
		kaddr = "http://" + kaddr
	}
	log.Println("Connecting to kaf: " + kaddr + "...")

	kaddr = kaddr + "get/xx?from="
	var latest, from uint32
	var url strings.Builder
	var end bool = false

	for {

		from = latest + 1

		url.Reset()
		url.WriteString(kaddr)
		url.WriteString(strconv.FormatUint(uint64(from), 10))

		resp, err := http.Get(url.String())

		if err == nil {
			num, last := process(resp, h)
			if last > latest {
				latest = last
			}
			end = num == 0
		}

		wait := s(err, end)
		if wait == 0 {
			return err
		}
		time.Sleep(wait)

	}
}

/*
 * Response headers
 */
const RespHeaderPfx = "KAF_MSGS|v1|"
const RecHeaderPfx = "KAF_MSG|"

/*    way/
 * Read the response header and process all the records
 */
func process(resp *http.Response, h Handler) (uint64, uint32) {
	in := resp.Body
	defer in.Close()

	if resp.StatusCode != 200 {
		return handleErrors(resp.StatusCode, in, h)
	}

	respHeader := []byte(RespHeaderPfx)
	hdr := make([]byte, len(respHeader))
	if _, err := io.ReadFull(in, hdr); err != nil {
		h(0, nil, err)
		return 0, 0
	}
	if bytes.Compare(respHeader, hdr) != 0 {
		h(0, nil, errors.New("invalid response header"))
		return 0, 0
	}
	num, err := readNum(in, '\n')
	if err != nil {
		h(0, nil, errors.New("failed to get number of records"))
		return 0, 0
	}

	var last uint32
	for i := 0; i < int(num); i++ {
		msgnum := processRec(in, h)
		if msgnum > last {
			last = msgnum
		}
	}
	return num, last
}

func readAtMost(in io.Reader, buf []byte) int {
	tot := 0
	for {
		if tot >= len(buf) {
			break
		}
		n, err := in.Read(buf[tot:])
		if n > 0 {
			tot += n
		}
		if err != nil {
			break
		}
	}
	return tot
}

/*    way/
 * Send the error message and status code
 */
func handleErrors(status int, in io.Reader, h Handler) (uint64, uint32) {
	var msg strings.Builder
	msg.WriteString(strconv.FormatUint(uint64(status), 10))
	e := make([]byte, 256)
	tot := readAtMost(in, e)
	if tot > 0 {
		msg.WriteByte(' ')
		msg.Write(e[:tot])
	}
	h(0, nil, errors.New(msg.String()))
	return 0, 0
}

/*    way/
 * Read the record header and send the record data to the
 * handler
 */
func processRec(in io.Reader, h Handler) uint32 {
	const TOOBIG = 1024
	recHeader := []byte(RecHeaderPfx)
	hdr := make([]byte, len(recHeader))
	if _, err := io.ReadFull(in, hdr); err != nil {
		h(0, nil, err)
		return 0
	}
	if bytes.Compare(recHeader, hdr) != 0 {
		h(0, nil, errors.New("invalid record header"))
		return 0
	}
	num, err := readNum(in, '|')
	if err != nil {
		h(0, nil, errors.New("invalid record number"))
		return 0
	}
	msgnum := uint32(num)
	sz, err := readNum(in, '\n')
	if err != nil || sz > TOOBIG {
		h(msgnum, nil, errors.New("invalid record size"))
	}
	data := make([]byte, sz+1) /* include terminating null */
	n, err := io.ReadAtLeast(in, data, int(sz))
	if err != nil && err != io.EOF {
		h(msgnum, nil, errors.New("failed reading record"))
		return 0
	}
	if n == int(sz) {
		_, err = in.Read(data[sz-1:])
		if err != nil {
			if err != io.EOF {
				h(msgnum, nil, errors.New("failed reading record end"))
				return 0
			} else {
				data[sz] = '\n'
			}
		}
	}
	if data[sz] != '\n' {
		h(msgnum, nil, errors.New("record not terminated correctly"))
		return 0
	}
	h(msgnum, data[:sz], nil)

	return msgnum
}

/*    way/
 * Read in bytes, one at a time till we hit the end
 * of the record and then return the matching number
 * found
 */
func readNum(in io.Reader, end byte) (uint64, error) {
	const BIGENOUGH = 32
	buf := make([]byte, BIGENOUGH)
	for i := 0; i < len(buf); i++ {
		p := buf[i : i+1]
		_, err := in.Read(p)
		if err != nil && err != io.EOF {
			return 0, errors.New("read failed")
		}
		if err == io.EOF {
			return strconv.ParseUint(string(buf[:i+1]), 10, 32)
		}
		if p[0] == end {
			return strconv.ParseUint(string(buf[:i]), 10, 32)
		}
	}
	return 0, errors.New("failed to header end")
}

type Handler func(num uint32, msg []byte, err error)
type Scheduler func(err error, end bool) time.Duration
