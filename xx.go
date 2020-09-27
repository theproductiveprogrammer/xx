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
	"strconv"
	"strings"
	"time"
)

/*    understand/
 * program version
 */
const VERSION = "0.1.0"

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
	num uint32
	Src string `json:"src"`
	Exe string `json:"exe"`
	log string `json:"log"`
	sec int    `json:"sec"`
}

type StatusMsg struct {
	When string `json:"when"`
	Ref  uint32 `json:"ref"`
	Exit int    `json:"exit"`
	out  string `json:"out"`
	err  string `json:"err"`
}

/*    way/
 * gets kaf messages and processes them
 */
func run(kaddr string) error {
	var pending []StartMsg

	c := make(chan StatusMsg)
	e := make(chan error)

	go func() {
		for {
			log.Println(<-e)
		}
	}()

	go putKafMsgs(kaddr, c, e)

	processor := func(num uint32, msg []byte, err error) {
		pending, err = processMsgs(num, msg, err, pending)
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
 * Update the pending list with our message
 */
func processMsgs(num uint32, msg []byte, err error, pending []StartMsg) ([]StartMsg, error) {
	if err != nil {
		return nil, err
	}

	mErr := func(err error) ([]StartMsg, error) {
		m := fmt.Sprintf("failed msg: %d (%s %s)",
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

func handle(setStatus chan StatusMsg, pending []StartMsg) {
	fmt.Println(pending)
	for i := 0; i < len(pending); i++ {
		curr := pending[i]
		status := StatusMsg{
			Ref: curr.num,
		}
		setStatus <- status
	}
}

func putKafMsgs(kaddr string, c chan StatusMsg, e chan error) {
	if kaddr[len(kaddr)-1] != '/' {
		kaddr = kaddr + "/"
	}
	if !strings.HasPrefix(kaddr, "http") {
		kaddr = "http://" + kaddr
	}
	kaddr = kaddr + "put/xx"

	for {
		status := <-c
		data, err := json.Marshal(status)
		if err != nil {
			e <- err
			return
		}
		log.Println("Sending message:", string(data))
		_, err = http.Post(kaddr,
			"application/json",
			bytes.NewReader(data))
		if err != nil {
			e <- err
		}
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

/*    way/
 * Send the error message and status code
 */
func handleErrors(status int, in io.Reader, h Handler) (uint64, uint32) {
	var msg strings.Builder
	msg.WriteString(strconv.FormatUint(uint64(status), 10))
	e := make([]byte, 256)
	tot := 0
	for {
		if tot >= len(e) {
			break
		}
		n, err := in.Read(e[tot:])
		if n > 0 {
			tot += n
		}
		if err != nil {
			break
		}
	}
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
