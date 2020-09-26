package main

import (
  "io"
  "errors"
  "bytes"
  "strconv"
  "time"
  "strings"
  "fmt"
  "log"
  "net/http"
  "os"
)

/*    understand/
 * program version
 */
const VERSION = "0.1.0"

func main() {
  if(len(os.Args) != 2) {
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

/*    way/
 * runs forever, listening for start messages from kaf.
 */
func run(kaddr string) error {
  return getKafMsgs(kaddr,
  func (num uint32, msg []byte, err error) {
    if err != nil {
      log.Println(err)
      return
    }
    fmt.Println(num)
    fmt.Println(string(msg))
  },
  func (err error) time.Duration {
    if err != nil {
      log.Println(err)
    }
    return 7 * time.Second
  })
}

func getKafMsgs(kaddr string, h Handler, s Scheduler) error {
  if kaddr[len(kaddr)-1] != '/' {
    kaddr = kaddr + "/"
  }
  if !strings.HasPrefix(kaddr, "http") {
    kaddr = "http://" + kaddr
  }
  log.Println("Connecting to kaf: " + kaddr + "...")

  kaddr = kaddr + "get/xx?from="
  var msgnum uint64 = 1
  var url strings.Builder

  url.WriteString(kaddr)
  url.WriteString(strconv.FormatUint(msgnum, 10))
  resp, err := http.Get(url.String())
  if err != nil {
    log.Fatal("Failed to connect")
  }

  for {
    if err == nil {
      process(resp, h)
    }
    wait := s(err)
    if wait == 0 {
      return nil
    }
    time.Sleep(wait)
    resp, err = http.Get(url.String())
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
func process(resp *http.Response, h Handler) {
  in := resp.Body
  defer in.Close()

  respHeader := []byte(RespHeaderPfx)
  hdr := make([]byte, len(respHeader))
  if _, err := io.ReadFull(in, hdr); err != nil {
    h(0, nil, err)
    return
  }
  if bytes.Compare(respHeader, hdr) != 0 {
    h(0, nil, errors.New("invalid response header"))
    return
  }
  num, err := readNum(in, '\n')
  if err != nil {
    h(0, nil, errors.New("failed to get number of records"))
    return
  }
  for ;num > 0; num-- {
    processRec(in, h)
  }
}

/*    way/
 * Read the record header and send the record data to the
 * handler
 */
func processRec(in io.Reader, h Handler) {
  const TOOBIG = 1024
  recHeader := []byte(RecHeaderPfx)
  hdr := make([]byte, len(recHeader))
  if _, err := io.ReadFull(in, hdr); err != nil {
    h(0, nil, err)
    return
  }
  if bytes.Compare(recHeader, hdr) != 0 {
    h(0, nil, errors.New("invalid record header"))
    return
  }
  num, err := readNum(in, '|')
  if err != nil {
    h(0, nil, errors.New("invalid record number"))
    return
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
    return
  }
  if n == int(sz) {
    _,err = in.Read(data[sz-1:])
    if err != nil {
      if err != io.EOF {
        h(msgnum, nil, errors.New("failed reading record end"))
        return
      } else {
        data[sz] = '\n'
      }
    }
  }
  if data[sz] != '\n' {
    h(msgnum, nil, errors.New("record not terminated correctly"))
    return
  }
  h(msgnum, data[:sz], nil)
}

/*    way/
 * Read in bytes, one at a time till we hit the end
 * of the record and then return the matching number
 * found
 */
func readNum(in io.Reader, end byte) (uint64,error) {
  const BIGENOUGH = 32
  buf := make([]byte, BIGENOUGH)
  for i := 0;i < len(buf);i++ {
    p := buf[i:i+1]
    _,err := in.Read(p)
    if err != nil && err != io.EOF {
      return 0, errors.New("read failed")
    }
    if err == io.EOF || p[0] == end {
      return strconv.ParseUint(string(buf[:i]), 10, 32)
    }
  }
  return 0, errors.New("failed to header end")
}

type Handler func (num uint32, msg []byte, err error)
type Scheduler func (err error) time.Duration


