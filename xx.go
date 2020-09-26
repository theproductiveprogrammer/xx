package main

import (
  "strconv"
  "io/ioutil"
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
  run(os.Args[1])
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
func run(kaddr string) {
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
      process(resp)
    }
    time.Sleep(7 * time.Second)
    resp, err = http.Get(url.String())
  }
}

/*    way/
 * Get the message data for processing
 */
func process(resp *http.Response) {
  msgs := loadMsgs(resp)
  fmt.Println(msgs)
}

func loadMsgs(resp *http.Response) []msg {
  defer resp.Body.Close()
  body, err := ioutil.ReadAll(resp.Body)
  if err != nil {
    fmt.Println(err)
    return nil
  }
  return []msg{ msg{ string(body) } }
}

type msg struct {
  v string
}


