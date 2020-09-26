package main

import (
  "strconv"
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

  kaddr = kaddr + "xx?from="
  var msgnum uint64 = 1
  var url strings.Builder

  url.WriteString(kaddr)
  url.WriteString(strconv.FormatUint(msgnum, 10))
  _, err := http.Get(url.String())
  if err != nil {
    log.Fatal("Failed to connect")
  }

  for {
  }
}
