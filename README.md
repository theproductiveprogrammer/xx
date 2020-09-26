# XX

*Executes programs*

## Usage

Uses [kaf](https://github.com/theproductiveprogrammer/kaf) to decide what to do:

```sh
$> xx <kaf addr>  # start
```

Listens for start requests on log `xx`.

```json
{
  src: "requester",
  exe: "full command to start",
  log: "none" | "out" | "err",
  sec: "10"
}
```

And adds status:

```json
{
  when: <ISO-Format>,
  ref: "msg id",
  exit: "code",
  out: "output",
  err: "output"
}
```

