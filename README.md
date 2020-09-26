# XX

*Executes programs*

## Usage

Uses [kaf](https://github.com/theproductiveprogrammer/kaf) to decide what to do:

```sh
$> xx <kaf addr>  # start
```

Listens for start requests on log `xx`:

```json
{
  from: "requester",
  proc: "full command to start",
  report: none | exit | out | err,
  report: {
    every: "10", /* seconds */
    report: out | err,
  }
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

