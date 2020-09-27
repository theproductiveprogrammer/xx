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
  exe: "path to executable",
  dir: "start directory",
  args: ["arguments","to","exe"],	
  secs: 10,
}
```

And adds status:

```json
{
  when: <ISO-Format>,
  ref: "msg id",
  pid: "process pid when running",
  exit: "code",
  op: "output"
}
```

