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
  args: ["arguments","to","exe"],	
  sec: "10"
}
```

And adds status:

```json
{
  when: <ISO-Format>,
  ref: "msg id",
  exit: "code",
  op: "output"
}
```

