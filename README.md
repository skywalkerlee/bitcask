# example

```go
package main

import (
	"github.com/skywalkerlee/bitcask"
)

func main() {
	bc := bitcask.NewBC()
	bc.Open("test")
	defer bc.Close()
	bc.Put([]byte("foo1"), []byte("bar1"))
	bc.Put([]byte("foo2"), []byte("bar2"))
	bc.Put([]byte("foo3"), []byte("bar3"))
	println(string(bc.Get([]byte("foo1"))))
	println(string(bc.Get([]byte("foo2"))))
	println(string(bc.Get([]byte("foo3"))))
}

$ go run main.go
bar1
bar2
bar3

```
## TODO
* merge operation
* checksum compare
