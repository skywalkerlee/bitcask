package bitcask

import "testing"

var bc *Bitcask

func Test_PutAndGet(t *testing.T) {
	bc = NewBC()
	if err := bc.Open("test"); err != nil {
		println(err.Error())
		return
	}
	if err := bc.Put([]byte("foo1"), []byte("bar1")); err != nil {
		t.Error(err.Error())
	}
	if err := bc.Put([]byte("foo2"), []byte("bar2")); err != nil {
		t.Error(err.Error())
	}
	if err := bc.Put([]byte("foo3"), []byte("bar3")); err != nil {
		t.Error(err.Error())
	}

	t.Log(string(bc.Get([]byte("foo1"))))
	t.Log(string(bc.Get([]byte("foo2"))))
	t.Log(string(bc.Get([]byte("foo3"))))
}

func Test_Marge(t *testing.T) {
	bc.marge()
}
