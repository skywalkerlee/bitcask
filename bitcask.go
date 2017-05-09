package bitcask

import (
	"encoding/binary"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type hint struct {
	dfNo     uint32
	valueSz  uint32
	valuePos uint32
	tstamp   uint32
}

type Bitcask struct {
	dir      string
	interval uint32
	mutex    *sync.RWMutex
	k2v      map[string]*hint
	hf       *os.File
	odfs     []*os.File
	adf      *os.File
}

func (bc *Bitcask) creatdf() error {
	var err error
	files, err := ioutil.ReadDir(bc.dir)
	if err != nil {
		return err
	}
	if len(files) <= 1 {
		bc.adf, err = os.OpenFile(bc.dir+"/datafile0.dat", os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
		return err
	}
	bc.odfs = append(bc.odfs, bc.adf)
	bc.adf, err = os.OpenFile(bc.dir+"/datafile"+strconv.Itoa(len(bc.odfs))+".dat", os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	return err
}

func (bc *Bitcask) Put(key, value []byte) error {
	var err error
	de := newDE(key, value)
	he := newHE(de)
	hint := new(hint)
	hint.tstamp = he.tstamp
	hint.valueSz = he.valueSz
	hint.dfNo = uint32(len(bc.odfs))
	he.dfNo = hint.dfNo
	bc.mutex.Lock()
	defer bc.mutex.Unlock()
	finfo, err := bc.adf.Stat()
	if err != nil {
		return err
	}
	if finfo.Size() > 5*1024*1024 {
		bc.creatdf()
		finfo, err = bc.adf.Stat()
		if err != nil {
			return err
		}
	}
	he.valuePos = uint32(finfo.Size()) + 16 + de.keySz
	hint.valuePos = he.valuePos
	bc.k2v[string(key)] = hint
	if _, err = bc.adf.Write(de.marshal()); err != nil {
		return err
	}
	if _, err = bc.hf.Write(he.marshal()); err != nil {
		return err
	}
	return nil
}

func (bc *Bitcask) Get(key []byte) []byte {
	bc.mutex.RLock()
	v, ok := bc.k2v[string(key)]
	bc.mutex.RUnlock()
	if !ok {
		return nil
	}
	tmp := make([]byte, v.valueSz)
	if int(v.dfNo) == len(bc.odfs) {
		bc.adf.ReadAt(tmp, int64(v.valuePos))
		return tmp
	}
	bc.odfs[int(v.dfNo)].ReadAt(tmp, int64(v.valuePos))
	return tmp
}

func NewBC(i ...uint32) *Bitcask {
	bc := new(Bitcask)
	if len(i) > 0 {
		bc.interval = i[0]
	} else {
		bc.interval = 3600
	}
	bc.mutex = new(sync.RWMutex)
	bc.k2v = make(map[string]*hint)
	bc.odfs = make([]*os.File, 0)
	return bc
}

func (bc *Bitcask) Open(dir string) error {
	defer func() {
		go bc.marge()
	}()
	bc.dir = dir
	if _, err := os.Stat(bc.dir); os.IsNotExist(err) {
		os.MkdirAll(bc.dir, 0777)
	}
	files, err := ioutil.ReadDir(bc.dir)
	if err != nil {
		return err
	}
	if len(files) <= 1 {
		if err = bc.creatdf(); err != nil {
			return err
		}
		if bc.hf, err = os.OpenFile(bc.dir+"/hint.dat", os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666); err != nil {
			return err
		}
	} else if len(files) == 2 {
		if bc.adf, err = os.OpenFile(bc.dir+"/"+files[0].Name(), os.O_RDWR|os.O_APPEND, 0666); err != nil {
			return err
		}
	} else {
		for _, f := range files[:len(files)-2] {
			if strings.HasPrefix(f.Name(), "datafile") {
				file, err := os.OpenFile(bc.dir+"/"+f.Name(), os.O_RDWR|os.O_APPEND, 0666)
				if err != nil {
					return err
				}
				bc.odfs = append(bc.odfs, file)
			}
		}
		bc.adf, err = os.OpenFile(bc.dir+"/"+files[len(files)-1].Name(), os.O_RDWR|os.O_APPEND, 0666)
	}
	return bc.load()
}

func (bc *Bitcask) load() error {
	var err error
	if bc.hf, err = os.OpenFile(bc.dir+"/hint.dat", os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666); err != nil {
		return err
	}
	return bc.scanhf()
}

func (bc *Bitcask) scanhf() error {
	header := make([]byte, 20)
	for {
		_, err := bc.hf.Read(header)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		hint := new(hint)
		hint.tstamp = binary.LittleEndian.Uint32(header[:4])
		hint.valueSz = binary.LittleEndian.Uint32(header[8:12])
		hint.dfNo = binary.LittleEndian.Uint32(header[12:16])
		hint.valuePos = binary.LittleEndian.Uint32(header[16:20])
		tmp := make([]byte, binary.LittleEndian.Uint32(header[4:8]))
		_, err = bc.hf.Read(tmp)
		if err != nil {
			return err
		}
		bc.k2v[string(tmp)] = hint
	}
}

func (bc *Bitcask) marge() {
	for {
		time.Sleep(time.Duration(bc.interval) * time.Second)
		bc.mutex.Lock()
		defer bc.mutex.Unlock()
		for k, v := range bc.k2v {
			tmp := make([]byte, v.valueSz)
			if int(v.dfNo) == len(bc.odfs) {
				bc.adf.ReadAt(tmp, int64(v.valuePos))
			} else {
				bc.odfs[int(v.dfNo)].ReadAt(tmp, int64(v.valuePos))
			}
			println(k, string(tmp))
		}
	}
}

func (bc *Bitcask) Close() {
	for _, f := range bc.odfs {
		f.Close()
	}
	if bc.adf != nil {
		bc.adf.Close()
	}
	if bc.hf != nil {
		bc.hf.Close()
	}
}
