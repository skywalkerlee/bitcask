package bitcask

import "encoding/binary"

type hintElem struct {
	tstamp   uint32
	keySz    uint32
	valueSz  uint32
	dfNo     uint32
	valuePos uint32
	key      []byte
}

func newHE(de *dataElem) *hintElem {
	he := new(hintElem)
	he.tstamp = de.tstamp
	he.keySz = de.keySz
	he.valueSz = de.valueSz
	he.key = de.key
	return he
}

func (he *hintElem) marshal() []byte {
	tmp := make([]byte, 20+he.keySz)
	binary.LittleEndian.PutUint32(tmp[:4], he.tstamp)
	binary.LittleEndian.PutUint32(tmp[4:8], he.keySz)
	binary.LittleEndian.PutUint32(tmp[8:12], he.valueSz)
	binary.LittleEndian.PutUint32(tmp[12:16], he.dfNo)
	binary.LittleEndian.PutUint32(tmp[16:20], he.valuePos)
	copy(tmp[20:], he.key)
	return tmp
}
