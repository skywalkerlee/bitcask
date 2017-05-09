package bitcask

import (
	"encoding/binary"
	"hash/crc32"
	"time"
)

type dataElem struct {
	crc     uint32
	tstamp  uint32
	keySz   uint32
	valueSz uint32
	key     []byte
	value   []byte
}

func newDE(key, value []byte) *dataElem {
	de := new(dataElem)
	de.tstamp = uint32(time.Now().Unix())
	de.keySz = uint32(len(key))
	de.valueSz = uint32(len(value))
	de.key = key
	de.value = value
	tmp := make([]byte, 12, 32)
	binary.LittleEndian.PutUint32(tmp[:4], de.tstamp)
	binary.LittleEndian.PutUint32(tmp[4:8], de.keySz)
	binary.LittleEndian.PutUint32(tmp[8:], de.valueSz)
	tmp = append(tmp, key...)
	tmp = append(tmp, value...)
	de.crc = crc32.ChecksumIEEE(tmp)
	return de
}

func (de *dataElem) marshal() []byte {
	tmp := make([]byte, 16+de.keySz+de.valueSz, 16+de.keySz+de.valueSz)
	binary.LittleEndian.PutUint32(tmp[:4], de.crc)
	binary.LittleEndian.PutUint32(tmp[4:8], de.tstamp)
	binary.LittleEndian.PutUint32(tmp[8:12], de.keySz)
	binary.LittleEndian.PutUint32(tmp[12:16], de.valueSz)
	copy(tmp[16:16+de.keySz], de.key)
	copy(tmp[16+de.keySz:], de.value)
	return tmp
}
