package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

//encoding that we persist record
var (
	enc = binary.BigEndian
	)

//defines the number of bytes used to store the record’s length
const (
	lenWidth = 8
	)
		
type store struct{
	*os.File
	mu sync.Mutex
	buf *bufio.Writer
	size uint64
}

func newStore(f *os.File) (*store,error){
	//get size os file to open
	fil,err:=os.Stat(f.Name())
	if err!= nil{
		return nil,err 
		
	}
	//get the size of the file opened
	size := uint64(fil.Size())
	return &store{
		File: f,
		size: size,
		buf: bufio.NewWriter(f),
	},nil

}

func (s* store)Append(p[]byte)(n uint64, pos uint64, err error){

	s.mu.Lock()
	defer s.mu.Unlock()
	//get the size of the record here for the append
	pos = s.size
	// Write the length of the payload 'p' as a uint64 to the buffer 's.buf'.
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}
	//write the record
	w,err :=s.buf.Write(p)
	if err != nil{
		return 0,0,err
	}
	//nombre of bytes written
	w += lenWidth
	// new size of the record
	s.size += uint64(w)
	return uint64(w),pos,nil
}

func (s* store)Read(pos uint64)([]byte,error){

	s.mu.Lock()
	defer s.mu.Unlock()

	//prevent the buffer for reading previous things
	if err := s.buf.Flush();err!= nil{
		return nil,err
	}

	//create a slice to read with size of reecord
	size := make([]byte,lenWidth)

	//read record at given pos
	if _,err := s.File.ReadAt(size,int64(pos));err!= nil{
		return nil,err
	}
	//gett the record
	b:=make([]byte,enc.Uint64(size))

	if _,err := s.File.ReadAt(b,int64(lenWidth+pos));err !=nil{
		return nil,err
	}

	return b,nil

}

func (s *store) ReadAt(p []byte, off int64) (int, error) {

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
	return 0, err
	}
	return s.File.ReadAt(p, off)
}

func (s *store) Close()error{

	s.mu.Lock()
	defer s.mu.Unlock()
	err:=s.buf.Flush()
	if err != nil{
		return err
	}
	return s.File.Close()


}
	