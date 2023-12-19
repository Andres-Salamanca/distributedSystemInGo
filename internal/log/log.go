package log

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	api "github.com/Andres-Salamanca/proglog/api/v1"
)

type Log struct {
	mu sync.RWMutex
	Dir string
	Config Config
	activeSegment *segment
	segments []*segment
}

func NewLog(dir string,c Config)(*Log,error){

	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = 1024
	}
	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = 1024
	}
	l := &Log{
		Dir: dir,
		Config: c,
	}
		
	return l,l.Setup()

}

func (l *Log) Setup()error{
// Read the contents of the log directory
	files, err := ioutil.ReadDir(l.Dir)
	if err != nil {
		return err
	}
	// Extract base offsets from file names
	var baseOffsets []uint64
	for _, file := range files{

		offStr := strings.TrimSuffix(
			file.Name(),
			path.Ext(file.Name()),
		)


		off, _ := strconv.ParseUint(offStr, 10, 0)
		baseOffsets = append(baseOffsets, off)
			
	}
 	// Sort base offsets in ascending order
	sort.Slice(baseOffsets, func(i, j int) bool {

		return baseOffsets[i] < baseOffsets[j]
		
	})

	 // Create segments based on sorted base offsets
	for i := 0; i < len(baseOffsets); i++ {

		if err = l.newSegment(baseOffsets[i]); err != nil {
			return err
		}
		// baseOffset contains dup for index and store so we skip
		// the dup
		i++
	}

	 // If no segments were created, create a new segment with the initial offset
	if l.segments == nil {

		if err = l.newSegment(
		l.Config.Segment.InitialOffset,
		); err != nil {
			return err
		}

	}

	return nil
		
}


func (l* Log)Append(record *api.Record)(uint64,error){

	l.mu.Lock()
	defer l.mu.Unlock()
	off,err:=l.activeSegment.Append(record)
	if err!=nil{
		return 0 , err
	}
	if l.activeSegment.IsMaxed(){
		err = l.newSegment(off + 1)
	}

	return off,err

}

func (l* Log)Read(off uint64)(*api.Record,error){

	l.mu.Lock()
	defer l.mu.Unlock()

	var s *segment

	for _,segment := range l.segments{

		if segment.baseOffset <= off && off < segment.nextOffset{
			s =segment
			break
		}

	}

	if s==nil || s.nextOffset<= off{

		return nil, fmt.Errorf("offset out of range: %d", off)
	}

	return s.Read(off)


}

func (l* Log)Close()error{

	l.mu.Lock()
	defer l.mu.Unlock()
	for _,segment := range l.segments{

		err := segment.Close()
		if err != nil{
			return err
		}

	}

	return nil

}



func (l* Log)Remove()error{


	if err := l.Close();err!= nil{

		return err
	}

	return os.RemoveAll(l.Dir)

}

func (l* Log)Reset(uint64)error{

	if err := l.Remove();err!= nil{

		return err
	}

	return l.Setup()

}


func (l *Log) LowestOffset() (uint64, error) {

	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.segments[0].baseOffset, nil

}

func (l *Log) HighestOffset() (uint64, error) {

	l.mu.RLock()
	defer l.mu.RUnlock()
	off := l.segments[len(l.segments)-1].nextOffset
	if off == 0 {
	return 0, nil
	}
	return off - 1, nil

}

func (l *Log) Truncate(lowest uint64) error {

	l.mu.RLock()
	defer l.mu.RUnlock()

	var segments []*segment
	for _, s := range l.segments {

		if s.nextOffset <= lowest+1 {

			if err := s.Remove(); err != nil {
				return err
			}
			continue

		}
		segments = append(segments, s)

	}

	l.segments = segments
	return nil

}

func(l*Log)Reader()io.Reader{

	l.mu.RLock()
	defer l.mu.RUnlock()

	readers := make([]io.Reader, len(l.segments))
	for i,segment := range l.segments{

		readers[i]= &originReader{segment.store, 0}
		
	}

	return io.MultiReader(readers...)


}

type originReader struct {
	*store
	off int64
}

func (o *originReader) Read(p []byte) (int, error) {

	n, err := o.ReadAt(p, o.off)
	o.off += int64(n)
	return n, err

}
	

func (l*Log)newSegment(off uint64) error{

	s, err := newSegment(l.Dir, off, l.Config)
	if err != nil{
		return err
	}
	l.segments = append(l.segments, s)
	l.activeSegment = s
	return nil
}
