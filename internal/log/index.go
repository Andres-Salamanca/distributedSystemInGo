package log


import (
	"io"
	"os"
	"github.com/tysonmote/gommap"
)

var (
	offWidth uint64 = 4
	posWidth uint64 = 8
	entWidth = offWidth + posWidth
)
type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}
	
func newIndex(f *os.File,c Config)(*index,error){


	idx := &index{
		file: f,
		}

	fi, err := os.Stat(f.Name())

	if err != nil {
	return nil, err
	}

	idx.size = uint64(fi.Size())

	if err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes),); err != nil {
		return nil, err
	}
	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,); err != nil {
		return nil, err
	}

	return idx, nil

}

func (i*index)Close()error{
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
		}
		if err := i.file.Sync(); err != nil {
		return err
		}
		if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
		}
		return i.file.Close()

}

func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
    // Check if the index is empty.
    if i.size == 0 {
        return 0, 0, io.EOF
    }

    // Determine the index to read based on the provided input.
    if in == -1 {
        out = uint32((i.size / entWidth) - 1)
    } else {
        out = uint32(in)
    }

    // Calculate the starting position of the entry in the mmap.
    pos = uint64(out) * entWidth

    // Check if there is enough space to read a full entry.
    if i.size < pos+entWidth {
        return 0, 0, io.EOF
    }

    // Read the offset and position fields from the mmap.
    out = enc.Uint32(i.mmap[pos : pos+offWidth])
	pos = enc.Uint64(i.mmap[pos+offWidth : pos+entWidth])

    // Return the read offset, position, and nil to indicate success.
    return out, pos, nil
}


func (i *index) Write(off uint32, pos uint64) error {
    // Check if there is enough space in the mmap to write the entry.
    if uint64(len(i.mmap)) < i.size+entWidth {
        return io.EOF
    }

    // Write the offset 'off' to the mmap at the current size position.
    enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)

    // Write the position 'pos' to the mmap at the offset field's end position.
    enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)

    // Update the index size by adding the size of the new entry.
    i.size += uint64(entWidth)

    // Return nil to indicate success.
    return nil
}


func (i *index) Name() string {
	return i.file.Name()
}
	


