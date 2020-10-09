# First-Non-Repeat
Finding the first non-repeating word in a 100G file.
- One original file scan only
- Reduce IO in best possibility/

# Usage
The main file is `main.go` and config constant are stored in `config.go`.
Effective fields are:

```
WorkingDir       string = "/Volumes/MacData/PingCAP"
OrigFilePath     string = "/Volumes/MacData/PingCAP/orig.txt"
NumFileSplit     int    = 500           // number of file that contains splits of original file
MemoryLimitBytes int64  = 1 << 30 * 5   // size of hash map
chanBufferSize   int    = 1 << 7        // channel size for file split IO
BufferSizeBytes  int    = 1 << 20 * 1   // buffer size for file reader and writers
infinityInt64    int64  = 1 << 63 - 1
delimiter 		   byte   = ' '   // the delimiter byte that separates words
```
