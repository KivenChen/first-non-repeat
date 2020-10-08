package main

const ( // overall config of this module
	WorkingDir       string = "/Volumes/MacData/PingCAP"
	OrigFilePath     string = "/Volumes/MacData/PingCAP/orig_tiny.txt"
	NumFileSplit     int    = 500
	ChunkSplit       int    = 1 << 20 * 40
	MemoryLimitBytes int64  = 1 << 30 * 5
	chanBufferSize   int    = 1 << 7
	BufferSizeBytes  int    = 1 << 20
	infinityInt64    int64  = 1<<63 - 1
	NumSplitConst    int64  = 1 << 20 * 50
	delimiter        byte   = ' '
)
