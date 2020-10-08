package main

import (
	"bufio"
	"fmt"
	pb2 "github.com/cheggaaa/pb/v3"
	"log"
	"os"

	"time"
)

func main_() { // test out the file split utility
	fmt.Println("Module initiating")
	programStartTime := time.Now()

	var splitReaders [NumFileSplit]*bufio.Reader
	var splitWriters [NumFileSplit]*bufio.Writer
	for i := 0; i < NumFileSplit; i++ {
		chunkFileName := fmt.Sprint(WorkingDir, "/chunk", i, ".txt")
		newFile, err := os.OpenFile(chunkFileName, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, os.ModeAppend)
		if err != nil {
			log.Fatal("Failed to create chunk file writers")
		}
		splitWriters[i] = bufio.NewWriterSize(newFile, BufferSizeBytes)
		splitReaders[i] = bufio.NewReaderSize(newFile, BufferSizeBytes)

		//log.Println("Created reader and writer for ", i, "th " +
		//"file chunk \"", chunkFileName, "\"")
	}

	fileSplit := FileSplitReader{
		origFilePath:     OrigFilePath,
		destFilePath:     WorkingDir, // todo: fix usage problem
		startCharIndex:   0,
		memLimitPerSplit: 1 << 20 * 40,
		cacher: &FileSplitsCacheWriter{
			_map:                map[string]int64{},
			maxMemSizeBytes:     1 << 30 * 6,
			currentMemSizeBytes: 0,
			//origReaderChan chan[word string]
			splitWriters: splitWriters,
		},
	}
	log.Println("Reducing file to clusters...")
	fileSplit.proceed(delimiter)
	programDuration := time.Since(programStartTime)
	fmt.Println("Reduction duration: ", programDuration.Seconds())
}

type (
	// the hashmap cache used to quickly identify repetitions when
	// original file is being splited
	FileSplitsCacheWriter struct {
		_map                map[string]int64
		maxMemSizeBytes     int64
		currentMemSizeBytes int64
		writeBufferChan     chan *WordStruct
		_bytesWritten       int64
		bytesFlushed        chan int64
		_readDone           bool
		splitWriters        [NumFileSplit]*bufio.Writer
		writeDone           bool
	}

	FileSplitReader struct {
		origFilePath     string
		destFilePath     string
		startCharIndex   int64
		memLimitPerSplit int64
		//writeBufferChan  chan *WordStruct
		cacher *FileSplitsCacheWriter
	}
)

func (c *FileSplitsCacheWriter) InputWord(wordStruct *WordStruct) {
	key := wordStruct.word
	wordFreq, keyExists := c._map[key]
	if !keyExists && c.memorySufficient() {
		// sufficient capacity for a non-repetitive word
		c._map[key] = 1
		c.currentMemSizeBytes += 20 + int64(len(key)) // hashcode, int64 freq, ref to key and the key literal
	}

	if keyExists && c.memorySufficient() {
		// sufficient but the word is duplicated
		c._map[key] += 1
	}

	// todo: any better algorithms to drop from map?
	if (keyExists && wordFreq < 2) || (!keyExists && !c.memorySufficient()) {
		// NOT sufficient capacity for new word
		indexChunk := Hash(key) % NumFileSplit
		c.writeBuffer(c.splitWriters[indexChunk], wordStruct)
	}
}

func (c *FileSplitsCacheWriter) memorySufficient() bool {
	return c.currentMemSizeBytes < c.maxMemSizeBytes
}

func (c *FileSplitsCacheWriter) writeBuffer(writer *bufio.Writer, wordStruct *WordStruct) {
	toWrite := fmt.Sprintf("%s %d\n", wordStruct.word, wordStruct.indexFirstOccr)
	if writer.Buffered()+len(toWrite) > BufferSizeBytes {
		//log.Println("flush?")
		if err := writer.Flush(); err != nil {
			log.Fatal("Error flushing buffer", err)
		}
		//log.Println("flush")
	}

	if bytesWritten, err := writer.WriteString(toWrite); err != nil {
		log.Fatal("Error writing buffer", err)
	} else {
		c._bytesWritten += int64(bytesWritten)
	}
}

func (worker *FileSplitReader) proceed(delimiter byte) {
	worker.readThread(delimiter)
}

func (worker *FileSplitReader) readThread(delimiter byte) {
	origFile, errR := os.OpenFile(worker.origFilePath, os.O_RDONLY, os.ModePerm)
	stat, errS := os.Stat(worker.origFilePath)
	if errR != nil || errS != nil {
		log.Fatal("Cannot read orig file")
	}
	var lenFile, lenRead int64 = stat.Size(), 0
	worker.cacher._readDone = false
	go worker.cacher.writeThread(delimiter)

	reader := bufio.NewReaderSize(origFile, BufferSizeBytes)
	progress := pb2.New64(lenFile)
	progress.Start()

	word, errR := reader.ReadString(delimiter) // todo: switch to read string
	var indexCounter int64 = 0
	for errR == nil {
		lenRead += int64(len(word) + 1)
		progress.SetCurrent(lenRead)
		indexCounter++
		wordStruct := WordStruct{
			word[:len(word)-1],
			indexCounter, // todo: implement counter
			0,
		}
		worker.cacher.writeBufferChan <- &wordStruct
		word, errR = reader.ReadString(delimiter)
	}
	worker.cacher._readDone = true
	//log.Println("cacher now: ", worker.cacher._readDone)
	for !worker.cacher.writeDone {
	}
	progress.Finish()
	//log.Println("Written bytes: ", <- worker.cacher.bytesFlushed)

}

func (c *FileSplitsCacheWriter) writeThread(delimiter byte) {
	newAdded, addOccr, toFile := 0, 0, 0
	for !c._readDone || len(c.writeBufferChan) > 0 {
		//log.Println(c._readDone, len(c.writeBufferChan))
		//if c._readDone {
		//	log.Println("Read done. Existing buffer", len(c.writeBufferChan))
		//}
		wordStruct := <-c.writeBufferChan
		key := wordStruct.word
		wordFreq, keyExists := c._map[key]

		if !keyExists && c.memorySufficient() {
			// sufficient capacity for a non-repetitive word
			c._map[key] = 1
			c.currentMemSizeBytes += 60 + int64(len(key)) // hashcode, int64 freq, the index ref to key and the key literal
			newAdded++
			keyExists = true
		}

		if keyExists && c.memorySufficient() {
			// sufficient but the word is duplicated
			c._map[key] += 1
			addOccr++
		}

		// todo: any better algorithms to drop from map?
		if (keyExists && wordFreq <= 2) || (!keyExists && !c.memorySufficient()) {
			// NOT sufficient capacity for new word
			indexChunk := Hash(key) % NumFileSplit
			c.writeBuffer(c.splitWriters[indexChunk], wordStruct)
			toFile++
		}
	}
	//log.Println(newAdded, addOccr, toFile)

	flushAllBuffer(c.splitWriters)
	c.writeDone = true
	log.Println("Completed writing to file splits")
}

func flushAllBuffer(writers [NumFileSplit]*bufio.Writer) {
	for _, w := range writers {
		if err := w.Flush(); err != nil {
			log.Fatal(err)
		}
	}
}
