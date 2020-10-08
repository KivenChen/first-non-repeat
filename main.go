package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

// TODO: fix ReadString isPrefix problem (at least think about it)
func main() {
	fmt.Println("Module initiated")
	programStartTime := time.Now()
	resultWordStruct := WordStruct{
		word:           "not_found",
		indexFirstOccr: infinityInt64,
		nOccr:          infinityInt64,
	}

	// initialize file pipelines
	var splitReaders [NumFileSplit]*bufio.Reader
	var splitWriters [NumFileSplit]*bufio.Writer
	var fpRead [NumFileSplit]*os.File
	var fpWrite [NumFileSplit]*os.File
	var err error
	for i := 0; i < NumFileSplit; i++ {
		chunkFileName := fmt.Sprint(WorkingDir, "/chunk", i, ".txt")
		fpWrite[i], err = os.OpenFile(chunkFileName, os.O_TRUNC|os.O_CREATE|os.O_RDWR, os.ModePerm)
		if err != nil {
			log.Fatal("Failed to create chunk file writers")
		}
		splitWriters[i] = bufio.NewWriterSize(fpWrite[i], BufferSizeBytes)
		//splitReaders[i] = bufio.NewReaderSize(newFile, BufferSizeBytes)

		log.Println("Created reader and writer for ", i, "th "+
			"file chunk \"", chunkFileName, "\"")
	}

	fileSplitBufferChan := make(chan *WordStruct, chanBufferSize)
	// split files into chunk
	fileSplit := FileSplitReader{
		origFilePath:     OrigFilePath,
		destFilePath:     WorkingDir, // todo: fix usage problem
		startCharIndex:   0,
		memLimitPerSplit: 1 << 20 * 40,
		cacher: &FileSplitsCacheWriter{
			_map:                make(map[string]int64),
			maxMemSizeBytes:     MemoryLimitBytes,
			writeBufferChan:     fileSplitBufferChan,
			currentMemSizeBytes: 0,
			bytesFlushed:        make(chan int64),
			splitWriters:        splitWriters,
		},
	}
	fileSplit.proceed(delimiter)
	log.Println("File Split Completed")
	log.Println("Total duration: ", time.Since(programStartTime).Seconds())

	for i, file := range fpWrite {
		if file != nil {
			_ = file.Close()
		}
		chunkFileName := fmt.Sprint(WorkingDir, "/chunk", i, ".txt")
		fpRead[i], err = os.OpenFile(chunkFileName, os.O_RDONLY, os.ModePerm)
		splitReaders[i] = bufio.NewReaderSize(fpRead[i], BufferSizeBytes)
		if err != nil {
			log.Fatal(err)
		}
	}
	log.Println("writer pipeline closed")

	// find non-repetitive words by chunk
	//progress := pb.New(NumFileSplit)
	//progress.Start()
	for i, chunkReader := range splitReaders {
		//log.Println("Progress:", i, "/", len(splitReaders))
		wordPool := WordCount{map[string]*WordStruct{}}
		line, err := chunkReader.ReadString('\n')
		if err != nil {
			log.Println(i, line)
			log.Fatal(err)
		}

		for err == nil {
			fields := strings.Split(line, " ")
			if fields[0] == "TWODOG" {
				log.Println("Confirmed occurrence")
			}
			if len(fields) == 2 { // first field: word. Second field: word count
				parseInt, err := strconv.ParseInt(fields[1][:len(fields[1])-1], 10, 64)
				if err != nil {
					log.Fatal("Main: chunk file index parse int failed: ", fields[1])
				}
				if parseInt > resultWordStruct.indexFirstOccr { // older
					break
				}
				wordPool.PutWord(fields[0], parseInt)
			}
			line, err = chunkReader.ReadString('\n')
		}

		// todo: enhance performance of thsi Extract method
		wordStruct, resExists := wordPool.ExtractFirstNonRepeat()
		if resExists && wordStruct.nOccr == 1 && wordStruct.indexFirstOccr < resultWordStruct.indexFirstOccr {
			resultWordStruct = wordStruct
		}
		//log.Printf("Res: \"%s\", Now Extracted: \"%s\"\n", resultWordStruct.word, wordStruct.word)
		//progress.Increment()
	}
	//progress.Finish()
	fmt.Printf("Found result word: \"%s\" %d\n", resultWordStruct.word, resultWordStruct.indexFirstOccr)

	programDuration := time.Since(programStartTime)
	fmt.Println("Total duration: ", programDuration.Seconds())
}
