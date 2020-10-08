package main

import (
	"bufio"
	"github.com/cheggaaa/pb"
	"log"
	"os"
	"time"
)

func test_bruteRead() error {
	// this file aims to test the reading performance of golang

	file, err := os.OpenFile("/Volumes/MacData/PingCAP/orig.txt", os.O_RDONLY, 0644)
	stats, errstats := os.Stat("/Volumes/MacData/PingCAP/orig.txt")
	if err != nil {
		log.Fatal(err)
	}
	if errstats != nil {
		log.Fatal(errstats)
	}

	lenFile := stats.Size()
	reader := bufio.NewReaderSize(file, BufferSizeBytes)
	log.Println("File reader initiated")
	programStartTime := time.Now()

	progress := pb.New64(lenFile)
	var bytesRead int64 = 0
	progress.Start()
	line, err := reader.ReadString('\n')
	log.Println("Read Test: \nTrying to read first line", line)

	for err == nil {
		bytesRead += int64(len(line))
		progress.Set64(bytesRead)
		line, err = reader.ReadString('\n')
	}

	progress.Finish()
	programDuration := time.Since(programStartTime)
	log.Println("Total duration: ", programDuration.Seconds())
	return nil
}

func main_testBruteWrite() error {
	file, err := os.OpenFile("/Volumes/MacData/PingCAP/test_write.txt", os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0777)
	if err != nil {
		log.Fatal(err)
	}

	writer := bufio.NewWriterSize(file, BufferSizeBytes)
	log.Println("File writer initiated")
	programStartTime := time.Now()

	var bytesWritten, targetBytes int64 = 0, 1 << 30 * 10
	progress := pb.New64(targetBytes)
	progress.Start()
	line := "sinfully awake\n"
	lenLine, err := writer.WriteString(line)
	log.Println("Write Test: \nTrying to repeatedly write line:", line)

	for err == nil && bytesWritten < targetBytes {
		bytesWritten += int64(lenLine)
		progress.Set64(bytesWritten)
		lenLine, err = writer.WriteString(line)
	}

	progress.Finish()
	programDuration := time.Since(programStartTime)
	log.Println("Total duration: ", programDuration.Seconds())
	return nil
}

func testChanA(c chan int64) {
	log.Println("Gotcha:", <-c)
	c <- -1
}

func testChanB(c chan int64) {
	time.Sleep(3 * time.Second)
	c <- 233
	if <-c == -1 {
		log.Println("Yes!")
	}
}
