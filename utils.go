package main

import (
	"fmt"
	"hash/fnv"
	"io"
	"log"
	_ "log"
	"math/rand"
	"os"
)

type (
	// Struct for storing a word and its info
	WordStruct struct {
		word           string
		indexFirstOccr int64
		nOccr          int64
	}

	// Struct for finding the word struct
	WordCount struct {
		_map map[string]*WordStruct
	}
)

// The runes used to compose random words
var letterRunes = []rune("abcdefghijklmnopqrstuvwxyz")

func (index *WordCount) PutWord(word string, indexFirstOccr int64) {
	struct_, wordExist := index._map[word]
	if wordExist { // wordMap has been deleted
		struct_.nOccr++
	} else {
		if word == "TWODOG" {
			log.Println("putword: confirmed occurrence")
		}
		index._map[word] = &WordStruct{
			word:           word,
			indexFirstOccr: indexFirstOccr,
			nOccr:          1,
		}
	}
}

func (index *WordCount) ExtractFirstNonRepeat() (struct_ WordStruct, ok bool) {
	var indexOccr int64 = 1<<63 - 1
	resultStruct := WordStruct{nOccr: -1}
	for k, v := range index._map {
		if v.nOccr == 1 && v.indexFirstOccr < indexOccr {
			resultStruct = WordStruct{
				word:           k,
				indexFirstOccr: v.indexFirstOccr,
				nOccr:          1,
			}
		}
	}
	return resultStruct, resultStruct.nOccr != -1
}

// Creates a random word using 26 lower case letters
// params:
func CreateWord(minWordLen int, maxWordLen int) string {
	wordLen := rand.Intn(maxWordLen-minWordLen+1) + minWordLen
	wordInRunes := make([]rune, wordLen)
	for i := range wordInRunes {
		wordInRunes[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(wordInRunes)
}

func GetFileSize(path string) (int64, error) {
	fileInfo, err := os.Stat(path)
	if err != nil {
		return -1, err
	} else {
		return fileInfo.Size(), err
	}
}

func GetFileWriterByIndexChunk(indexChunk int) (io.Writer, error) {
	fileName := fmt.Sprint("chunk", indexChunk, ".txt")
	return os.OpenFile(fileName, os.O_RDWR, 0644)
}

func DeleteFile(path string) error {
	return os.Remove(path)
}

func Hash(str string) int {
	hashStream := fnv.New32a()
	_, _ = hashStream.Write([]byte(str))
	hashKey := hashStream.Sum32()
	return int(hashKey)
}

func HashAP(str string) int {
	hash := 0
	for i, r := range str {
		if i&1 == 0 { // odd index
			hash ^= (hash << 7) ^ int(r) ^ (hash >> 3)
		} else { // even index
			hash ^= ^((hash << 11) ^ int(r) ^ (hash >> 5))
		}
	}
	return hash
}
