package main

import (
	"log"
	"testing"
)

type teststruct struct {
	a int
}

func BenchmarkMain(m *testing.B) {
	main()
}

func BenchmarkTestChanA(B *testing.B) {
	T := teststruct{
		a: 233,
	}
	T.modifyA()
	log.Println(T.a)
}

func (t teststruct) modifyA() {
	t.a = 2333
}
