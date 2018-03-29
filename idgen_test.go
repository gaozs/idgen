package idgen

import (
	"testing"
	"time"
)

func TestIDGen(t *testing.T) {
	worker, err := NewWorker(MaxNodeID + 1)
	if err == nil {
		t.Error(MaxNodeID, "should not pass!")
	}
	worker, err = NewWorker(-100)
	if err == nil {
		t.Error("-100 should not pass!")
	}
	worker, err = NewWorker(MaxNodeID)
	if err != nil {
		t.Fatal(err)
	}

	batchNum := 1000000
	n := 10

	//test if sequence ids are same
	now := time.Now()
	var a, b int64
	a, err = worker.NextID()
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for i := 0; i < batchNum; i++ {
		b, err = worker.NextID()
		if err != nil {
			t.Fatal(err)
		}
		if b == a {
			t.Log(b)
			count++
		} else {
			a = b
		}
	}
	t.Log("get", batchNum, "ids used:", time.Since(now))
	if count != 0 {
		t.Error("has sequence same id, total count is not 0:", count)
	}

	//fulltest if ids are same
	now = time.Now()
	ids := make([]int64, batchNum*n)
	ch := make(chan int)

	for i := 0; i < n; i++ {
		go func(i int) {
			var err error
			s := i * batchNum
			e := s + batchNum
			for j := s; j < e; j++ {
				ids[j], err = worker.NextID()
				if err != nil {
					t.Fatal(err)
				}
			}
			ch <- 1
		}(i)
	}
	for i := 0; i < n; i++ {
		<-ch
	}
	t.Log("get", n, "*", batchNum, "ids used:", time.Since(now))
	m := make(map[int64]bool, batchNum*n)
	count = 0
	for i := 0; i < batchNum*n; i++ {
		if m[ids[i]] {
			count++
		}
		m[ids[i]] = true
	}
	t.Log("checked", n, "*", batchNum, "ids with repeated:", count)
}
