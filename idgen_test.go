package idgen

import (
	"testing"
	"time"
)

func TestIDGen(t *testing.T) {
	worker, err := NewWorker(0, 18, 0)
	if err == nil {
		t.Error(18, 0, 0, "should not pass!")
	} else {
		t.Log(18, 0, 0, "Not pass!", err)
	}
	worker, err = NewWorker(0, 0, 9)
	if err == nil {
		t.Error(0, 9, 0, "should not pass!")
	} else {
		t.Log(0, 9, 0, "Not pass!", err)
	}
	worker, err = NewWorker(0, 14, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("Max ID:", worker.MaxNodeID())

	batchNum := 1000000
	n := 10

	//test if sequence ids are same
	now := time.Now()
	var a, b int64
	a, err = worker.NextID()
	t.Log("first id:", a)
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
	ch := make(chan error)

	for i := 0; i < n; i++ {
		go func(i int) {
			var err error
			s := i * batchNum
			e := s + batchNum
			for j := s; j < e; j++ {
				ids[j], err = worker.NextID()
				if err != nil {
					ch <- err
					return
				}
			}
			ch <- nil
		}(i)
	}
	for i := 0; i < n; i++ {
		err := <-ch
		if err != nil {
			t.Fatal(err)
		}
	}
	t.Log("get", n, "*", batchNum, "ids used:", time.Since(now))
	m := make(map[int64]bool, batchNum*n)
	count = 0
	for i := 0; i < batchNum*n; i++ {
		if m[ids[i]] {
			t.Log(i, "repeated with previous with ID", ids[i])
			count++
		}
		m[ids[i]] = true
	}
	t.Log("checked", n, "*", batchNum, "ids with repeated:", count)
	if count > 0 {
		t.Fatal("ID dupilicated!")
	}
}

func TestNextIDMust(t *testing.T) {
	worker, err := NewWorker(1, 14, 5)
	if err != nil {
		t.Fatal(err)
	}

	// 测试正常情况
	id := worker.NextIDMust()
	if id <= 0 {
		t.Error("NextIDMust should return positive id")
	}

	// 测试连续生成
	ids := make(map[int64]bool)
	for i := 0; i < 1000; i++ {
		id := worker.NextIDMust()
		if ids[id] {
			t.Errorf("Duplicate ID generated: %d", id)
		}
		ids[id] = true
	}
}

func TestNextIDErrorCases(t *testing.T) {
	worker, err := NewWorker(1, 14, 5)
	if err != nil {
		t.Fatal(err)
	}

	// 测试时间错误情况
	// 通过修改 lastMs 来模拟时间错误
	w := worker.(*idGenWork)
	w.Lock()
	w.lastMs = time.Now().UnixNano()/int64(time.Millisecond) + 1000
	w.Unlock()

	_, err = worker.NextID()
	if err == nil {
		t.Error("Expected error for time going backwards, got nil")
	}

	// 测试序列号溢出情况
	w.Lock()
	w.count = 1<<14 - 1 // 序列号达到最大值
	w.lastMs = time.Now().UnixNano() / int64(time.Millisecond)
	w.Unlock()

	// 连续生成多个ID，测试序列号重置和时间戳递增
	for i := 0; i < 5; i++ {
		_, err = worker.NextID()
		if err != nil {
			t.Errorf("Unexpected error when sequence overflow: %v", err)
		}
	}
}

func TestNewWorkerEdgeCases(t *testing.T) {
	// 测试默认参数
	_, err := NewWorker(1, 0, 0)
	if err != nil {
		t.Error("Expected default parameters to work, got error:", err)
	}

	// 测试边界值
	_, err = NewWorker(1, 12, 8) // sequenceBits + nodeIDBits = 20
	if err != nil {
		t.Error("Expected boundary values to work, got error:", err)
	}

	// 测试无效的节点ID
	_, err = NewWorker(-1, 14, 5)
	if err == nil {
		t.Error("Expected error for negative node ID, got nil")
	}

	maxNodeID := 1<<5 - 1 // 对于 nodeIDBits=5 的最大节点ID
	_, err = NewWorker(maxNodeID+1, 14, 5)
	if err == nil {
		t.Error("Expected error for node ID exceeding max, got nil")
	}
}
