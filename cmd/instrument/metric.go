package instrument

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"time"
)

/**
纳秒，符号ns（英语：nanosecond ）.
1纳秒等于十亿分之一秒（10-9秒）
1,000 纳秒 = 1微秒
1,000,000 纳秒 = 1毫秒
1,000,000,000 纳秒 = 1秒
*/

const (
	benchResolution = 10000
	benchBucket     = 1000000000 / benchResolution
)

// An efficient statics collecting and rendering
type Stats struct {
	Data       []int
	Overflow   []int
	LocalStats []Stat
	Start      time.Time
	End        time.Time
	Total      int
}

type Stat struct {
	Completed   int
	Failed      int
	Transferred int64
}

var percentages = []int{50, 66, 75, 80, 90, 95, 98, 99, 100}

func NewStats(n int) *Stats {
	return &Stats{
		Data:       make([]int, benchResolution),
		Overflow:   make([]int, 0),
		LocalStats: make([]Stat, n),
	}
}

func (s *Stats) AddSample(d time.Duration) {
	index := int(d / benchBucket)
	if index < 0 {
		fmt.Printf("This request takes %3.1f seconds, skipping!\n", float64(index)/benchResolution)
	} else if index < len(s.Data) {
		s.Data[index]++
	} else {
		s.Overflow = append(s.Overflow, index)
	}
}

func (s *Stats) CheckProgress(finishChan chan bool, wait *sync.WaitGroup, progressName ...string) {
	if len(progressName) > 0 {
		fmt.Printf("\n------------ %s ----------\n", progressName[0])
	}
	ticker := time.Tick(time.Second)
	lastCompleted, lastTransferred, lastTime := 0, int64(0), time.Now()
	for {
		select {
		case <-finishChan:
			wait.Done()
			return
		case t := <-ticker:
			completed, transferred, taken := 0, int64(0), t.Sub(lastTime)
			for _, localStat := range s.LocalStats {
				completed += localStat.Completed
				transferred += localStat.Transferred
			}
			fmt.Printf("Completed %d of %d requests, %3.1f%% %3.1f/s %3.1fMB/s\n",
				completed, s.Total, float64(completed)*100/float64(s.Total),
				float64(completed-lastCompleted)*float64(int64(time.Second))/float64(int64(taken)),
				float64(transferred-lastTransferred)*float64(int64(time.Second))/float64(int64(taken))/float64(1024*1024),
			)
			lastCompleted, lastTransferred, lastTime = completed, transferred, t
		}
	}
}

func (s *Stats) PrintStats(concurrency int, progressName ...string) {
	completed, failed, transferred := 0, 0, int64(0)
	for _, localStat := range s.LocalStats {
		completed += localStat.Completed
		failed += localStat.Failed
		transferred += localStat.Transferred
	}
	timeTaken := int64(s.End.Sub(s.Start)) // nanosecond
	fmt.Printf("\nConcurrency Level:      %d\n", concurrency)
	printTimeTaken(timeTaken)

	fmt.Printf("Complete requests:      %d\n", completed)
	fmt.Printf("Failed requests:        %d\n", failed)
	fmt.Printf("Total transferred:      %d bytes\n", transferred)
	secondTaken := float64(timeTaken) / float64(int64(time.Second))
	fmt.Printf("Requests per second:    %.2f [#/sec]\n", float64(completed)/secondTaken)
	fmt.Printf("Transfer rate:          %.2f [Kbytes/sec]\n", float64(transferred)/1024/secondTaken)
	n, sum := 0, 0
	min, max := 10000000, 0
	for i := 0; i < len(s.Data); i++ {
		n += s.Data[i]
		sum += s.Data[i] * i
		if s.Data[i] > 0 {
			if min > i {
				min = i
			}
			if max < i {
				max = i
			}
		}
	}
	n += len(s.Overflow)
	for i := 0; i < len(s.Overflow); i++ {
		sum += s.Overflow[i]
		if min > s.Overflow[i] {
			min = s.Overflow[i]
		}
		if max < s.Overflow[i] {
			max = s.Overflow[i]
		}
	}
	avg := float64(sum) / float64(n)
	varianceSum := 0.0
	for i := 0; i < len(s.Data); i++ {
		if s.Data[i] > 0 {
			d := float64(i) - avg
			varianceSum += d * d * float64(s.Data[i])
		}
	}
	for i := 0; i < len(s.Overflow); i++ {
		d := float64(s.Overflow[i]) - avg
		varianceSum += d * d
	}
	std := math.Sqrt(varianceSum / float64(n))
	fmt.Printf("\nConnection Times (ms)\n")
	fmt.Printf("              min      avg        max      std\n")
	fmt.Printf("Total:        %2.1f      %3.1f       %3.1f      %3.1f\n", float32(min)/10, float32(avg)/10, float32(max)/10, std/10)
	//printing percentiles
	fmt.Printf("\nPercentage of the requests served within a certain time\n")
	percentiles := make([]int, len(percentages))
	for i := 0; i < len(percentages); i++ {
		percentiles[i] = n * percentages[i] / 100
	}
	percentiles[len(percentiles)-1] = n
	percentileIndex := 0
	currentSum := 0
	for i := 0; i < len(s.Data); i++ {
		currentSum += s.Data[i]
		if s.Data[i] > 0 && percentileIndex < len(percentiles) && currentSum >= percentiles[percentileIndex] {
			fmt.Printf("  %3d%%    %5.1f ms\n", percentages[percentileIndex], float32(i)/10.0)
			percentileIndex++
			for percentileIndex < len(percentiles) && currentSum >= percentiles[percentileIndex] {
				percentileIndex++
			}
		}
	}
	sort.Ints(s.Overflow)
	for i := 0; i < len(s.Overflow); i++ {
		currentSum++
		if percentileIndex < len(percentiles) && currentSum >= percentiles[percentileIndex] {
			fmt.Printf("  %3d%%    %5.1f ms\n", percentages[percentileIndex], float32(s.Overflow[i])/10.0)
			percentileIndex++
			for percentileIndex < len(percentiles) && currentSum >= percentiles[percentileIndex] {
				percentileIndex++
			}
		}
	}
	if len(progressName) > 0 {
		fmt.Printf("\n------------ %s ----------\n", progressName[0])
	}
}

func printTimeTaken(ns int64) {
	fns := float64(ns)
	take := fns / float64(int64(time.Second))
	if take > 1 {
		fmt.Printf("Time taken for tests:   %.3f seconds\n", take)
	} else {
		take = fns / float64(int64(time.Millisecond))
		if take > 1 {
			fmt.Printf("Time taken for tests:   %.3f milliseconds\n", take)
		} else {
			take = fns / float64(int64(time.Microsecond))
			fmt.Printf("Time taken for tests:   %.3f microseconds\n", take)
		}
	}
}

func PrintProgress(finishChan chan bool, wait *sync.WaitGroup, progressName ...string) {
	if len(progressName) > 0 {
		fmt.Printf("\n------------ %s ----------\n", progressName[0])
	}
	ticker := time.Tick(time.Second)

	for {
		select {
		case <-finishChan:
			wait.Done()
			if len(progressName) > 1 {
				fmt.Printf("\n------------ %s ----------\n", progressName[1])
			}
			return
		case <-ticker:
			fmt.Print(".")
		}
	}
}
