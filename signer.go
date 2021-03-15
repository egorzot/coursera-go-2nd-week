package main

import (
	"sort"
	"strconv"
	"sync"
)

var md5Chan chan struct{}
var wgSingle sync.WaitGroup
var wgMulti sync.WaitGroup

func SingleHash(in, out chan interface{}) {
	for {
		val, ok := <-in
		if !ok {
			wgSingle.Wait()
			return
		}
		wgSingle.Add(1)
		str := strconv.FormatInt(int64((val).(int)), 10)
		singleRes := make(chan string, 2)
		go func(s string, chOut chan<- string) {
			hash := DataSignerCrc32(str)
			chOut <- hash
			return
		}(str, singleRes)

		go func(s string, chOut chan<- string, mdChan chan struct{}) {
			md5Chan <- struct{}{}
			hash := DataSignerMd5(s)
			<-md5Chan
			hash = DataSignerCrc32(hash)
			chOut <- hash
		}(str, singleRes, md5Chan)

		go func(res chan string, chOut chan<- interface{}, s string) {
			c := (<-res) + "~" + (<-res)
			chOut <- c
			wgSingle.Done()
			close(res)
		}(singleRes, out, str)
	}

}

type multiResult struct {
	i int
	s string
}

func MultiHash(in, out chan interface{}) {
	for {
		s, ok := <-in

		if !ok {
			wgMulti.Wait()
			return
		}
		multiValues := make(chan multiResult, 6)
		wgMulti.Add(1)

		for i := 0; i < 6; i++ {
			go func(i int, v chan multiResult) {
				hash := DataSignerCrc32(strconv.Itoa(i) + s.(string))
				v <- multiResult{i: i, s: hash}
				return
			}(i, multiValues)
		}

		go func(v chan multiResult, chOut chan<- interface{}) {
			r := make([]string, 6)
			count := 0
			for {
				res, ok := <-v
				if !ok {
					return
				}
				r[res.i] = res.s
				count++
				if count == 6 {
					result := ""
					for _, str := range r {
						result += str
					}
					chOut <- result
					wgMulti.Done()
					close(v)

				}
			}
		}(multiValues, out)
	}
}

func CombineResults(in, out chan interface{}) {
	var resultMap []string
	for {
		s, ok := <-in
		if !ok {
			sort.Strings(resultMap)
			v := ""
			for _, str := range resultMap {
				v += "_" + str
			}
			v = v[1:]
			out <- v
			return
		}
		resultMap = append(resultMap, s.(string))
	}
}

func ExecutePipeline(jobs ...job) {
	md5Chan = make(chan struct{}, 1)
	out := make(chan interface{})

	for i, _ := range jobs {
		newOut := make(chan interface{})
		if i == len(jobs)-1 {
			jobs[i](out, newOut)
			continue
		}
		go func(j job, in chan interface{}, out chan interface{}) {
			j(in, out)
			defer close(out)
		}(jobs[i], out, newOut)
		out = newOut
	}
}
