package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"runtime/pprof"
	"slices"
	"strconv"
	"sync"
)

func main() {
	var cpuprofile = flag.String("cpuprofile", "", "write CPU profile to string")
	var memprofile = flag.String("memprofile", "", "write memory profile to string")
	var inputFile = flag.String("inputfile", "./measurements.txt", "override for input file containing temperature measurements")
	flag.Parse()

	if *cpuprofile != "" {

		f, err := os.Create(*cpuprofile)
		if err != nil {
			panic("Failed to create CPU Profile file")
		}

		defer f.Close()

		if err := pprof.StartCPUProfile(f); err != nil {
			panic("Could not start CPU Profiling!")
		}

		defer pprof.StopCPUProfile()
	}

	f, err := os.Open(*inputFile)

	if err != nil {
		panic(fmt.Sprintf("Failed to read file %v with error: %v", *inputFile, err.Error()))
	}

	defer f.Close()

	compute(f, os.Stdout)

	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			panic("Failed to create Memory Profile file")
		}
		defer f.Close()
		runtime.GC()
		if err := pprof.WriteHeapProfile(f); err != nil {
			panic("Could not start Memory Profiling!")
		}
	}
}

type TemperatureInfo struct {
	minimum int64
	maximum int64
	total   int64
	count   int
}

func compute(r io.Reader, w io.Writer) {
	var res bytes.Buffer
	temperatures := aggregateTemperatures(processChunk(processFile(r)))

	towns := make([]string, 0, len(temperatures))
	for k := range temperatures {
		towns = append(towns, k)
	}
	slices.Sort(towns)

	res.WriteRune('{')
	for i, town := range towns {
		res.WriteString(fmt.Sprintf(
			"%v=%.1f/%.1f/%.1f",
			town,
			float64(temperatures[town].minimum)/10.0,
			float64(temperatures[town].maximum)/10.0,
			(float64(temperatures[town].total)/10.0)/float64(temperatures[town].count),
		))
		if i < len(towns)-1 {
			res.WriteString(", ")
		}
	}
	res.WriteRune('}')
	w.Write(res.Bytes())
}

func processFile(r io.Reader) <-chan []byte {
	chunkStream := make(chan []byte)

	go func() {
		defer close(chunkStream)
		chunkSize := 64 * 1024 * 1024
		data := make([]byte, chunkSize)
		delim := byte('\n')
		leftover := 0
		for {
			n, err := r.Read(data[leftover:])

			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				panic(err)
			}

			dataCopy := data[:leftover+n]
			lastNewLineIndex := bytes.LastIndexByte(dataCopy, delim)
			data = make([]byte, chunkSize)
			leftover = copy(data, dataCopy[lastNewLineIndex+1:])
			chunkStream <- dataCopy[:lastNewLineIndex]
		}
	}()

	return chunkStream
}

func processChunk(chunkStream <-chan []byte) <-chan map[string]*TemperatureInfo {
	tempInfoStream := make(chan map[string]*TemperatureInfo, 100)
	var wg sync.WaitGroup

	go func() {
		defer close(tempInfoStream)
		defer wg.Wait()

		for range runtime.NumCPU() {
			wg.Add(1)

			go func() {
				defer wg.Done()
				for chunk := range chunkStream {
					chunk := string(chunk)
					temperatures := map[string]*TemperatureInfo{}
					start := 0
					lastSemiColon := 0
					var base, fract, temp int64
					var town string

					for i, c := range chunk {
						switch c {
						case ';':
							town = chunk[start:i]
							lastSemiColon = i
							continue
						case '\n':
							start = i + 1
						default:
							continue
						}

						base, _ = strconv.ParseInt(chunk[lastSemiColon+1:i-2], 0, 64)
						fract, _ = strconv.ParseInt(chunk[i-1:i], 0, 64)
						temp = base*10 + fract

						if _, ok := temperatures[town]; !ok {
							temperatures[town] = &TemperatureInfo{
								minimum: 100,
								maximum: -100,
								total:   0,
								count:   0,
							}
						}
						townTempInfo := temperatures[town]
						townTempInfo.minimum = min(townTempInfo.minimum, temp)
						townTempInfo.maximum = max(townTempInfo.maximum, temp)
						townTempInfo.total += temp
						townTempInfo.count++
					}
					tempInfoStream <- temperatures
				}
			}()
		}
	}()

	return tempInfoStream
}

func aggregateTemperatures(tempInfoStream <-chan map[string]*TemperatureInfo) map[string]*TemperatureInfo {
	temperatures := map[string]*TemperatureInfo{}

	for tempInfo := range tempInfoStream {
		for k, v := range tempInfo {
			if curr, ok := temperatures[k]; ok {
				curr.minimum = min(curr.minimum, v.minimum)
				curr.maximum = max(curr.maximum, v.maximum)
				curr.total += v.total
				curr.count += v.count
			} else {
				temperatures[k] = v
			}
		}
	}

	return temperatures
}
