package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	"runtime/pprof"
	"slices"
	"strconv"
	"strings"
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
	minimum float64
	maximum float64
	total   float64
	count   int
}

func compute(r io.Reader, w io.Writer) {
	s := bufio.NewScanner(r)
	res := []byte{}
	temperatures := map[string]*TemperatureInfo{}
	for s.Scan() {
		datum := strings.Split(s.Text(), ";")
		town := datum[0]
		temp, _ := strconv.ParseFloat(datum[1], 64)

		if _, ok := temperatures[town]; !ok {
			temperatures[town] = &TemperatureInfo{
				minimum: 100.0,
				maximum: -100.0,
				total:   0.0,
				count:   0,
			}
		}
		townTempInfo := temperatures[town]
		townTempInfo.minimum = min(townTempInfo.minimum, temp)
		townTempInfo.maximum = max(townTempInfo.maximum, temp)
		townTempInfo.total += temp
		townTempInfo.count++
	}
	towns := make([]string, 0, len(temperatures))
	for k := range temperatures {
		towns = append(towns, k)
	}
	slices.Sort(towns)

	for i, town := range towns {
		if i == 0 {
			res = append(res, byte('{'))
		} else if i < len(towns) {
			res = append(res, []byte(", ")...)
		}

		res = append(
			res,
			[]byte(fmt.Sprintf(
				"%v=%.1f/%.1f/%.1f",
				town,
				temperatures[town].minimum,
				temperatures[town].maximum,
				math.Round((temperatures[town].total/float64(temperatures[town].count))*10)/10,
			))...,
		)
		if i == len(towns)-1 {
			res = append(res, byte('}'))
		}
	}

	w.Write(res)
}
