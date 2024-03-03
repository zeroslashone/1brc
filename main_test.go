package main

import (
	"io"
	"testing"
)

type testCharter struct {
	input string
	want  string
	got   string
}

func (tc *testCharter) Read(p []byte) (n int, err error) {
	n = copy(p, []byte(tc.input))
	return n, io.EOF
}

func (tc *testCharter) Write(p []byte) (n int, err error) {
	tc.got = string(p)
	return len(p), nil
}

func TestEval(t *testing.T) {
	tc := &testCharter{}
	tc.input = `Kabala;-10.4
Khrustalnyi;-6.6
Kabala;96.4
Kabala;-11.3`
	tc.want = "{Kabala=-11.3/96.4/24.9, Khrustalnyi=-6.6/-6.6/-6.6}"
	compute(tc, tc)

	if tc.got != tc.want {
		t.Errorf("Got: %s, Want: %s", tc.got, tc.want)
	}
}
