// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package dskv

import (
	"fmt"
	"math"
	"math/rand"
	"time"

	"util/log"
	"golang.org/x/net/context"
)

const (
	// NoJitter makes the backoff sequence strict exponential.
	NoJitter = 1 + iota
	// FullJitter applies random factors to strict exponential.
	FullJitter
	// EqualJitter is also randomized, but prevents very short sleeps.
	EqualJitter
	// DecorrJitter increases the maximum jitter based on the last random value.
	DecorrJitter
)

// NewBackoffFn creates a backoff func which implements exponential backoff with
// optional jitters.
// See http://www.awsarchitectureblog.com/2015/03/backoff.html
func NewBackoffFn(base, cap, jitter int) func() int {
	attempts := 0
	lastSleep := base
	return func() int {
		var sleep int
		switch jitter {
		case NoJitter:
			sleep = expo(base, cap, attempts)
		case FullJitter:
			v := expo(base, cap, attempts)
			sleep = rand.Intn(v)
		case EqualJitter:
			v := expo(base, cap, attempts)
			sleep = v/2 + rand.Intn(v/2)
		case DecorrJitter:
			sleep = int(math.Min(float64(cap), float64(base+rand.Intn(lastSleep*3-base))))
		}
		time.Sleep(time.Duration(sleep) * time.Millisecond)

		attempts++
		lastSleep = sleep
		return lastSleep
	}
}

func expo(base, cap, n int) int {
	return int(math.Min(float64(cap), float64(base)*math.Pow(2.0, float64(n))))
}

type backoffType int

const (
	BoKVRPC      backoffType = iota
	BoMSRPC
	BoCacheLoad
	BoRangeMiss
	BoServerBusy
)

func (t backoffType) createFn() func() int {
	switch t {
	case BoKVRPC:
		return NewBackoffFn(100, 2000, EqualJitter)
	case BoMSRPC:
		return NewBackoffFn(300, 2000, EqualJitter)
	case BoCacheLoad:
		return NewBackoffFn(500, 3000, EqualJitter)
	case BoRangeMiss:
		return NewBackoffFn(100, 500, NoJitter)
	case BoServerBusy:
		return NewBackoffFn(2000, 10000, EqualJitter)
	}
	return nil
}

func (t backoffType) String() string {
	switch t {
	case BoKVRPC:
		return "dsRPC"
	case BoMSRPC:
		return "msRPC"
	case BoCacheLoad:
		return "cacheLoad"
	case BoRangeMiss:
		return "rangeMiss"
	case BoServerBusy:
		return "serverBusy"
	}
	return ""
}

// Maximum total sleep time(in ms) for kv commands.
const (
	MsMaxBackoff          = 5000
	ScannerNextMaxBackoff = 20000
	InsertMaxBackoff      = 5000
	GetMaxBackoff         = 20000
	RawkvMaxBackoff       = 20000
)

var commitMaxBackoff = 20000

// Backoffer is a utility for retrying queries.
type Backoffer struct {
	fn         map[backoffType]func() int
	maxSleep   int
	totalSleep int
	errors     []error
	ctx        context.Context
	types      []backoffType
}

// NewBackoffer creates a Backoffer with maximum sleep time(in ms).
func NewBackoffer(maxSleep int, ctx context.Context) *Backoffer {
	return &Backoffer{
		maxSleep: maxSleep,
		ctx:      ctx,
	}
}

// Backoff sleeps a while base on the backoffType and records the error message.
// It returns a retryable error if total sleep time exceeds maxSleep.
func (b *Backoffer) Backoff(typ backoffType, err error) error {
	select {
	case <-b.ctx.Done():
		return err
	default:
	}

	// todo metric

	// Lazy initialize.
	if b.fn == nil {
		b.fn = make(map[backoffType]func() int)
	}
	f, ok := b.fn[typ]
	if !ok {
		f = typ.createFn()
		b.fn[typ] = f
	}

	b.totalSleep += f()
	b.types = append(b.types, typ)

	log.Debug("%v, retry later(totalSleep %d ms, maxSleep %d ms)", err, b.totalSleep, b.maxSleep)
	b.errors = append(b.errors, err)
	if b.maxSleep > 0 && b.totalSleep >= b.maxSleep {
		errMsg := fmt.Sprintf("backoffer.maxSleep %dms is exceeded, errors:", b.maxSleep)
		for i, err := range b.errors {
			// Print only last 3 errors for non-DEBUG log levels.
			if log.GetFileLogger().IsEnableDebug() || i >= len(b.errors)-3 {
				errMsg += "\n" + err.Error()
			}
		}
		log.Warn(errMsg)
		return ErrRetryLater
	}
	return nil
}

func (b *Backoffer) CombineTime(sleep int) {
	b.totalSleep += sleep
}

func (b *Backoffer) String() string {
	if b.totalSleep == 0 {
		return ""
	}
	return fmt.Sprintf("backoff(%dms %s)", b.totalSleep, b.types)
}

// Clone creates a new Backoffer which keeps current Backoffer's sleep time and errors, and shares
// current Backoffer's context.
func (b *Backoffer) Clone() *Backoffer {
	return &Backoffer{
		maxSleep:   b.maxSleep,
		totalSleep: b.totalSleep,
		errors:     b.errors,
		ctx:        b.ctx,
	}
}

// Fork creates a new Backoffer which keeps current Backoffer's sleep time and errors, and holds
// a child context of current Backoffer's context.
func (b *Backoffer) Fork() (*Backoffer, context.CancelFunc) {
	ctx, cancel := context.WithCancel(b.ctx)
	return &Backoffer{
		maxSleep:   b.maxSleep,
		totalSleep: b.totalSleep,
		errors:     b.errors,
		ctx:        ctx,
	}, cancel
}
