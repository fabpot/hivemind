package main

import (
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

var colors = []int{2, 3, 4, 5, 6, 42, 130, 103, 129, 108}

type hivemindConfig struct {
	Title              string
	Procfile           string
	ProcNames          string
	Root               string
	PortBase, PortStep int
	Timeout            int
	NoPrefix           bool
	PrintTimestamps    bool
}

type hivemind struct {
	title       string
	output      *multiOutput
	root        string
	entries     []procfileEntry
	procs       []*process
	procWg      sync.WaitGroup
	done        chan int
	interrupted chan os.Signal
	timeout     time.Duration
}

func newHivemind(conf hivemindConfig) (h *hivemind) {
	h = &hivemind{timeout: time.Duration(conf.Timeout) * time.Second}

	if len(conf.Title) > 0 {
		h.title = conf.Title
	} else {
		h.title = filepath.Base(conf.Root)
	}

	h.root = conf.Root
	h.output = &multiOutput{printProcName: !conf.NoPrefix, printTimestamp: conf.PrintTimestamps}
	h.entries = parseProcfile(conf.Procfile, conf.PortBase, conf.PortStep, splitAndTrim(conf.ProcNames))
	h.procs = make([]*process, len(h.entries))

	return
}

func (h *hivemind) runProcess(i int) {
	h.procWg.Add(1)

	go func(i int) {
		defer h.procWg.Done()
		defer func() { h.done <- i }()

		entry := h.entries[i]
		h.procs[i] = newProcess(entry.Name, entry.Command, colors[i%len(colors)], h.root, entry.Port, h.output)
		h.procs[i].Run()
	}(i)
}

func (h *hivemind) waitForDoneOrInterrupt() {
forever:
	select {
	case i := <-h.done:
		// restart only if the process finished with an error
		if h.procs[i].ProcessState.ExitCode() > 0 {
			h.output.WriteLine(h.procs[i], []byte("\033[1mRestarting...\033[0m"))
			time.Sleep(1 * time.Second)
			h.runProcess(i)
		}
		goto forever
	case <-h.interrupted:
	}
}

func (h *hivemind) waitForTimeoutOrInterrupt() {
	select {
	case <-time.After(h.timeout):
	case <-h.interrupted:
	}
}

func (h *hivemind) waitForExit() {
	h.waitForDoneOrInterrupt()

	for _, proc := range h.procs {
		go proc.Interrupt()
	}

	h.waitForTimeoutOrInterrupt()

	for _, proc := range h.procs {
		go proc.Kill()
	}
}

func (h *hivemind) Run() {
	fmt.Printf("\033]0;%s | hivemind\007", h.title)

	h.done = make(chan int, len(h.procs))

	h.interrupted = make(chan os.Signal)
	signal.Notify(h.interrupted, syscall.SIGINT, syscall.SIGTERM)

	for i := range h.procs {
		h.runProcess(i)
	}

	go h.waitForExit()

	h.procWg.Wait()
}

// FIXME: Instead of killing everything when one process dies, we should only rerun the died one.
