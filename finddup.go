package main

import (
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"
)

const (
	READ_BLK_SIZE          = 1024 * 1024 * 10
	HASH_WORKERS           = 10
	HASH_WORKER_OP_CHAN_SZ = 100
	HASH_WORKER_IP_CHAN_SZ = 100
)

type hinfo struct {
	hash string
	file string
}

func hash_worker(wrk_ip_chan <-chan string, sc chan<- hinfo) {
	for file := range wrk_ip_chan {
		hf, e := os.Open(file)
		if e != nil {
			fmt.Println("Error in opening file ", file)
			return
		}
		defer hf.Close()

		h := sha256.New()
		rb := make([]byte, READ_BLK_SIZE)
		for {
			rl, err := hf.Read(rb)
			if err != nil {
				break
			}
			h.Write(rb[:rl])
		}
		hs := h.Sum(nil)
		hstr := hex.EncodeToString(hs)
		//fmt.Println("Hash ", hstr, "for file ", file)
		sc <- hinfo{hstr, file}
	}
}

func log_dup(scandir string, logfile string) error {
	filecount := 0
	hashcount := 0
	logf, err := os.Create(logfile)
	if err != nil {
		fmt.Println("Error in opening file : ", logfile)
		return err
	}
	defer logf.Close()
	/* channel to send work to the worker routines */
	hwo := make(chan hinfo, HASH_WORKER_OP_CHAN_SZ)
	hwi := make(chan string, HASH_WORKER_IP_CHAN_SZ)

	hmap := make(map[string]string)
	go func(hc <-chan hinfo) {
		for {
			hi := <-hc
			hstr := hi.hash
			//fmt.Println("Received Hash info for ", hi.file, " Hash = ", hstr)
			if f, k := hmap[hstr]; k {
				fmt.Fprintf(logf, "%s|%s\n", hi.file, f)
				//				fmt.Println("Hash[ ", hstr, "] of file ", hi.file, " already present for filename ", f)
			} else {
				hmap[hstr] = hi.file
			}
			hashcount++
		}
	}(hwo)

	for i := 0; i < HASH_WORKERS; i++ {
		go hash_worker(hwi, hwo)
	}
	filepath.WalkDir(scandir, func(f string, d fs.DirEntry, err error) error {
		if err != nil {
			fmt.Println("Scanning of ", f, "/", d, "failed !!!")
			return err
		}
		if d.IsDir() {
			return nil
		}
		filecount++
		hwi <- f
		return nil
	})
	for filecount != hashcount {
		time.Sleep(5)
	}
	return nil
}

func main() {
	scandir := flag.String("srcdir", "", "Source directory which has to be scaned")
	logfile := flag.String("logfile", "", "Log of duplicate files")
	flag.Parse()
	if *scandir == "" || *logfile == "" {
		fmt.Println("Invalid Arguments")
		flag.PrintDefaults()
		return
	}
	st := time.Now()
	fmt.Println("Start Time : ", st)
	log_dup(*scandir, *logfile)
	et := time.Now()
	fmt.Println("End Time : ", et)
	fmt.Println("Time Diff : ", et.Sub(st))
	return
}
