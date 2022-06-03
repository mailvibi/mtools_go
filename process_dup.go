package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

func find_orig_by_match(f1 string, f2 string, match string) (of string, df string, res bool) {
	res = false
	if strings.Contains(f1, match) && strings.Contains(f2, match) {
		fmt.Printf("Both file has %s to be decided how to proceed with identifying which is original\n", match)
		return
	}
	if strings.Contains(f1, match) {
		df = f1
		of = f2
		res = true
	} else if strings.Contains(f2, match) {
		df = f2
		of = f1
		res = true
	} else {
		//		fmt.Printf("Did not find >>%s<< - unable to identify orig and dup file from <%s> && <%s>\n", match, f1, f2)
	}
	return
}

func move_dup_files(wg *sync.WaitGroup, file string, sdir string, ddir string) {
	defer wg.Done()
	sf := filepath.Join(sdir, file)
	df := filepath.Join(ddir, file)
	err := os.Rename(sf, df)
	if err != nil {
		fmt.Println("Error Moving file from ", sf, " to ", df)
	}
	return
}

func main() {
	logfile := flag.String("logfile", "", "File which contains the duplicate file information")
	dupdir := flag.String("dupdir", "", "Directory to which duplicate files should be moved")
	flag.Parse()
	if *logfile == "" || *dupdir == "" {
		fmt.Println("Invalid arguments")
		return
	}
	o_files := make(map[string]bool)
	d_files := make(map[string]bool)
	track_o_n_d := func(o string, d string) {
		fmt.Printf("Original file -> %s , Duplicate file -> %s\n", o, d)
		if _, k := d_files[d]; k {
			fmt.Printf("duplicate file %s already exists in map\n", d)
		} else {
			d_files[d] = true
		}
		if _, k := o_files[o]; k {
			fmt.Printf("original file %s already exists in map\n", o)
		} else {
			o_files[o] = true
		}
	}
	lf, err := os.Open(*logfile)
	if err != nil {
		fmt.Println("Error in opening logfile - ", logfile, " -- ", err)
		return
	}
	defer lf.Close()
	lfscanner := bufio.NewScanner(lf)
	lfscanner.Split(bufio.ScanLines)
	for lfscanner.Scan() {
		line := lfscanner.Text()
		fmt.Println("Processing ------> ", line)
		sl := strings.Split(line, "|")
		if len(sl) != 2 {
			fmt.Println("Error len of splitted string not equal to 2 | len ->", len(sl), ", String ->", sl)
			return
		}
		d1, f1 := filepath.Split(sl[0])
		d2, f2 := filepath.Split(sl[1])

		if d1 != d2 {
			fmt.Printf("Base Directories not matching -> [%s] & [%s]\n", d1, d2)
			fmt.Println(sl[1], " will be considered as duplicate")
			// Temporary rule - mark the second file as dup
			track_o_n_d(sl[0], sl[1])
			continue
		}

		for _, m := range []string{"(", "Copy", "_"} {
			of, df, ok := find_orig_by_match(f1, f2, m)
			if ok {
				track_o_n_d(filepath.Join(d1, of), filepath.Join(d1, df))
				break
			}
		}
	}
	fmt.Println("Original Files -> ", o_files)
	fmt.Println("Duplicate Files -> ", d_files)
	var wg sync.WaitGroup
	for k, _ := range d_files {
		wg.Add(1)
		b := filepath.Base(k)
		sd := filepath.Dir(k)
		fmt.Println(k, b, sd)
		go move_dup_files(&wg, b, sd, *dupdir)
	}
	wg.Wait()
}
