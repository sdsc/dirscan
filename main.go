package main

import (
	"fmt"
	"log"
	"path/filepath"
	"sync"
	"time"

	"github.com/Songmu/prompter"
	//https://groups.google.com/forum/#!topic/golang-nuts/Mwn9buVnLmY
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	delFlag         = kingpin.Flag("delete", "DELETE the folder provided WITH ALL ITS CONTENTS").Short('d').Bool()
	cpFlag          = kingpin.Flag("copy", "Copy the folder provided").Short('c').Bool()
	fileWorkersFlag = kingpin.Flag("fworkers", "Number of file copy workers per folder").Default("4").Short('w').Int()
	dirWorkersFlag  = kingpin.Flag("dworkers", "Number of dir copy workers per folder").Default("2").Short('e').Int()
	dirParam        = kingpin.Arg("dir", "Folder in Lustre to process").Required().ExistingDir()
	targetParam     = kingpin.Arg("tdir", "Folder in Lustre to copy to").ExistingDir()
)

var (
	totalFiles     uint64
	totalDirs      uint64
	processedFiles uint64
	processedDirs  uint64
	bytes          uint64
)

var srcDir string
var targetDir string

func main() {
	kingpin.Version("1.0").Author("Dmitry Mishin <dmishin@sdsc.edu>")
	kingpin.CommandLine.Help = "A command-line tool to scan or delete huge data collections in Lustre.\nBy default scans the folder and displays the number of files and folders found."
	kingpin.CommandLine.HelpFlag.Short('h')

	kingpin.Parse()

	var err error
	srcDir, err = filepath.Abs(*dirParam)
	if err != nil {
		fmt.Printf("Error finding the abs src folder: %s", err.Error())
		return
	}
	targetDir, err = filepath.Abs(*targetParam)
	if err != nil {
		fmt.Printf("Error finding the abs target folder: %s", err.Error())
		return
	}

	if *delFlag {
		absPath, err := filepath.Abs(*dirParam)
		if err != nil {
			fmt.Printf("Error finding the folder: %s", err.Error())
			return
		}

		if !prompter.YN(fmt.Sprintf("Do you really want to DELETE EVERYTHING in %s", absPath), false) {
			return
		}
	}

	var wg sync.WaitGroup
	wg.Add(1)
	ticker := time.NewTicker(2 * time.Second)
	quit := make(chan struct{})
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ticker.C:
				printStatus()
			case <-quit:
				ticker.Stop()
				printStatus()
				fmt.Println("")
				return
			}
		}
	}()

	err = processDir(*dirParam)
	if err != nil {
		log.Printf("Error: %s", err.Error())
	}

	close(quit)
	wg.Wait()
}

func printStatus() {
	if *delFlag {
		fmt.Printf("\rScanned Files = %v; Deleted Files = %v; Scanned Dirs = %v; Deleted Dirs = %v;", totalFiles, processedFiles, totalDirs, processedDirs)
	} else if *cpFlag {
		fmt.Printf("\rScanned Files = %v; Copied Files = %v; Scanned Dirs = %v; Copied Dirs = %v; Bytes transferred = %v", totalFiles, processedFiles, totalDirs, processedDirs, bytes)
	} else {
		fmt.Printf("\rScanned Files = %v; Scanned Dirs = %v;", totalFiles, totalDirs)
	}
}
