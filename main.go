package main

import (
	"fmt"
	"log"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/Songmu/prompter"
	//https://groups.google.com/forum/#!topic/golang-nuts/Mwn9buVnLmY
	"github.com/dustin/go-humanize"
	"gopkg.in/alecthomas/kingpin.v2"
)

type kv struct {
	Key   string
	Value uint64
}

var (
	summaryFlag = kingpin.Flag("summary", "Only give summary in the end").Default("false").Bool()

	excludeFlag = kingpin.Flag("exclude", "Folder to exclude").Short('x').ExistingDirs()

	fileWorkersFlag = kingpin.Flag("fworkers", "Number of file copy workers per folder").Default("4").Short('w').Int()
	dirWorkersFlag  = kingpin.Flag("dworkers", "Number of dir copy workers per folder").Default("2").Short('e').Int()

	countCommand   = kingpin.Command("count", "Count the number of files and folders")
	countSizeParam = countCommand.Flag("size", "Count the space occupied by files").Short('s').Default("false").Bool()
	countDirParam  = countCommand.Arg("dir", "Folder in Lustre").Required().ExistingDir()

	rmCommand  = kingpin.Command("rm", "DELETE the folder with all its contents")
	rmDirParam = rmCommand.Arg("dir", "Folder in Lustre to delete").Required().ExistingDir()

	cpCommand     = kingpin.Command("cp", "Copy the folder with all its contents")
	cpSourceParam = cpCommand.Arg("source", "Source folder in Lustre").Required().ExistingDir()
	cpTargetParam = cpCommand.Arg("target", "Target folder in Lustre").Required().ExistingDir()

	emptyDirCommand     = kingpin.Command("emp", "Count the empty dirs per account")
	empDirParam = emptyDirCommand.Arg("dir", "The directory to scan").Required().ExistingDir()
	empDirCountFilesParam = emptyDirCommand.Flag("filenum", "Consider folder emoty if has this many files").Short('c').Default("0").Uint64()
)

var (
	totalFiles     uint64
	totalDirs      uint64
	emptyDirs      map[string]uint64 = map[string]uint64{}
	totalData      uint64
	processedFiles uint64
	processedDirs  uint64
	bytes          uint64
)

var srcRootDir string
var destRootDir string

var command string

func main() {
	kingpin.Version("1.0").Author("Dmitry Mishin <dmishin@sdsc.edu>")
	kingpin.CommandLine.Help = "A command-line tool for managing huge data collections in Lustre."
	kingpin.CommandLine.HelpFlag.Short('h')

	command = kingpin.Parse()

	switch command {
	case rmCommand.FullCommand():
		sourceFullPath, err := filepath.Abs(*rmDirParam)
		if err != nil {
			log.Fatalf("Error path: %s", err.Error())
		}
		if !prompter.YN(fmt.Sprintf("Do you really want to DELETE EVERYTHING in %s", sourceFullPath), false) {
			return
		}
	}

	var wg sync.WaitGroup
	quit := make(chan struct{})

	if !*summaryFlag {
		wg.Add(1)
		ticker := time.NewTicker(2 * time.Second)
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
	}

	var err, err1 error
	switch command {
	case rmCommand.FullCommand():
		srcRootDir, err = filepath.Abs(*rmDirParam)
	case cpCommand.FullCommand():
		srcRootDir, err = filepath.Abs(*cpSourceParam)
		destRootDir, err1 = filepath.Abs(*cpTargetParam)
	case countCommand.FullCommand():
		srcRootDir, err = filepath.Abs(*countDirParam)
	case emptyDirCommand.FullCommand():
		srcRootDir, err = filepath.Abs(*empDirParam)
	}
	if err != nil {
		log.Fatalf("Error path: %s", err.Error())
	}
	if err1 != nil {
		log.Fatalf("Error path: %s", err.Error())
	}

	err = processDir(srcRootDir)
	if err != nil {
		log.Printf("Error: %s", err.Error())
	}

	close(quit)
	wg.Wait()

	if *summaryFlag {
		printStatus()
	}

}

func printStatus() {
	switch command {
	case rmCommand.FullCommand():
		fmt.Printf("\rScanned Files = %v; Deleted Files = %v; Scanned Dirs = %v; Deleted Dirs = %v;", totalFiles, processedFiles, totalDirs, processedDirs)
	case cpCommand.FullCommand():
		fmt.Printf("\rScanned Files = %v; Copied Files = %v; Scanned Dirs = %v; Copied Dirs = %v; Bytes transferred = %s", totalFiles, processedFiles, totalDirs, processedDirs, humanize.Bytes(bytes))
	case emptyDirCommand.FullCommand():
		fmt.Printf("%s: Scanned Files = %v; Scanned Dirs = %v\n", *countDirParam, totalFiles, totalDirs)

		var ss []kv
		emptyDirLock.RLock()
		for k, v := range emptyDirs {
			ss = append(ss, kv{k, v})
		}
		emptyDirLock.RUnlock()

		sort.Slice(ss, func(i, j int) bool {
			return ss[i].Value > ss[j].Value
		})

		elms := 10
		if len(ss) < elms {
			elms = len(ss)
		}

		for _, kv := range ss[:elms] {
			fmt.Printf("%s: %d\n", kv.Key, kv.Value)
		}
	case countCommand.FullCommand():
		if *countSizeParam {
			fmt.Printf("\rScanned Files = %v; Scanned Dirs = %v; Data = %s", totalFiles, totalDirs, humanize.Bytes(totalData))
		} else {
			fmt.Printf("\rScanned Files = %v; Scanned Dirs = %v", totalFiles, totalDirs)
		}
	}
}
