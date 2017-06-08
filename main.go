package main

import (
	"bufio"
	"gopkg.in/alecthomas/kingpin.v2"
	"log"
	"fmt"
	"os"
	"strings"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

var (
	delFlag  = kingpin.Flag("delete", "DELETE the folder provided WITH ALL ITS CONTENTS").Short('d').Bool()
	dirParam = kingpin.Arg("dir", "Folder in Lustre to process").Required().ExistingDir()
)

var (
	totalFiles     uint64 = 0
	totalDirs      uint64 = 0
	processedFiles uint64 = 0
	processedDirs  uint64 = 0
)

func processDir(dir string) error {
	dirsChan := make(chan string, 1000001)
	filesChan := make(chan string, 1000001)

	var wg sync.WaitGroup

	absPath, err := filepath.Abs(dir)
	if err != nil {
		return err
	}

	cmdDirName := "lfs"
	cmdDirArgs := []string{"find", absPath, "-maxdepth", "1", "-type", "d"}

	cmdDir := exec.Command(cmdDirName, cmdDirArgs...)
	cmdDirReader, err := cmdDir.StdoutPipe()
	if err != nil {
		log.Print("Error running lustre find: %v", err)
		return err
	}

	scannerDir := bufio.NewScanner(cmdDirReader)

	err = cmdDir.Start()
	if err != nil {
		return err
	}

	wg.Add(1)
	go func() {
		defer close(dirsChan)
		defer wg.Done()

		for scannerDir.Scan() {
			newDir := scannerDir.Text()
			if newDir != absPath {
				dirsChan <- newDir
				atomic.AddUint64(&totalDirs, uint64(1))
			}
		}
		cmdDir.Wait()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for dir := range dirsChan {
			err := processDir(dir)
			if err != nil {
				log.Print("Error processing folder %s: %v", dir, err)
			}
		}
	}()

	cmdFileName := "lfs"
	cmdFileArgs := []string{"find", absPath, "-maxdepth", "1", "!", "-type", "d"}

	cmdFile := exec.Command(cmdFileName, cmdFileArgs...)
	cmdFileReader, err := cmdFile.StdoutPipe()
	if err != nil {
		log.Print("Error running lustre find: %v", err)
		return err
	}

	scannerFile := bufio.NewScanner(cmdFileReader)

	err = cmdFile.Start()
	if err != nil {
		return err
	}

	wg.Add(1)
	go func() {
		defer close(filesChan)
		defer wg.Done()

		for scannerFile.Scan() {
			newFile := scannerFile.Text()
			filesChan <- newFile
			atomic.AddUint64(&totalFiles, uint64(1))
		}
		cmdFile.Wait()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for file := range filesChan {
			if *delFlag {
				// log.Printf("Deleting file %s", file)
				os.Remove(file)
			}
			atomic.AddUint64(&processedFiles, uint64(1))
		}
	}()

	wg.Wait()

	if *delFlag {
		// log.Printf("Deleting dir %s", absPath)
		os.Remove(absPath)
	}

	atomic.AddUint64(&processedDirs, uint64(1))
	return nil
}

func main() {
	kingpin.Version("1.0").Author("Dmitry Mishin <dmishin@sdsc.edu>")
	kingpin.CommandLine.Help = "A command-line tool to scan or delete huge data collections in Lustre.\nBy default scans the folder and displays the number of files and folders found."
	kingpin.CommandLine.HelpFlag.Short('h')

	kingpin.Parse()

	if *delFlag {
		absPath, err := filepath.Abs(*dirParam)
		if err != nil {
			fmt.Printf("Error finding the folder: %s", err)
			return
		}

		if ! askForConfirmation(fmt.Sprintf("Do you really want to DELETE EVERYTHING in %s", absPath)) {
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
	        case <- ticker.C:
	        	printStatus()
	        case <- quit:
	            ticker.Stop()
	        	printStatus()
	        	fmt.Println("")
	            return
	        }
	    }
	 }()

	err := processDir(*dirParam)
	if err != nil {
		log.Printf("Error: %s", err)
	}

	close(quit)
	wg.Wait()
}

func printStatus() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("\rAlloc Mem = %vKB; Total Files = %v; Processed Files = %v; Total Dirs = %v; Processed Dirs = %v;", m.Alloc/1024, totalFiles, processedFiles, totalDirs, processedDirs)
}

func askForConfirmation(s string) bool {
	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Printf("%s [yes/no]: ", s)

		response, err := reader.ReadString('\n')
		if err != nil {
			log.Fatal(err)
		}

		response = strings.ToLower(strings.TrimSpace(response))

		if response == "yes" {
			return true
		} else if response == "no" {
			return false
		}
	}
}
