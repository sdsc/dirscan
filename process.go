package main

import (
	"bufio"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/karalabe/bufioprop"
)

func processDir(dir string) error {
	log.Printf("Started: %s", dir)
	dirsChan := make(chan string, 1000001)
	filesChan := make(chan string, 1000001)

	var wg sync.WaitGroup

	absPath, err := filepath.Abs(dir)
	if err != nil {
		return err
	}

	if srcDir != absPath {
		relPath, err := filepath.Rel(srcDir, dir)
		if err != nil {
			log.Print("Error finding relative path for ", dir, ": ", err.Error())
			return err
		}
		destDir := filepath.Join(*targetParam, relPath)
		srcMeta, err := os.Lstat(absPath)
		if err != nil {
			log.Print("Error reading source folder meta ", absPath, ": ", err.Error())
			return err
		}

		os.Mkdir(destDir, srcMeta.Mode())
	}

	cmdDirName := "lfs"
	cmdDirArgs := []string{"find", absPath, "-maxdepth", "1", "-type", "d"}

	cmdDir := exec.Command(cmdDirName, cmdDirArgs...)
	cmdDirReader, err := cmdDir.StdoutPipe()
	if err != nil {
		log.Printf("Error running lustre find: %v", err.Error())
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

	for i := 0; i < *dirWorkersFlag; i++ {
		go func() {
			wg.Add(1)
			defer wg.Done()
			for dir := range dirsChan {
				err = processDir(dir)
				if err != nil {
					log.Printf("Error processing folder %s: %v", dir, err.Error())
				}
			}
		}()
	}

	cmdFileName := "lfs"
	cmdFileArgs := []string{"find", absPath, "-maxdepth", "1", "!", "-type", "d"}

	cmdFile := exec.Command(cmdFileName, cmdFileArgs...)
	cmdFileReader, err := cmdFile.StdoutPipe()
	if err != nil {
		log.Printf("Error running lustre find: %v", err.Error())
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

	for i := 0; i < *fileWorkersFlag; i++ {
		go func() {
			wg.Add(1)
			defer wg.Done()
			for file := range filesChan {
				if *delFlag {
					os.Remove(file)
				} else if *cpFlag {
					log.Printf("Copying: %s", file)
					srcMeta, err := os.Lstat(file)
					if err != nil {
						log.Print("Error reading source file meta ", file, ": ", err.Error())
						continue
					}

					switch mode := srcMeta.Mode(); {
					case mode.IsRegular():
						src, err := os.Open(file)
						if err != nil {
							log.Print("Error opening src file ", file, ": ", err.Error())
							continue
						}

						relPath, err := filepath.Rel(srcDir, file)
						if err != nil {
							log.Print("Error finding relative path for ", file, ": ", err.Error())
							continue
						}
						destFile := filepath.Join(*targetParam, relPath)
						dest, err := os.Create(destFile)
						if err != nil {
							log.Print("Error opening dst file ", file, ": ", err.Error())
							src.Close()
							continue
						}
						bytesCopied, err := bufioprop.Copy(dest, src, 1048559)
						if err != nil {
							log.Print("Error copying file ", file, ": ", err.Error())
							src.Close()
							dest.Close()
							continue
						}
						src.Close()
						dest.Close()
						atomic.AddUint64(&bytes, uint64(bytesCopied))
						log.Printf("Done copying: %s", file)
					}
				}
				atomic.AddUint64(&processedFiles, uint64(1))
			}
		}()
	}

	wg.Wait()

	if *delFlag {
		// log.Printf("Deleting dir %s", absPath)
		os.Remove(absPath)
	}

	atomic.AddUint64(&processedDirs, uint64(1))
	log.Printf("Done with: %s", dir)
	return nil
}
