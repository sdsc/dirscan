package main

import (
	"bufio"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/karalabe/bufioprop"
)

const LustreNoStripeSize = 10 * 1000000000
const Lustre5StripeSize = 100 * 1000000000
const Lustre10StripeSize = 1000 * 1000000000

func processDir(dir string) error {
	// log.Printf("Started: %s", dir)
	var wg sync.WaitGroup

	absPath, err := filepath.Abs(dir)
	if err != nil {
		return err
	}

	if command == cpCommand.FullCommand() {
		if err = makeDestDir(absPath); err != nil {
			return err
		}
	}

	err = findFolders(absPath, &wg)
	if err != nil {
		return err
	}

	err = findFiles(absPath, &wg)
	if err != nil {
		return err
	}

	wg.Wait()

	// After processing the folder is done, can remove one if needed
	if command == rmCommand.FullCommand() {
		// log.Printf("Deleting dir %s", absPath)
		if err = os.Remove(absPath); err != nil {
			log.Println(err.Error())
		}
	}

	atomic.AddUint64(&processedDirs, uint64(1))
	// log.Printf("Done with: %s", dir)
	return nil
}

func makeDestDir(dir string) error {
	if srcRootDir != dir {
		relPath, err := filepath.Rel(srcRootDir, dir)
		if err != nil {
			return err
		}
		destDir := filepath.Join(destRootDir, relPath)
		srcMeta, err := os.Lstat(dir)
		if err != nil {
			return err
		}

		if destDirMeta, err := os.Lstat(destDir); err == nil { // the dest folder exists
			sourceDirStat := srcMeta.Sys().(*syscall.Stat_t)
			sourceDirUid := int(sourceDirStat.Uid)
			sourceDirGid := int(sourceDirStat.Gid)

			destDirStat := destDirMeta.Sys().(*syscall.Stat_t)
			destDirUid := int(destDirStat.Uid)
			destDirGid := int(destDirStat.Gid)

			if destDirMeta.Mode() != srcMeta.Mode() {
				if err = os.Chmod(destDir, srcMeta.Mode()); err != nil {
					return err
				}
			}

			if sourceDirUid != destDirUid || sourceDirGid != destDirGid {
				if err = os.Lchown(destDir, sourceDirUid, sourceDirGid); err != nil {
					return err
				}
			}
		} else {
			if err = os.Mkdir(destDir, srcMeta.Mode()); err != nil {
				return err
			}
			if err = os.Lchown(destDir, int(srcMeta.Sys().(*syscall.Stat_t).Uid), int(srcMeta.Sys().(*syscall.Stat_t).Gid)); err != nil {
				return err
			}
		}

	}
	return nil
}

func findFolders(dir string, wg *sync.WaitGroup) error {
	dirsChan := make(chan string, 1000001)

	wg.Add(1)
	go func() {
		defer close(dirsChan)
		defer wg.Done()

		cmdDirName := "lfs"
		cmdDirArgs := []string{"find", dir, "-maxdepth", "1", "-type", "d"}

		cmdDir := exec.Command(cmdDirName, cmdDirArgs...)
		cmdDirReader, err := cmdDir.StdoutPipe()
		if err != nil {
			log.Println(err.Error())
			return
		}

		stderr, err := cmdDir.StderrPipe()
		if err != nil {
			log.Println(err.Error())
			return
		}

		scannerDir := bufio.NewScanner(cmdDirReader)

		err = cmdDir.Start()
		if err != nil {
			log.Println(err.Error())
			return
		}

		for scannerDir.Scan() {
			newDir := scannerDir.Text()
			if newDir != dir {
				dirsChan <- newDir
				atomic.AddUint64(&totalDirs, uint64(1))
			}
		}

		slurp, _ := ioutil.ReadAll(stderr)

		if err := cmdDir.Wait(); err != nil {
			log.Printf("%s %s", err.Error(), slurp)
		}
	}()

	for i := 0; i < *dirWorkersFlag; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for dir := range dirsChan {
				err := processDir(dir)
				if err != nil {
					log.Println(err.Error())
					return
				}
			}
		}()
	}
	return nil
}

func findFiles(dir string, wg *sync.WaitGroup) error {
	filesChan := make(chan string, 1000001)

	wg.Add(1)
	go func() {
		defer close(filesChan)
		defer wg.Done()

		cmdFileName := "lfs"
		cmdFileArgs := []string{"find", dir, "-maxdepth", "1", "!", "-type", "d"}

		cmdFile := exec.Command(cmdFileName, cmdFileArgs...)

		cmdFileReader, err := cmdFile.StdoutPipe()
		if err != nil {
			log.Println(err.Error())
			return
		}

		stderr, err := cmdFile.StderrPipe()
		if err != nil {
			log.Println(err.Error())
			return
		}

		scannerFile := bufio.NewScanner(cmdFileReader)

		err = cmdFile.Start()
		if err != nil {
			log.Println(err.Error())
			return
		}

		for scannerFile.Scan() {
			newFile := scannerFile.Text()
			filesChan <- newFile
			atomic.AddUint64(&totalFiles, uint64(1))
		}

		slurp, _ := ioutil.ReadAll(stderr)

		if err := cmdFile.Wait(); err != nil {
			log.Printf("%s %s", err.Error(), slurp)
		}
	}()

	if command == cpCommand.FullCommand() || command == rmCommand.FullCommand() {
		for i := 0; i < *fileWorkersFlag; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for file := range filesChan {

					switch command {
					case rmCommand.FullCommand():
						if err := os.Remove(file); err != nil {
							log.Println(err.Error())
						}
					case cpCommand.FullCommand():
						// log.Printf("Copying: %s", file)
						srcMeta, err := os.Lstat(file)
						if err != nil {
							log.Print("Error reading source file meta ", file, ": ", err.Error())
							continue
						}

						relPath, err := filepath.Rel(srcRootDir, file)
						if err != nil {
							log.Print("Error finding relative path for ", file, ": ", err.Error())
							continue
						}
						destFile := filepath.Join(destRootDir, relPath)

						switch mode := srcMeta.Mode(); {
						case mode.IsRegular():
							if dstMeta, err := os.Lstat(destFile); err == nil { // the dest file exists
								if srcMeta.Size() == dstMeta.Size() &&
									srcMeta.Mode() == dstMeta.Mode() &&
									srcMeta.ModTime() == dstMeta.ModTime() {
									atomic.AddUint64(&processedFiles, uint64(1))
									continue
								} else {
									if err = os.Remove(destFile); err != nil {
										log.Println(err.Error())
										continue
									}
								}
								log.Printf("File %s exists and is modified ", destFile)
							}

							src, err := os.Open(file)
							if err != nil {
								log.Print("Error opening src file ", file, ": ", err.Error())
								continue
							}

							if srcMeta.Size() > LustreNoStripeSize {
								cmdName := "/usr/bin/lfs"
								var cmdArgs []string
								if srcMeta.Size() < Lustre5StripeSize {
									cmdArgs = []string{"setstripe", "-c", "5", destFile}
								} else if srcMeta.Size() < Lustre10StripeSize {
									cmdArgs = []string{"setstripe", "-c", "10", destFile}
								} else {
									cmdArgs = []string{"setstripe", "-c", "50", destFile}
								}

								if _, err = exec.Command(cmdName, cmdArgs...).Output(); err != nil {
									log.Println(err.Error())
									continue
								}
							}

							dest, err := os.OpenFile(destFile, os.O_CREATE|os.O_RDWR|os.O_TRUNC, srcMeta.Mode())
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

							sourceMtime := srcMeta.ModTime()
							sourceAtime := getAtime(srcMeta.Sys().(*syscall.Stat_t))

							if err = os.Lchown(destFile, int(srcMeta.Sys().(*syscall.Stat_t).Uid), int(srcMeta.Sys().(*syscall.Stat_t).Gid)); err != nil {
								log.Println(err.Error())
							}
							if err = os.Chmod(destFile, srcMeta.Mode()); err != nil {
								log.Println(err.Error())
							}
							if err = os.Chtimes(destFile, sourceAtime, sourceMtime); err != nil {
								log.Println(err.Error())
							}

							atomic.AddUint64(&bytes, uint64(bytesCopied))
							// log.Printf("Done copying: %s", file)
						case mode&os.ModeSymlink != 0:
							linkTarget, err := os.Readlink(file)
							if err != nil {
								log.Print("Error reading symlink ", file, ": ", err)
								continue
							}

							if _, err = os.Lstat(destFile); err == nil { // the dest link exists
								if dlinkTarget, err := os.Readlink(destFile); err == nil && dlinkTarget == linkTarget {
									atomic.AddUint64(&processedFiles, uint64(1))
									continue
								}

								if err = os.Remove(destFile); err != nil {
									log.Print("Error removing symlink ", destFile, ": ", err)
									continue
								}
							}

							if err = os.Symlink(linkTarget, destFile); err != nil {
								log.Print("Error seting symlink ", destFile, ": ", err)
								continue
							}
							if err = os.Lchown(destFile, int(srcMeta.Sys().(*syscall.Stat_t).Uid), int(srcMeta.Sys().(*syscall.Stat_t).Gid)); err != nil {
								log.Println(err.Error())
							}
						}
					}
					atomic.AddUint64(&processedFiles, uint64(1))
				}
			}()
		}
	}

	return nil
}
