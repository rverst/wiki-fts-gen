package main

import (
	"fmt"
	"github.com/integrii/flaggy"
	wikiFtsGen "github.com/rverst/wiki-fts-gen"
	"log"
	"os"
)

var (
	version = "dev"
)

func main() {

	flaggy.SetVersion(version)

	var dir string
	var file string
	flaggy.String(&dir, "d", "directory", "Directory with the multistream dump files")
	flaggy.String(&file, "f", "file", "A single multistream dump file")

	flaggy.Parse()

	var files []string

	if file != "" {
		fi, err := os.Stat(file)
		if err != nil {
			log.Fatal(err)
		}

		if fi.IsDir() {
			log.Fatal(fmt.Errorf("file expected, not a direcory: %s", fi.Name()))
		}
		files = make([]string, 1)
		files[0] = file
	} else if dir != "" {
		fi, err := os.Stat(file)
		if err != nil {
			log.Fatal(err)
		}

		if !fi.IsDir() {
			log.Fatal(fmt.Errorf("directory expected, not a file: %s", fi.Name()))
		}
	} else {
		flaggy.ShowHelpAndExit("directory or file expected")
	}

	ch := make(chan wikiFtsGen.Doc, 4000)
	go func() {
		err := wikiFtsGen.Generate(files, 60000, 160000, ch)
		if err != nil {
			log.Fatal(err)
		}
	}()

	c := 0
	for t := range ch {
			fmt.Println(t)
		c++
	}




}
