package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"novize/splitter/mysqldump"
	"os"
	"regexp"
	"sort"
	"strings"
)

func splitFile(filename string) {
	sqlFile, err := os.Open(filename)
	if nil != err {
		panic(err)
	}

	var (
		rd = bufio.NewReader(sqlFile)
		fq = mysqldump.FileQueries{IsAddingHead: true}

		tables = make(map[string]*mysqldump.Table)

		currentTable = ""
	)

	regexTableName, err := regexp.Compile("^DROP TABLE IF EXISTS `(.*)`;")
	if err != nil {
		panic(err)
	}

	for {
		line, err := mysqldump.ReadFullLine(rd)

		if errors.Is(err, io.EOF) {
			break
		} else if nil != err {
			panic(err)
		}

		if line == "" || strings.HasPrefix(line, "--") {
			continue
		}

		if strings.HasPrefix(line, "DROP TABLE") {
			matches := regexTableName.FindStringSubmatch(line)
			if len(matches) == 0 {
				panic("Drop table query match failed")
			}
			currentTable = matches[1]
			tables[currentTable] = mysqldump.NewTable(currentTable, line)
			fq.IsAddingHead = false
			continue
		}

		if currentTable == "" {
			fq.AddQuery(&line)
			continue
		}

		err = tables[currentTable].AddQuery(&line)
		if errors.Is(err, mysqldump.ErrEndOfTable) {
			currentTable = ""
		}
	}

	fileStat, _ := sqlFile.Stat()
	fmt.Println(fileStat.Name(), " size ", fileStat.Size())

	sorts := []struct {
		Name string
		Size int
	}{}

	for name, t := range tables {
		sorts = append(sorts, struct {
			Name string
			Size int
		}{Name: name, Size: t.Size})
	}

	sort.SliceStable(sorts, func(i, j int) bool {
		return sorts[i].Size > sorts[j].Size
	})

	// DebugPrint(sorts)
	mergeIndex := 0
	for _, t := range tables {
		mergeIndex, err = t.Save(&fq, mergeIndex)
		if err != nil {
			panic(err)
		}
	}
}

func main() {
	filename := "hn-stage.sql"
	if len(os.Args[1:]) > 0 {
		filename = os.Args[1:][0]
	}

	splitFile(filename)
}

func DebugPrint(sorts interface{}) {
	b, err := json.MarshalIndent(sorts, "", "  ")
	if err != nil {
		fmt.Println("error:", err)
	}
	fmt.Print(string(b))
}
