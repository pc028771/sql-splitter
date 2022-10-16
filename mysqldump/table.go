package mysqldump

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"

	"golang.org/x/exp/slices"
)

const FILE_SIZE_LIMIT = 20 * 1024 * 1024
const MERGE_FILE_SIZE_LIMIT = 20 * 1024 * 1024
const targetDomain = "www.herenow.city"

// const targetDomain = "herenow.novize.com.tw"

var AllowedSiteIds = []int{2, 6, 9, 10}
var ErrEndOfTable = errors.New("EndOfTable")

type dml struct {
	Head []string
	Tail []string
	Size int
}

type Table struct {
	Name   string
	DDL    []string
	DML    dml
	Values []*string
	Size   int
	IsDDL  bool
	FQ     *FileQueries
	Files  []*Output
	IsSkip bool
}

func NewTable(name string, query string) *Table {
	t := Table{Name: name, IsDDL: true, IsSkip: false}

	reSiteId, err := regexp.Compile(`hn_(\d+)_`)
	if err != nil {
		panic(err)
	}

	matches := reSiteId.FindStringSubmatch(name)
	if len(matches) == 0 {
		return t.AddDDL(query)
	}

	siteId, _ := strconv.Atoi(matches[1])
	if slices.Contains(AllowedSiteIds, siteId) {
		return t.AddDDL(query)
	}

	t.IsSkip = true
	return &t
}

func (t *Table) AddQuery(query *string) error {
	if len(*query) == 14 && strings.HasPrefix(*query, "UNLOCK TABLES") {
		t.AddDML(query)
		t.IsDDL = true
		return ErrEndOfTable
	}

	if t.IsSkip {
		return nil
	}

	if strings.HasPrefix(*query, "LOCK TABLES") {
		t.IsDDL = false
	}

	if t.IsDDL {
		t.AddDDL(*query)
	} else {
		t.AddDML(query)
	}

	return nil
}

func (t *Table) AddDDL(query string) *Table {
	t.DDL = append(t.DDL, query)
	t.Size += len(query)
	return t
}

func (t *Table) AddDML(query *string) {
	if t.IsSkip {
		return
	}

	if strings.HasPrefix(*query, "INSERT") {
		t.addValue(query)
		return
	}

	t.DML.Size += len(*query)
	if len(t.Values) == 0 {
		t.DML.Head = append(t.DML.Head, *query)
	} else {
		t.DML.Tail = append(t.DML.Tail, *query)
	}
}

func (t *Table) addValue(value *string) {
	t.Size += len(*value)
	t.Values = append(t.Values, value)
}

func (t *Table) Save(fq *FileQueries, index int) (mergeIndex int, err error) {
	t.FQ = fq

	if t.Size+fq.Size > FILE_SIZE_LIMIT {
		err = t.SplitToFiles()
	} else {
		mergeIndex, err = t.AppendToFile(index)
	}

	return mergeIndex, err
}

func (t *Table) SplitToFiles() error {
	fileIndex := 0
	o := NewOutput(t.Name, fileIndex, t.FQ, t.DDL, t.DML)

	for _, value := range t.Values {
		err := o.AddValue(value)

		if err == nil {
			continue
		} else if !errors.Is(err, ErrSizeLimitIsReached) {
			return err
		}

		err = o.WriteToFile()
		if err != nil {
			return err
		}

		t.Files = append(t.Files, o)
		fileIndex++
		o = NewOutput(t.Name, fileIndex, t.FQ, nil, t.DML)
		o.AddValue(value)
	}

	err := o.WriteToFile()
	t.Files = append(t.Files, o)
	return err
}

func (t *Table) AppendToFile(index int) (int, error) {
	filename := fmt.Sprintf("output/merged_file_%d.sql", index)
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return index, err
	}
	defer f.Close()

	fileStat, _ := f.Stat()
	fileSize := int(fileStat.Size())
	isNewFile := fileSize == 0

	if isNewFile {
		fileSize += t.FQ.Size
	}

	if fileSize+t.Size > MERGE_FILE_SIZE_LIMIT {
		for _, q := range t.FQ.Tail {
			f.WriteString(*q + "\n")
		}
		return t.AppendToFile(index + 1)
	}

	if isNewFile {
		for _, q := range t.FQ.Head {
			f.WriteString(*q + "\n")
		}
	}

	for _, q := range t.DDL {
		f.WriteString(q + "\n")
	}

	for _, q := range t.DML.Head {
		f.WriteString(q + "\n")
	}

	replacer := strings.NewReplacer("/stg.herenow.city/", targetDomain, "/cdn.herenow.city/", targetDomain)

	for _, q := range t.Values {
		f.WriteString(replacer.Replace(*q))
		f.WriteString("\n")
	}

	for _, q := range t.DML.Tail {
		f.WriteString(q + "\n")
	}

	// fmt.Println("Writting to", filename)
	return index, nil
}
