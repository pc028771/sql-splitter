package mysqldump

import (
	"errors"
	"fmt"
	"os"
	"strings"
)

type FileQueries struct {
	Index        int
	Head         []*string
	Tail         []*string
	Size         int
	IsAddingHead bool
}

func (fq *FileQueries) AddQuery(query *string) {
	fq.Size += len(*query)

	if fq.IsAddingHead {
		fq.Head = append(fq.Head, query)
	} else {
		fq.Tail = append(fq.Tail, query)
	}
}

var ErrSizeLimitIsReached = errors.New("EndOfTable")

type Output struct {
	Filename string
	Head     []string
	Tail     []string
	Size     int
	Values   []string
}

func NewOutput(table string, idx int, fq *FileQueries, ddl []string, dml dml) *Output {
	o := Output{
		Filename: fmt.Sprint("output/", table, "_", idx, ".sql"),
		Size:     0,
	}

	for _, q := range (*fq).Head {
		o.Size += len(*q)
		o.Head = append(o.Head, *q)
		// fmt.Println(v)
	}

	for _, q := range ddl {
		o.Size += len(q)
		o.Head = append(o.Head, q)
		// fmt.Println(v)
	}

	for _, q := range dml.Head {
		v := string(q)
		o.Size += len(q)
		o.Head = append(o.Head, v)
		// fmt.Println(v)
	}

	for _, q := range dml.Tail {
		v := string(q)
		o.Size += len(q)
		o.Tail = append(o.Tail, v)
		// fmt.Println(v)
	}

	for _, q := range (*fq).Tail {
		o.Size += len(*q)
		o.Tail = append(o.Tail, *q)
		// fmt.Println(v)
	}

	return &o
}

func (o *Output) AddValue(value *string) error {
	str := strings.ReplaceAll(*value, "www.herenow.city", "herenow.novize.com.tw")
	str = strings.ReplaceAll(str, "stg.herenow.city", "herenow.novize.com.tw")

	if o.Size+len(str) > FILE_SIZE_LIMIT {
		return ErrSizeLimitIsReached
	}

	o.Values = append(o.Values, str)
	o.Size += len(str)
	return nil
}

func (o *Output) WriteToFile() error {
	// fmt.Println("Writting to", o.Filename)
	f, err := os.Create(o.Filename)
	if err != nil {
		return err
	}

	defer f.Close()

	for _, str := range o.Head {
		f.WriteString(str + "\n")
	}

	for _, str := range o.Values {
		f.WriteString(str)
		f.WriteString("\n")
	}

	for _, str := range o.Tail {
		f.WriteString(str + "\n")
	}

	return nil
}
