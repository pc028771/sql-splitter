package mysqldump

import (
	"bufio"
)

func ReadFullLine(r *bufio.Reader) (line string, err error) {
	buffer := []byte{}

	for {
		bytes, isPrefix, err := r.ReadLine()

		if nil != err {
			return "", err
		}

		if !isPrefix && len(buffer) == 0 {
			return string(bytes), nil
		}

		buffer = append(buffer, bytes...)

		if !isPrefix {
			return string(buffer), nil
		}
	}
}
