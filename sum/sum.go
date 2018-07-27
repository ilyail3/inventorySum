package sum

import (
	"os"
	"log"
	"strings"
	"compress/gzip"
	"fmt"
	"encoding/csv"
	"io"
	"strconv"
	"sort"
)

type MapInterface interface {
	mapRecord(record []string)(interface{}, error)
}

type ResultRecord struct {
	Key string
	Size uint64
}

type SortedResult struct {
	Records []ResultRecord
}

func (sr *SortedResult) Len() int {
	return len(sr.Records)
}

func (sr *SortedResult) Less(i, j int) bool{
	return strings.Compare(sr.Records[i].Key, sr.Records[j].Key) < 0
}

func (sr *SortedResult) Swap(i, j int) {
	sr.Records[i], sr.Records[j] = sr.Records[j], sr.Records[i]
}

func ProcessFile(filename string, filterFunction func([]string)bool, mapFunction MapInterface, result *map[interface{}]uint64) error{
	log.Printf("file %s\n", filename)

	rawReader, err := os.Open(filename)

	if err != nil {
		return fmt.Errorf("failed to read file:%v", err)

	}

	defer rawReader.Close()

	var reader io.ReadCloser = rawReader

	if strings.HasSuffix(filename, ".gz") {
		reader, err = gzip.NewReader(rawReader)

		if err != nil {
			return fmt.Errorf("failed to open gzip file:%v", err)
		}

		defer reader.Close()
	}

	csvReader := csv.NewReader(reader)
	csvReader.FieldsPerRecord = 5
	csvReader.Comma = ','

	for {
		record, err := csvReader.Read()

		if err == io.EOF {
			return nil
		}

		if err != nil {
			return fmt.Errorf("failed to read record:%v", err)
		}

		if filterFunction(record) {
			key, err := mapFunction.mapRecord(record)

			if err != nil {
				return fmt.Errorf("failed to map record:%v error:%v", record, err)
			}

			current, exists := (*result)[key]

			if !exists {
				current = 0
			}

			size, err := strconv.ParseUint(record[2],10, 64)

			if err != nil {
				return fmt.Errorf("encountered invalid size for record %v, size:%s, err:%v", record, record[2], err)
			}

			(*result)[key] = current + size
		}

		// fmt.Printf("record is:%v", record)
	}
}

func ProcessFiles(fileNames []string, filterFunction func([]string)bool, mapFunction MapInterface) (SortedResult, error){
	r := SortedResult{Records:make([]ResultRecord, 0)}
	result := make(map[interface{}]uint64)

	for _, file := range fileNames {
		err := ProcessFile(file, filterFunction, mapFunction, &result)

		if err != nil {
			return r, fmt.Errorf("error processing file:%s, error:%v", file, err)
		}
	}

	for key, num := range result {
		//fmt.Printf("%v %d\n", key, num)
		if key != nil {
			r.Records = append(r.Records, ResultRecord{
				Key: fmt.Sprintf("%v", key),
				Size: num })
		}
	}

	sort.Sort(&r)

	return r, nil
}
