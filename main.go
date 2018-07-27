package main

import (
	"log"
	"fmt"
	"github.com/ilyail3/inventorySum/sum"
	"os"
)

func acceptAllFilterFunction(record []string) bool {
	return true
}

func main(){
	// os.Args[1:]

	// accounts/834644773037/day/2017/11/13

	// mapInterface, err := sum.AccountMapFunction()
	mapInterface, err := sum.YearMonthMapFunction()

	if err != nil {
		log.Panicf("Failed to prepare a map function:%v\n", err)
	}

	r, err := sum.ProcessFiles(os.Args[1:], acceptAllFilterFunction, mapInterface)

	if err != nil {
		log.Panicf("Process file error:%v", err)
	}

	for _, record := range r.Records {
		fmt.Printf("%s %d\n", record.Key, record.Size)
	}
}