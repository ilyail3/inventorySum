package main

import (
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/ilyail3/inventorySum/sum"
	"log"
)

func acceptAllFilterFunction(record []string) bool {
	return true
}

const BUCKET = "dpcloudtrail-production-archive"
const PREFIX = "Inventory/dpcloudtrail-production-archive/inventory/"

func main() {

	awsProfileFile := flag.String("aws-profile-file", "", "AWS profile file")
	awsProfile := flag.String("aws-profile", "", "AWS profile name")
	regionFlag := flag.String("region", "", "AWS Region")

	flag.Parse()

	conf := aws.Config{
		Region: regionFlag,
		S3DisableContentMD5Validation: aws.Bool(true)}

	if *awsProfile != "" {
		conf.WithCredentials(credentials.NewSharedCredentials(*awsProfileFile, *awsProfile))
	}

	// accounts/834644773037/day/2017/11/13

	// mapInterface, err := sum.AccountMapFunction()
	mapInterface, err := sum.YearMonthMapFunction()

	if err != nil {
		log.Panicf("Failed to prepare a map function:%v\n", err)
	}

	r, err := sum.S3Read(conf, BUCKET, PREFIX, acceptAllFilterFunction, mapInterface)

	if err != nil {
		log.Panicf("Process file error:%v", err)
	}

	for _, record := range r.Records {
		fmt.Printf("%s %d\n", record.Key, record.Size)
	}
}
