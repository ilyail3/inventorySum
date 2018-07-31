package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/ilyail3/inventorySum/sum"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

func acceptAllFilterFunction(record []string) bool {
	return true
}

const BUCKET = "dpcloudtrail-production-archive"
const PREFIX = "Inventory/dpcloudtrail-production-archive/inventory/"

type ManifestFile struct {
	Key         string `json:"key"`
	Size        uint64 `json:"size"`
	MD5checksum string
}

type Manifest struct {
	SourceBucket      string         `json:"sourceBucket"`
	DestinationBucket string         `json:"destinationBucket"`
	Version           string         `json:"version"`
	CreationTimestamp string         `json:"creationTimestamp"`
	FileFormat        string         `json:"fileFormat"`
	FileSchema        string         `json:"fileSchema"`
	Files             []ManifestFile `json:"files"`
}

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

	s, err := session.NewSession(&conf)

	if err != nil {
		log.Panicf("failed to open s3 session:%v", err)
	}

	s3Service := s3.New(s)

	listResult, err := s3Service.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket:    aws.String(BUCKET),
		Prefix:    aws.String(PREFIX),
		Delimiter: aws.String("/")})
	// os.Args[1:]

	if err != nil {
		log.Panicf("failed to list prefix err:%v", err)
	}

	maxPrefix := ""

	for _, prefix := range listResult.CommonPrefixes {
		parts := strings.Split(*prefix.Prefix, "/")

		if len(parts[len(parts)-2]) == 17 {
			maxPrefix = *prefix.Prefix
		}
	}

	log.Printf("last date seems to be:%s", maxPrefix)

	resultFile, err := s3Service.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(BUCKET),
		Key:    aws.String(maxPrefix + "manifest.json")})

	if err != nil {
		log.Panicf("failed to get manifest %v", err)
	}

	defer resultFile.Body.Close()

	jReader := json.NewDecoder(resultFile.Body)
	m := Manifest{}
	jReader.Decode(&m)

	files := make([]string, 0)

	defer func() {
		for _, fileName := range files {
			os.Remove(fileName)
		}
	}()

	for _, manifestFile := range m.Files {
		tmpFile, err := ioutil.TempFile("", "manifest_file-")

		if err != nil {
			log.Panicf("failed to create temporary file")
		}

		files = append(files, tmpFile.Name())

		resultFile, err := s3Service.GetObject(&s3.GetObjectInput{Bucket: aws.String(BUCKET), Key: aws.String(manifestFile.Key)})

		if err != nil {
			tmpFile.Close()
			log.Panicf("failed to get file %s from aws %v", manifestFile.Key, err)
		}

		_, err = io.Copy(tmpFile, resultFile.Body)

		tmpFile.Close()
		resultFile.Body.Close()

		if err != nil {
			log.Panicf("failed to copy s3 file to temp file %v", err)
		}
	}

	log.Printf("files:%v", files)

	// accounts/834644773037/day/2017/11/13

	// mapInterface, err := sum.AccountMapFunction()
	mapInterface, err := sum.YearMonthMapFunction()

	if err != nil {
		log.Panicf("Failed to prepare a map function:%v\n", err)
	}

	r, err := sum.ProcessFiles(files, acceptAllFilterFunction, mapInterface, true)

	if err != nil {
		log.Panicf("Process file error:%v", err)
	}

	for _, record := range r.Records {
		fmt.Printf("%s %d\n", record.Key, record.Size)
	}
}
