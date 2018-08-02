package sum

import (
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

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

func readManifest(s3Service *s3.S3, bucket string, object string) (Manifest, error) {
	m := Manifest{}

	resultFile, err := s3Service.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object)})

	if err != nil {
		return m, fmt.Errorf("failed to get manifest %v", err)
	}

	defer resultFile.Body.Close()

	jReader := json.NewDecoder(resultFile.Body)
	jReader.Decode(&m)

	return m, nil
}

func readFile(s3Service *s3.S3, bucket string, key string) (string, error) {
	tmpFile, err := ioutil.TempFile("", "manifest_file-")

	defer tmpFile.Close()

	if err != nil {
		return "", fmt.Errorf("failed to create temporary file %v", err)
	}

	resultFile, err := s3Service.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key)})

	if err != nil {
		return tmpFile.Name(), fmt.Errorf("failed to get file %s from aws %v", key, err)
	}

	_, err = io.Copy(tmpFile, resultFile.Body)

	if err != nil {
		return tmpFile.Name(), fmt.Errorf("failed to copy s3 file to temp file %v", err)
	}

	return tmpFile.Name(), nil
}

func S3Read(config aws.Config, bucket string, prefix string, filterFunction func([]string) bool, mapFunction MapInterface) (SortedResult, error) {
	blankResults := SortedResult{}

	s, err := session.NewSession(&config)

	if err != nil {
		return blankResults, fmt.Errorf("failed to open s3 session:%v", err)
	}

	s3Service := s3.New(s)

	listResult, err := s3Service.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket:    aws.String(bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/")})
	// os.Args[1:]

	if err != nil {
		return blankResults, fmt.Errorf("failed to list prefix err:%v", err)
	}

	maxPrefix := ""

	for _, prefix := range listResult.CommonPrefixes {
		parts := strings.Split(*prefix.Prefix, "/")

		if len(parts[len(parts)-2]) == 17 {
			maxPrefix = *prefix.Prefix
		}
	}

	log.Printf("last date seems to be:%s", maxPrefix)

	m, err := readManifest(s3Service, bucket, maxPrefix+"manifest.json")

	if err != nil {
		return blankResults, fmt.Errorf("failed to get manifest %v", err)
	}

	files := make([]string, 0)

	defer func() {
		for _, fileName := range files {
			os.Remove(fileName)
		}
	}()

	for _, manifestFile := range m.Files {
		fileName, err := readFile(s3Service, bucket, manifestFile.Key)

		if fileName != "" {
			files = append(files, fileName)
		}

		if err != nil {
			return blankResults, fmt.Errorf("failed to get inventory log file %v", err)
		}
	}

	log.Printf("files:%v", files)

	return ProcessFiles(files, filterFunction, mapFunction, true)
}

func WriteResult(config aws.Config, bucket string, keyName string, results SortedResult) error {
	tmpFile, err := ioutil.TempFile("", "inventory_sum_results")

	if err != nil {
		return fmt.Errorf("failed to open temp file for result:%v", err)
	}

	defer func() {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
	}()

	for i, record := range results.Records {
		if i > 0 {
			_, err = fmt.Fprint(tmpFile, "\n")

			if err != nil {
				return fmt.Errorf("failed to write to temp file:%v", err)
			}

		}

		_, err = fmt.Fprintf(tmpFile, "%s,%d", record.Key, record.Size)

		if err != nil {
			return fmt.Errorf("failed to write to temp file:%v", err)
		}
	}

	s, err := session.NewSession(&config)

	if err != nil {
		return fmt.Errorf("failed to open s3 session:%v", err)
	}

	s3Service := s3.New(s)

	_, err = tmpFile.Seek(0, io.SeekStart)

	if err != nil {
		return fmt.Errorf("failed to seek to start:%v", err)
	}

	_, err = s3Service.PutObject(&s3.PutObjectInput{Body: tmpFile, Bucket: aws.String(bucket), Key: aws.String(keyName)})

	if err != nil {
		return fmt.Errorf("failed to upload tmp file to s3:%v", err)
	}

	return nil
}
