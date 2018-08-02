package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/ilyail3/inventorySum/sum"
	"os"
	"strings"
	"time"
)

type MyEvent struct {
}

func HandleRequest(ctx context.Context, event MyEvent) (string, error) {
	conf := aws.Config{
		Region: aws.String(os.Getenv("AWS_DEFAULT_REGION")),
		S3DisableContentMD5Validation: aws.Bool(true)}

	monthAgo := "/month" + time.Now().UTC().AddDate(0, -1, 0).Format("/2006/01")
	fmt.Printf("expecting month to be:%s\n", monthAgo)

	prevMonth := func(record []string) bool {
		return strings.Contains(record[1], monthAgo)
	}

	mapInterface, err := sum.AccountMapFunction(true)

	if err != nil {
		return "", fmt.Errorf("failed to prepare a map function:%v", err)
	}

	r, err := sum.S3Read(
		conf,
		os.Getenv("bucket"),
		os.Getenv("prefix"),
		prevMonth,
		mapInterface)

	if err != nil {
		return "", fmt.Errorf("faield to read s3 data:%v", err)
	}

	err = sum.WriteResult(conf, os.Getenv("bucket"), os.Getenv("result"), r)

	if err != nil {
		return "", fmt.Errorf("failed to write result file to s3:%v", err)
	}

	return fmt.Sprintf("write %d results", len(r.Records)), nil
}

func main() {
	lambda.Start(HandleRequest)
}
