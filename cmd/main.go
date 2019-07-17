package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func makeRecord(table string, record []string) *dynamodb.PutItemInput {
	params := &dynamodb.PutItemInput{
		TableName: aws.String(table),
		Item: map[string]*dynamodb.AttributeValue{
			"Col0": {
				S: aws.String(record[0]),
			},
			"Col1": {
				S: aws.String(record[1]),
			},
			"Col2": {
				S: aws.String(record[2]),
			},
		},
	}
	return params
}

var (
	c      = flag.Int("c", -1, "(Option) number of import data. non set is all data imported.")
	file   = flag.String("file", "", "(MUST) imported file name. default empty file name")
	table  = flag.String("table", "", "(MUST) Dynamo DB table name. default empty table name")
	region = flag.String("region", "ap-northeast-1", "(MUST) AWS Region name. default ap-northeast-1")
)

func main() {
	// flag parse

	flag.Parse()
	if *file == "" || *table == "" || *region == "" {
		log.Fatal("Must parameter lacked")
	}

	// Client Initialized
	svc := dynamodb.New(session.New(), aws.NewConfig().WithRegion(*region))

	// Data
	fp, err := os.Open(*file)
	if err != nil {
		log.Fatal(err)
	}
	defer fp.Close()

	reader := csv.NewReader(fp)
	reader.Comma = ','
	reader.LazyQuotes = true // ダブルクォートをチェックしない

	// data import
	counter := 0
	for {
		record, err := reader.Read()
		if err != nil {
			log.Fatal(err)
		}
		if err == io.EOF {
			break
		}
		params := makeRecord(*table, record)
		_, err = svc.PutItem(params)
		if err != nil {
			log.Fatal(err)
		}
		counter++

		if counter == *c {
			break
		}
	}

	if *c != -1 {
		fmt.Printf("%d Data Import Completed\n", *c)
	}

	os.Exit(0)

}
