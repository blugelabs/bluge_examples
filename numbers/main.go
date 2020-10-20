package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/blugelabs/bluge"
)

func main() {
	// create a temp directory to store the index
	tmpDir, err := ioutil.TempDir("", "quickstart.bluge")
	if err != nil {
		log.Fatalf("error creating temp directory: %v", err)
	}

	// by default this index will be removed after execution
	defer func() {
		_ = os.RemoveAll(tmpDir)
	}()

	// create a default configuration for working with an index
	// that will be stored on disk in the temp directory we created
	config := bluge.DefaultConfig(tmpDir)

	// open an index writer using the configuration
	writer, err := bluge.OpenWriter(config)
	if err != nil {
		log.Fatalf("error opening writer: %v", err)
	}
	defer func() {
		err = writer.Close()
		if err != nil {
			log.Fatalf("error closing writer: %v", err)
		}
	}()

	// create a document to put in the index
	// the document has one numeric field named 'age', with value 0.1
	doc := bluge.NewDocument("a").
		AddField(bluge.NewNumericField("age", 0.1))

	// update the index with this document
	err = writer.Update(doc.ID(), doc)
	if err != nil {
		log.Fatalf("error updating document: %v", err)
	}
	fmt.Println("indexed document with id:a age:0.1")

	// get a reader for the index
	reader, err := writer.Reader()
	if err != nil {
		log.Fatalf("error getting index reader: %v", err)
	}
	defer func() {
		err = reader.Close()
		if err != nil {
			log.Fatalf("error closing reader: %v", err)
		}
	}()

	// build a query to find the document we indexed
	query := bluge.NewNumericRangeQuery(0, 1).SetField("age")

	// build a top-n request to find the top 10 matches,
	// also include the standard aggregations
	request := bluge.NewTopNSearch(10, query).
		WithStandardAggregations()

	fmt.Println("searching for age greater or equal to 0 and less than 1")

	// execute this search on the reader
	documentMatchIterator, err := reader.Search(context.Background(), request)
	if err != nil {
		log.Fatalf("error executing search: %v", err)
	}

	// iterate through the document matches
	match, err := documentMatchIterator.Next()
	for err == nil && match != nil {

		// load the identifier for this match
		err = match.VisitStoredFields(func(field string, value []byte) bool {
			if field == "_id" {
				fmt.Printf("match: %s\n", string(value))
			}
			return true
		})
		if err != nil {
			log.Fatalf("error loading stored fields: %v", err)
		}

		// load the next document match
		match, err = documentMatchIterator.Next()
	}
	if err != nil {
		log.Fatalf("error iterator document matches: %v", err)
	}

}
