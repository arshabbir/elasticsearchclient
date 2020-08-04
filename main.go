package main

import (
	"elasticclient/client"
	"elasticclient/domain"
	"log"
	"os"
)

func main() {
	//Create es client
	//url := "http://35.168.3.84:9200/"

	os.Setenv("ELASTICSEARCH_URL", "http://35.168.3.84:9200/")

	insertrecord := domain.Student{StudentId: "T2",
		Name:  "arshabbirhussain1",
		Class: "(Phd)1",
		Marks: 72,
	}

	_ = insertrecord
	searchQuery := map[string]interface{}{
		"name":  "arshabbirhussain",
		"marks": 70,
	}

	esclient := client.NewESClient(os.Getenv("ELASTICSEARCH_URL"))

	if esclient == nil {
		log.Println("Error connection to the client ")
		return
	}

	log.Println("Connection successful to ElasticCluster ")
	//Create Index

	if err := esclient.Create("student", insertrecord); err != nil {
		log.Println(err)
		return
	}

	log.Println("Indexing Successful . ")

	result, err := esclient.Search("student", searchQuery)
	//Search
	if err != nil {
		log.Println(err)
		return
	}

	log.Println("\nLength of the result :  ", len(result))
	log.Println("Result", result)
	return

}
