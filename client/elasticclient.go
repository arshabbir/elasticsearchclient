package client

import (
	"context"
	"elasticclient/domain"
	"elasticclient/utils"
	"encoding/json"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

type esClient struct {
	client *elasticsearch.Client
}

type EsClient interface {
	Create(string, domain.Student) *utils.ApiError
	Search(string, map[string]interface{}) ([]domain.Student, *utils.ApiError)
	//Update(string, domain.Student) *utils.ApiError
	//Delete(string, domain.Student) *utils.ApiError

}

func NewESClient(url string) EsClient {

	//Create new client
	client, err := elasticsearch.NewDefaultClient()

	if err != nil {

		log.Println("Error creating the elastic client")
		os.Exit(1)

	}

	return &esClient{client: client}

}

func (es *esClient) Create(indexname string, record domain.Student) *utils.ApiError {

	//convert into JSON

	bytes, merr := json.Marshal(&record)
	if merr != nil {
		log.Println("Parsing Error")
		return &utils.ApiError{Message: "Parsing Error", Id: 0}
	}

	//Form the index request

	req := esapi.IndexRequest{
		Index:      "student",
		Body:       strings.NewReader(string(bytes)),
		DocumentID: strconv.Itoa(rand.Intn(1000) + 1),
	}

	//Perform the query

	resp, err := req.Do(context.Background(), es.client)

	if err != nil {
		log.Println("Error while Indexing ")
		return &utils.ApiError{Message: "Error While Indexing ", Id: 0}
	}

	log.Println("Index Successful :  ", resp)
	return nil
}

func (es *esClient) Search(indexname string, queryConditions map[string]interface{}) ([]domain.Student, *utils.ApiError) {

	//Create a New bool query and add must/should conditions, execute the query on Index

	//Create the search Template
	searchTemplate := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []map[string]interface{}{},
			},
		},
	}

	//Populate the search template with the query conditions
	matchs := make([]map[string]interface{}, 1, len(queryConditions))

	for key, value := range queryConditions {

		matchs = append(matchs, map[string]interface{}{"match": map[string]interface{}{key: value}})
	}

	searchTemplate["query"].(map[string]interface{})["bool"].(map[string]interface{})["must"] = matchs

	log.Println("Search Template: ", searchTemplate)

	//Encode in JSON

	bytes, err := json.Marshal(&searchTemplate)

	if err != nil {
		log.Println("JSON Encoding error")
		return nil, &utils.ApiError{Message: err.Error(), Id: 0}
	}

	log.Println(string(bytes))

	//Perform Search operation

	resp, serr := es.client.Search(es.client.Search.WithIndex(indexname),
		es.client.Search.WithBody(strings.NewReader(string(bytes))),
		es.client.Search.WithContext(context.Background()),
		es.client.Search.WithTrackTotalHits(true),
	)

	if serr != nil {
		log.Println("Search error")
		return nil, &utils.ApiError{Message: err.Error(), Id: 0}
	}

	//log.Println(resp)

	//Parse the response

	var r map[string]interface{}

	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		log.Println("Error unmarshaling the response")
		return nil, &utils.ApiError{Message: err.Error(), Id: 0}

	}

	students := make([]domain.Student, 1, int(r["hits"].(map[string]interface{})["total"].(map[string]interface{})["value"].(float64)))
	for _, hit := range r["hits"].(map[string]interface{})["hits"].([]interface{}) {

		//log.Println(hit.(map[string]interface{})["_id"])
		var st domain.Student

		st.StudentId = hit.(map[string]interface{})["_source"].(map[string]interface{})["studentid"].(string)
		st.Class = hit.(map[string]interface{})["_source"].(map[string]interface{})["class"].(string)
		st.Name = hit.(map[string]interface{})["_source"].(map[string]interface{})["name"].(string)
		st.Marks = int64(hit.(map[string]interface{})["_source"].(map[string]interface{})["marks"].(float64))

		students = append(students, st)

	}
	return students, nil

}
