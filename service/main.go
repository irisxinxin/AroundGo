package main

import (
	"encoding/json"
	"fmt"
	"github.com/pborman/uuid"
	elastic "gopkg.in/olivere/elastic.v3"
	"log"
	"net/http"
	"reflect"
	"strconv"
)

type Location struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}

type Post struct {
	User     string   `json:"user"`
	Message  string   `json:"message"`
	Location Location `json:"location"`
}

const (
	INDEX    = "around"
	TYPE     = "post"
	ES_URL   = "http://34.67.97.161:9200"
	DISTANCE = "200km"
)

func handlePost(w http.ResponseWriter, r *http.Request) {
	//Parse body of request to get a json object
	fmt.Println("Received one post request")
	decoder := json.NewDecoder(r.Body)
	var p Post
	if err := decoder.Decode(&p); err != nil {
		panic(err)
		return
	}

	id := uuid.New()
	//Save to ES
	saveToES(&p, id)
	fmt.Fprintf(w, "Post received: %s\n", p.Message)
}

func saveToES(p *Post, id string) {
	//Create a client
	es_client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		panic(err)
		return
	}

	//Save to index
	_, err = es_client.Index().Index(INDEX).Type(TYPE).Id(id).BodyJson(p).Refresh(true).Do()

	if err != nil {
		panic(err)
		return
	}

	fmt.Printf("Post if saved to index : %s \n", p.Message)

}

func handleSearch(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Received one request for search")
	lat, _ := strconv.ParseFloat(r.URL.Query().Get("lat"), 64)
	lon, _ := strconv.ParseFloat(r.URL.Query().Get("lon"), 64)
	//range is optional
	ran := DISTANCE
	if val := r.URL.Query().Get("range"); val != "" {
		ran = val + "km"
	}
	fmt.Println("range is", ran)

	fmt.Printf("Search received: %f %f %s\n", lat, lon, ran)

	//Create a client
	client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		panic(err)
		return
	}

	//Define geo distance query
	q := elastic.NewGeoDistanceQuery("location")
	q = q.Distance(ran).Lat(lat).Lon(lon)

	//Search
	searchResult, err := client.Search().Index(INDEX).Query(q).Pretty(true).Do()
	if err != nil {
		panic(err)
	}

	fmt.Printf("Query took %d milliseconds\n", searchResult.TookInMillis)
	fmt.Printf("Found a total of %d post\n", searchResult.TotalHits())

	var typ Post
	var ps []Post
	// Each is a convinence function that iterates over hits in a search result that you don't need to check
	// for nil values in the response
	for _, item := range searchResult.Each(reflect.TypeOf(typ)) {
		p := item.(Post)
		fmt.Printf("Post by %s: %s at lat %v and lon %v\n", p.User,
			p.Message, p.Location.Lat, p.Location.Lon)
		//TODO :perform filtering based on keywords such as web spam
		ps = append(ps, p)
	}
	js, err := json.Marshal(ps)

	if err != nil {
		panic(err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Write(js)
}

func main() {
	// Create a client
	client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		panic(err)
		return
	}
	//Use the IndexExists service to check if a specified index exist
	exists, err := client.IndexExists(INDEX).Do()
	if err != nil {
		panic(err)
	}

	if !exists {
		//Create a new index
		mapping := `{
			"mappings":{
				"post":{
					"properties":{
						"location":{
							"type":"geo_point"
						}
					}
				}
			}
		}`

		_, err := client.CreateIndex(INDEX).Body(mapping).Do()

		if err != nil {
			panic(err)
		}
	}

	fmt.Println("started service")
	http.HandleFunc("/post", handlePost)
	http.HandleFunc("/search", handleSearch)
	log.Fatal(http.ListenAndServe(":8080", nil))
}
