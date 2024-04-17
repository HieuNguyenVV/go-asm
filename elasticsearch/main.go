package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/olivere/elastic/v7"
	"log"
)

type User struct {
	Name    string   `json:"name"`
	Address string   `json:"address"`
	Email   string   `json:"email"`
	Phone   string   `json:"phone"`
	Age     int      `json:"age"`
	Status  int      `json:"status"`
	Active  bool     `json:"active"`
	Tags    []string `json:"tags"`
	Ratings []int    `json:"ratings"`
	Date    int64    `json:"date"`
}

const (
	index = "users-data"
)

type repoElastic struct {
	client  *elastic.Client
	context context.Context
}

func main() {
	client, err := elastic.NewClient(elastic.SetURL("http://localhost:9200"), elastic.SetSniff(false))
	if err != nil {
		log.Fatalf("Create client error, err: %v", err)
	}

	ctx := context.Background()
	repo := repoElastic{
		client:  client,
		context: ctx,
	}
	//err = repo.Create()
	//if err != nil {
	//	log.Fatalf("Error: %v", err)
	//}
	//err = repo.CreateManyDoc()
	//if err != nil {
	//	return
	//}
	//err = repo.GetById()
	//if err != nil {
	//	return
	//}
	err = repo.UpdateByQuery()
	if err != nil {
		return
	}

}

func (repo *repoElastic) Create() error {
	user := User{
		Name:    "John Doe 123",
		Address: "123 Main St",
		Email:   "john.doe@example.com",
		Phone:   "555-1234",
		Age:     30,
		Status:  1,
		Active:  true,
		Tags:    []string{"tag1", "tag2"},
		Ratings: []int{4, 5, 3},
		Date:    1618695035,
	}

	_, err := repo.client.Index().Index(index).BodyJson(user).Do(repo.context)
	if err != nil {
		return err
	}
	log.Printf("Create new doc success")
	return err

}

func (repo *repoElastic) CreateManyDoc() error {
	users := []User{
		{Name: "John Doe 123",
			Address: "123 Main St",
			Email:   "john.doe@example.com",
			Phone:   "555-1234",
			Age:     30,
			Status:  1,
			Active:  true,
			Tags:    []string{"tag1", "tag2"},
			Ratings: []int{4, 5, 3},
			Date:    1618695035,
		},
		{
			Name:    "John Doe 1235",
			Address: "123 Main St",
			Email:   "john.doe@example.com",
			Phone:   "555-1234",
			Age:     30,
			Status:  1,
			Active:  true,
			Tags:    []string{"tag1", "tag2"},
			Ratings: []int{4, 5, 3},
			Date:    1618695035,
		},
		{
			Name:    "John Doe 1237",
			Address: "123 Main St",
			Email:   "john.doe@example.com",
			Phone:   "555-1234",
			Age:     30,
			Status:  1,
			Active:  true,
			Tags:    []string{"tag1", "tag2"},
			Ratings: []int{4, 5, 3},
			Date:    1618695035,
		},
	}

	bulkRequest := repo.client.Bulk()

	for _, user := range users {
		req := elastic.NewBulkIndexRequest().Index(index).Doc(user)
		bulkRequest = bulkRequest.Add(req)
	}
	bulkRespone, err := bulkRequest.Do(repo.context)
	if err != nil {
		return err
	}
	if bulkRespone.Errors {
		log.Printf("Bulk request contains errors")
		for _, item := range bulkRespone.Items {
			for _, action := range item {
				if action.Error != nil {
					log.Printf("Error for document ID %s: %s", action.Id, action.Error)
				}
			}
		}
	} else {
		log.Printf("Bulk request executed successfully")
	}
	return err
}

func (repo *repoElastic) GetById() error {
	result, err := repo.client.Get().Index(index).Id("KI_c6o4B1dM7FwgwIfi4").Do(repo.context)
	if err != nil {
		return err
	}
	if !result.Found {
		return nil
	}
	user := User{}
	if err := json.Unmarshal(result.Source, &user); err != nil {
		return err
	}
	fmt.Printf("Value: %v", user)
	fmt.Println()
	return err
}

func (repo *repoElastic) GetByIds() error {
	ids := []string{"KI_c6o4B1dM7FwgwIfi4", "VKaf544B0_G3Nak_MMbB", "Vaaf544B0_G3Nak_MMbG"}
	query := elastic.NewIdsQuery().Ids(ids...)

	result, err := repo.client.Search().Index(index).Query(query).Size(len(ids)).Do(repo.context)
	if err != nil {
		return err
	}
	var users []User
	for _, hit := range result.Hits.Hits {
		var user User
		if err := json.Unmarshal(hit.Source, &user); err != nil {
			continue
		}
		users = append(users, user)
	}
	fmt.Println(users)
	return err
}

func (repo *repoElastic) GetByQuery() error {
	query := elastic.NewBoolQuery()

	//termQuert := elastic.NewTermQuery("status", 4)
	//rangeQuery := elastic.NewRangeQuery("date").Gte(1681660722)
	//query = query.Must(termQuert, rangeQuery)

	statusQuery := elastic.NewTermQuery("status", 4)
	rangeQuery := elastic.NewRangeQuery("date").Gte(1681660722)

	query = query.Must(statusQuery, rangeQuery)

	activeQuery := elastic.NewTermQuery("active", true)
	query = query.Should(activeQuery)
	result, err := repo.client.Search().Index(index).Query(query).Size(1000).Do(repo.context)
	if err != nil {
		return err
	}

	var users []User
	for _, hit := range result.Hits.Hits {
		var user User
		if err := json.Unmarshal(hit.Source, &user); err != nil {
			continue
		}
		users = append(users, user)
	}
	fmt.Println(users)
	return err
}

func (repo *repoElastic) UpdateById() error {
	update := User{
		Name:    "Nguyen Van B",
		Address: "Ha Noi",
		Email:   "nguyenvanb@gmail.com",
	}
	id := "Vaaf544B0_G3Nak_MMbG"
	updateResult, err := repo.client.Update().Index(index).Id(id).Doc(update).Do(repo.context)
	if err != nil {
		return err
	}

	if updateResult.Result == "updated" {
		log.Printf("ok")
	} else {
		log.Printf("unknow: %v", updateResult.Result)
	}
	return nil
}

func (repo *repoElastic) UpdateByQuery() error {
	query := elastic.NewTermQuery("active", true)

	//script := `ctx._source.age= 24;ctx._source.status=4`
	script := `if (ctx._source.age == 24){
			ctx._source.status = 9;
			} else {
            ctx._source.status = 5;
            }`

	updateResult, err := repo.client.UpdateByQuery().
		Index(index).
		Query(query).
		Script(elastic.NewScriptInline(script).Lang("painless").Param("threshold", 18)).
		Do(repo.context)

	if err != nil {
		log.Println(err)
		return err
	}
	log.Printf("Updated total: %v", updateResult.Total)
	return nil
}
