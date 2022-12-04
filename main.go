package main

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/MasatoTokuse/exectime"
	"github.com/MasatoTokuse/mongo-go-example/mymongo"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mymongo.NewClient(ctx)
	if err != nil {
		log.Fatalln(err)
	}
	defer func() {
		if err = client.Disconnect(ctx); err != nil {
			panic(err)
		}
	}()

	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		log.Fatalln(err)
	}

	collection := client.Database("test").Collection("surveys")
	results := mymongo.AggregateSurveys(ctx, collection)
	printAggregate(results)
	// mymongo.InsertManySurveys(ctx, collection)
	// results := mymongo.FindSurveys(ctx, collection)
	// totalSurveys(results)
}

func totalSurveys(results []bson.D) {
	execTime := exectime.Measure(func() {
		sum := make(map[string]int)
		for i := range results {
			m := results[i].Map()
			for i := 1; i <= 4; i++ {
				choice := m[strconv.Itoa(i)]
				if choice != nil {
					sum[strconv.Itoa(i)]++
				}
			}
		}

		for i := 1; i <= 4; i++ {
			fmt.Println(i, "=", sum[strconv.Itoa(i)])
		}
	})
	fmt.Println("total took", execTime.Seconds())
}

func printAggregate(results []primitive.M) {
	for i := range results {
		for k, v := range results[i] {
			fmt.Println(i, k, v)
		}
	}
}
