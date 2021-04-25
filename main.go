package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/MasatoTokuse/exectime"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

func main() {
	fmt.Println("Begin MongoDB")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	credential := options.Credential{
		Username:    "root",
		Password:    "example",
		PasswordSet: true,
	}
	client, err := mongo.Connect(ctx, options.Client().SetAuth(credential).ApplyURI("mongodb://mongo:27017"))
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
	surveys := make([]interface{}, 0)
	for i := 0; i < 10000; i++ {
		surveys = append(surveys, bson.D{{"survey_id", 100}, {"selected", i}, {"user_id", i}})
	}
	execTime := exectime.Measure(func() {
		_, err = collection.InsertMany(ctx, surveys)
	})
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println(execTime.Seconds())

	execTime = exectime.Measure(func() {
		cur, err := collection.Find(ctx, bson.D{{"survey_id", 100}})
		if err != nil {
			log.Fatalln(err)
		}
		defer cur.Close(ctx)
		results := make([]bson.D, 0)
		for cur.Next(ctx) {
			var result bson.D
			err := cur.Decode(&result)
			if err != nil {
				log.Fatal(err)
			}
			results = append(results, result)
		}
		fmt.Println(len(results))
		if err := cur.Err(); err != nil {
			log.Fatal(err)
		}
	})
	fmt.Println(execTime.Seconds())

	fmt.Println("End MongoDB")
}
