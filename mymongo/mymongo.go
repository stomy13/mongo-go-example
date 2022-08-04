package mymongo

import (
	"context"
	"fmt"
	"log"

	"github.com/MasatoTokuse/exectime"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func NewClient(ctx context.Context) (*mongo.Client, error) {
	credential := newTestCredential()

	// host := "mongo"
	host := "localhost"
	uri := fmt.Sprintf("mongodb://%s:27017", host)
	return mongo.Connect(
		ctx,
		options.Client().SetAuth(credential).ApplyURI(uri),
	)
}

func newTestCredential() options.Credential {
	return options.Credential{
		Username:    "root",
		Password:    "example",
		PasswordSet: true,
	}
}

func InsertManySurveys(ctx context.Context, collection *mongo.Collection) {
	surveys := make([]interface{}, 0)
	for surveyId := 1; surveyId < 11; surveyId++ {
		for i := 0; i < 10000; i++ {
			if i%2 == 0 {
				surveys = append(surveys, bson.D{{"survey_id", surveyId}, {"user_id", i}, {"1", 1}})
			} else {
				surveys = append(surveys, bson.D{{"survey_id", surveyId}, {"user_id", i}, {"1", 1}, {"3", 1}, {"4", 1}, {"5", 1}})
			}
		}
	}

	var err error
	execTime := exectime.Measure(func() {
		_, err = collection.InsertMany(ctx, surveys)
	})
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println("insert took", execTime.Seconds())
}

func FindSurveys(ctx context.Context, collection *mongo.Collection) []bson.D {

	results := make([]bson.D, 0)

	execTime := exectime.Measure(func() {
		cur, err := collection.Find(ctx, bson.D{{"survey_id", 5}})
		if err != nil {
			log.Fatalln(err)
		}
		defer cur.Close(ctx)

		for cur.Next(ctx) {
			var result bson.D
			err := cur.Decode(&result)
			if err != nil {
				log.Fatal(err)
			}
			results = append(results, result)
		}
		fmt.Println("finded objects =", len(results))

		if err := cur.Err(); err != nil {
			log.Fatal(err)
		}
	})
	fmt.Println("find took", execTime.Seconds())

	return results
}
