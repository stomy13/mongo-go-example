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
				surveys = append(surveys, bson.D{
					{Key: "survey_id", Value: surveyId},
					{Key: "user_id", Value: i},
					{Key: "1", Value: 1},
				})
			} else {
				surveys = append(surveys, bson.D{
					{Key: "survey_id", Value: surveyId},
					{Key: "user_id", Value: i},
					{Key: "1", Value: 1},
					{Key: "3", Value: 1},
					{Key: "4", Value: 1},
					{Key: "5", Value: 1},
				})
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
		cur, err := collection.Find(ctx, bson.D{{Key: "survey_id", Value: 5}})
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

func AggregateSurveys(ctx context.Context, collection *mongo.Collection) []bson.M {

	var results []bson.M

	execTime := exectime.Measure(func() {
		matchStage := bson.D{
			{Key: "$match", Value: bson.D{
				{Key: "$or", Value: bson.A{
					bson.D{
						{"survey_id", 5},
					},
					bson.D{
						{"survey_id", 7},
					},
					bson.D{
						{"survey_id", 3},
					},
					bson.D{
						{"survey_id", 1},
					},
				}},
			}}}
		groupStage := bson.D{
			{"$group", bson.D{
				{"_id", "$survey_id"},
				{"1_count", bson.D{
					{"$sum", "$1"},
				}},
				{"2_count", bson.D{
					{"$sum", "$2"},
				}},
				{"3_count", bson.D{
					{"$sum", "$3"},
				}},
				{"4_count", bson.D{
					{"$sum", "$4"},
				}},
				// {"5_count", bson.D{
				// 	{"$sum", "$5"},
				// }},
			}}}
		_ = matchStage
		cur, err := collection.Aggregate(ctx, mongo.Pipeline{
			matchStage,
			groupStage,
		})
		if err != nil {
			log.Fatalln(err)
		}
		defer cur.Close(ctx)

		if err = cur.All(ctx, &results); err != nil {
			panic(err)
		}
		fmt.Println("aggregate objects =", len(results))

		if err := cur.Err(); err != nil {
			log.Fatal(err)
		}
	})
	fmt.Println("aggregate took", execTime.Seconds())

	return results
}
