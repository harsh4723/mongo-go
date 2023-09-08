package main

import (
	"context"
	"fmt"
	"log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
	log.Print("Init....")
	clientOptions := options.Client().ApplyURI("mongodb://hodor-mongo:27017")

	// Connect to MongoDB
	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Disconnect(context.Background())

	// Access the collection you want to perform the replaceOne operation on
	collection := client.Database("products").Collection("ss-unbxd-gcp-test1-1014331675337846")

	// Create a context with a timeout
	ctx := context.Background()

	// Define an array of products
	products := []map[string]interface{}{
		{
			"uniqueId":   121,
			"name":       "Mehawk",
			"occupation": "Greatest swordsmen",
		},
		{
			"uniqueId":   122,
			"name":       "Zoro",
			"occupation": "2nd best",
		},
	}

	// Create a list of WriteModel operations (including ReplaceOneModel)
	var replacements []mongo.WriteModel

	// Loop through the products and append ReplaceOneModel operations to replacements
	for _, product := range products {
		uniqueId, ok := product["uniqueId"]
		if !ok {
			fmt.Println("Harsh no uniqueId", uniqueId)
			continue
		}
		operationA := mongo.NewReplaceOneModel()
		operationA.SetFilter(bson.M{"uniqueId": uniqueId})
		operationA.SetReplacement(bson.M(product))
		operationA.SetUpsert(true)
		replacements = append(replacements, operationA)
	}

	bulkOption := options.BulkWriteOptions{}
	bulkOption.SetOrdered(true)
	// Perform the bulk write operation
	result, err := collection.BulkWrite(ctx, replacements, &bulkOption)
	if err != nil {
		log.Fatal(err)
	}

	// Print the result
	log.Printf("Matched %d documents and modified %d documents.\n", result.MatchedCount, result.ModifiedCount)
}
