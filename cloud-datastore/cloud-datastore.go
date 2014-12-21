package main

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/cloud"
	"google.golang.org/cloud/datastore"
)

func getCtx() context.Context {
	// Initialize an authorized transport with Google Developers Console
	// JSON key. Read the google package examples to learn more about
	// different authorization flows you can use.
	// http://godoc.org/golang.org/x/oauth2/google
	opts, err := oauth2.New(
		google.ServiceAccountJSONKey("CassandraTest-key.json"),
		//google.ServiceAccountJSONKey("CassandraTest-804d05ba010f.json"),
		oauth2.Scope(datastore.ScopeDatastore, datastore.ScopeUserEmail),
	)
	if err != nil {
		log.Fatal(err)
	}

	//titanium-goods-766 is the project id for CassandraTest (under sthilakan@eyeota.com)

	ctx := cloud.NewContext("titanium-goods-766", &http.Client{Transport: opts.NewTransport()})

	// Use the context (see other examples)
	return ctx
}

type contactInfoEntity struct {
	EmailKey  *datastore.Key
	FirstName string
	LastName  string
}

func main() {
	ctx := getCtx()
	fmt.Println("successfully got context", ctx)

	t0 := time.Now()
	err := putEntity(ctx, "sowmya", "ramakrishnan", "sowmya.ram@gmail.com")
	t1 := time.Now()

	if err != nil {
		fmt.Println("Error:", err)
	} else {
		fmt.Println("datastore put success. Time elapsed:", t1.Sub(t0))
	}

	t0 = time.Now()
	err = putManyEntities(ctx)

	t1 = time.Now()

	if err != nil {
		fmt.Println("Error:", err)
	} else {
		fmt.Println("datastore put multi success. Time elapsed:", t1.Sub(t0))
	}
}

func putEntity(ctx context.Context, firstName string, lastName string, email string) error {
	key := datastore.NewKey(ctx, "contactInfoEntity", email, 0, nil)

	contactInfoEntity := contactInfoEntity{
		EmailKey:  key,
		FirstName: firstName,
		LastName:  lastName,
	}

	_, err := datastore.Put(ctx, key, &contactInfoEntity)

	return err
}

func putManyEntities(ctx context.Context) error {
	const numEntities = 10

	keys := []*datastore.Key{
		datastore.NewKey(ctx, "contactInfoEntity", "sowmya1@gmail.com", 0, nil),
		datastore.NewKey(ctx, "contactInfoEntity", "sowmya2@gmail.com", 0, nil),
		datastore.NewKey(ctx, "contactInfoEntity", "sowmya3@gmail.com", 0, nil),
		datastore.NewKey(ctx, "contactInfoEntity", "sowmya4@gmail.com", 0, nil),
		datastore.NewKey(ctx, "contactInfoEntity", "sowmya5@gmail.com", 0, nil),
		datastore.NewKey(ctx, "contactInfoEntity", "sowmya6@gmail.com", 0, nil),
		datastore.NewKey(ctx, "contactInfoEntity", "sowmya7@gmail.com", 0, nil),
		datastore.NewKey(ctx, "contactInfoEntity", "sowmya8@gmail.com", 0, nil),
		datastore.NewKey(ctx, "contactInfoEntity", "sowmya9@gmail.com", 0, nil),
		datastore.NewKey(ctx, "contactInfoEntity", "sowmya10@gmail.com", 0, nil),
	}

	entities := []*contactInfoEntity{
		{FirstName: "sowmya", LastName: "ram"},
		{FirstName: "sowmya", LastName: "ram"},
		{FirstName: "sowmya", LastName: "ram"},
		{FirstName: "sowmya", LastName: "ram"},
		{FirstName: "sowmya", LastName: "ram"},
		{FirstName: "sowmya", LastName: "ram"},
		{FirstName: "sowmya", LastName: "ram"},
		{FirstName: "sowmya", LastName: "ram"},
		{FirstName: "sowmya", LastName: "ram"},
		{FirstName: "sowmya", LastName: "ram"},
	}

	_, err := datastore.PutMulti(ctx, keys, entities)

	return err
}
