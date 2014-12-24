package main

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"strconv"
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

	putEntity2(ctx)
	putEntity2(ctx)
	putEntity2(ctx)

	putManyEntities(ctx)
	putManyEntities(ctx)
	putManyEntities(ctx)
	putManyEntities(ctx)
	putManyEntities(ctx)
	putManyEntities(ctx)

	getEntity(ctx)
	getEntity(ctx)
}

func getEntity(ctx context.Context) {
	email := getRandomEmail()

	key := datastore.NewKey(ctx, "contactInfoEntity", email, 0, nil)

	var entity contactInfoEntity
	t0 := time.Now()
	err := datastore.Get(ctx, key, &entity)
	t1 := time.Now()

	if err == nil {
		fmt.Println("datastore get completed in: ", t1.Sub(t0))
	} else {
		log.Fatal("cannot get entity from datastore")
	}
}

func putEntity2(ctx context.Context) {

	email := getRandomEmail()

	key := datastore.NewKey(ctx, "contactInfoEntity", email, 0, nil)

	contactInfoEntity := contactInfoEntity{
		FirstName: "fname",
		LastName:  "lname",
	}

	t0 := time.Now()
	_, err := datastore.Put(ctx, key, &contactInfoEntity)
	t1 := time.Now()

	if err == nil {
		fmt.Println("datastore put completed in: ", t1.Sub(t0))
	} else {
		log.Fatal("cannot put entity in datastore", err)
	}
}

func getRandomEmail() string {
	var buffer bytes.Buffer

	random := rand.Int()
	buffer.WriteString("email")
	buffer.WriteString(strconv.Itoa(random))

	return buffer.String()
}

func get(ctx *context.Context) {

}

func putManyEntities(ctx context.Context) {
	t0 := time.Now()

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

		datastore.NewKey(ctx, "contactInfoEntity", "sowmya11@gmail.com", 0, nil),
		datastore.NewKey(ctx, "contactInfoEntity", "sowmya12@gmail.com", 0, nil),
		datastore.NewKey(ctx, "contactInfoEntity", "sowmya13@gmail.com", 0, nil),
		datastore.NewKey(ctx, "contactInfoEntity", "sowmya14@gmail.com", 0, nil),
		datastore.NewKey(ctx, "contactInfoEntity", "sowmya15@gmail.com", 0, nil),
		datastore.NewKey(ctx, "contactInfoEntity", "sowmya16@gmail.com", 0, nil),
		datastore.NewKey(ctx, "contactInfoEntity", "sowmya17@gmail.com", 0, nil),
		datastore.NewKey(ctx, "contactInfoEntity", "sowmya18@gmail.com", 0, nil),
		datastore.NewKey(ctx, "contactInfoEntity", "sowmya19@gmail.com", 0, nil),
		datastore.NewKey(ctx, "contactInfoEntity", "sowmya20@gmail.com", 0, nil),

		datastore.NewKey(ctx, "contactInfoEntity", "sowmya21@gmail.com", 0, nil),
		datastore.NewKey(ctx, "contactInfoEntity", "sowmya22@gmail.com", 0, nil),
		datastore.NewKey(ctx, "contactInfoEntity", "sowmya23@gmail.com", 0, nil),
		datastore.NewKey(ctx, "contactInfoEntity", "sowmya24@gmail.com", 0, nil),
		datastore.NewKey(ctx, "contactInfoEntity", "sowmya25@gmail.com", 0, nil),
		datastore.NewKey(ctx, "contactInfoEntity", "sowmya26@gmail.com", 0, nil),
		datastore.NewKey(ctx, "contactInfoEntity", "sowmya27@gmail.com", 0, nil),
		datastore.NewKey(ctx, "contactInfoEntity", "sowmya28@gmail.com", 0, nil),
		datastore.NewKey(ctx, "contactInfoEntity", "sowmya29@gmail.com", 0, nil),
		datastore.NewKey(ctx, "contactInfoEntity", "sowmya30@gmail.com", 0, nil),

		datastore.NewKey(ctx, "contactInfoEntity", "sowmya31@gmail.com", 0, nil),
		datastore.NewKey(ctx, "contactInfoEntity", "sowmya32@gmail.com", 0, nil),
		datastore.NewKey(ctx, "contactInfoEntity", "sowmya33@gmail.com", 0, nil),
		datastore.NewKey(ctx, "contactInfoEntity", "sowmya34@gmail.com", 0, nil),
		datastore.NewKey(ctx, "contactInfoEntity", "sowmya35@gmail.com", 0, nil),
		datastore.NewKey(ctx, "contactInfoEntity", "sowmya36@gmail.com", 0, nil),
		datastore.NewKey(ctx, "contactInfoEntity", "sowmya37@gmail.com", 0, nil),
		datastore.NewKey(ctx, "contactInfoEntity", "sowmya38@gmail.com", 0, nil),
		datastore.NewKey(ctx, "contactInfoEntity", "sowmya39@gmail.com", 0, nil),
		datastore.NewKey(ctx, "contactInfoEntity", "sowmya40@gmail.com", 0, nil),

		datastore.NewKey(ctx, "contactInfoEntity", "sowmya41@gmail.com", 0, nil),
		datastore.NewKey(ctx, "contactInfoEntity", "sowmya42@gmail.com", 0, nil),
		datastore.NewKey(ctx, "contactInfoEntity", "sowmya43@gmail.com", 0, nil),
		datastore.NewKey(ctx, "contactInfoEntity", "sowmya44@gmail.com", 0, nil),
		datastore.NewKey(ctx, "contactInfoEntity", "sowmya45@gmail.com", 0, nil),
		datastore.NewKey(ctx, "contactInfoEntity", "sowmya46@gmail.com", 0, nil),
		datastore.NewKey(ctx, "contactInfoEntity", "sowmya47@gmail.com", 0, nil),
		datastore.NewKey(ctx, "contactInfoEntity", "sowmya48@gmail.com", 0, nil),
		datastore.NewKey(ctx, "contactInfoEntity", "sowmya49@gmail.com", 0, nil),
		datastore.NewKey(ctx, "contactInfoEntity", "sowmya50@gmail.com", 0, nil),
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

	if err == nil {
		t1 := time.Now()
		fmt.Println("put multi completed in:", t1.Sub(t0))
	} else {
		log.Fatal("put multi failed: ", err)
	}

	return
}
