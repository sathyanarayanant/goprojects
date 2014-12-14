package main

import (
	"fmt"
	"log"
	"net/http"

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
		oauth2.Scope(datastore.ScopeDatastore),
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

	err := putEntity(ctx, "sowmya", "ramakrishnan", "sowmya.ram@gmail.com")

	if err != nil {
		fmt.Println("Error:", err)
	} else {
		fmt.Println("success")
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
