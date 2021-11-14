package AcornStore


import (
	"github.com/danielblagy/hurlean"
	"fmt"
	"sync"
	"errors"
	"strings"
	"github.com/tidwall/sjson"
)


// internal

type DbClientFunctionalityProvider struct{
	connectionWaitGroup sync.WaitGroup
	connectionError error
	
	clientInstance *hurlean.ClientInstance
	
	dbName string
	username string
	password string
	
	responseWaitGroup sync.WaitGroup
	response string
}

func (fp *DbClientFunctionalityProvider) OnServerMessage(ci *hurlean.ClientInstance, message hurlean.Message) {
	
	switch (message.Type) {
	case "auth request":
		authJson, _ := sjson.Set("", "db", fp.dbName)
		authJson, _ = sjson.Set(authJson, "user", fp.username)
		authJson, _ = sjson.Set(authJson, "password", fp.password)
		
		ci.Send(hurlean.Message{"auth", authJson})
			
	case "connected":
		fp.clientInstance = ci
		fp.connectionWaitGroup.Done()
			
	case "rejected":
		fp.clientInstance = ci
		fp.connectionError = errors.New("Connection rejected by the server")
		fp.connectionWaitGroup.Done()
			
	case "response":
		fp.response = message.Body
		fp.responseWaitGroup.Done()
		
	case "server error response":
		fmt.Printf("Server Error Response on request '%v'\n", message.Body)
		fp.response = ""
		fp.responseWaitGroup.Done()
	}
	
	// debug
	if hurlean.EnableDebug {
		fmt.Printf("Client: a new message\n")
		fmt.Printf("----------------\n")
		fmt.Printf("Message:\n")
		fmt.Printf("  Type: %v\n", message.Type)
		fmt.Printf("  Body: %v\n", message.Body)
		fmt.Printf("----------------\n\n")
	}
}

func (fp *DbClientFunctionalityProvider) OnClientInit(ci *hurlean.ClientInstance) {
	
}

func (fp *DbClientFunctionalityProvider) OnClientUpdate(ci *hurlean.ClientInstance) {
	
}


// external

func Connect(url string) (*Db, error) {
	
	// disable hurlean debug prints
	hurlean.EnableDebug = false
	
	// TODO : check the lengths of string slices to validate the url
	url = strings.TrimPrefix(url, "acorn-store://")
	splitUrl := strings.Split(url, "/")
	
	address := strings.Split(splitUrl[0], ":")
	ip := address[0]
	port := address[1]
	
	dbName := splitUrl[1]
	
	userCredentials := strings.Split(splitUrl[2], ":")
	username := userCredentials[0]
	password := userCredentials[1]
	
	fp := &DbClientFunctionalityProvider{
		connectionWaitGroup: 	sync.WaitGroup{},
		connectionError: 		nil,
		
		clientInstance: 		nil,
		
		dbName: 				dbName,
		username: 				username,
		password: 				password,
		
		responseWaitGroup: 		sync.WaitGroup{},
		response: 				"",
	}
	
	fp.connectionWaitGroup.Add(1)
	
	go func() {
		if err := hurlean.ConnectToServer(ip, port, fp); err != nil {
			fp.connectionWaitGroup.Done()
			fp.connectionError = err
		}
	}()
	
	fp.connectionWaitGroup.Wait()
	
	return &Db{ ip, port, dbName, username, password, fp }, fp.connectionError
}

type Db struct {
	Ip string
	Port string
	Name string
	Username string
	Password string
	
	fp *DbClientFunctionalityProvider
}

// Turn on debug prints
func (db *Db) EnableDebug() {
	
	hurlean.EnableDebug = true
}

// Turn off debug prints
func (db *Db) DisableDebug() {
	
	hurlean.EnableDebug = false
}

// Close the database connection
func (db *Db) CloseConnection() {
	
	db.fp.clientInstance.Disconnect()
}

// Returns a JSON array of the names of the collections in the database
func (db *Db) ShowCollections() string {
	
	db.fp.responseWaitGroup.Add(1)
	db.fp.clientInstance.Send(hurlean.Message{"collections", db.Name})
	db.fp.responseWaitGroup.Wait()
	return db.fp.response
}

// Returns a JSON array of the grants of the user in the database
func (db *Db) ShowUserGrants() string {
	
	db.fp.responseWaitGroup.Add(1)
	db.fp.clientInstance.Send(hurlean.Message{"user grants", ""})
	db.fp.responseWaitGroup.Wait()
	return db.fp.response
}

// Returns a Collection object representing a collection in the database
func (db *Db) Collection(name string) Collection {
	
	return Collection{
		db: db,
		Name: name,
	}
}

// Create a new collection in the database
// Returns:
//	* Collection object representing the collection
//	* error (nil on success)
func (db *Db) CreateCollection(name string) (Collection, error) {
	
	db.fp.responseWaitGroup.Add(1)
	db.fp.clientInstance.Send(hurlean.Message{"create collection", name})
	db.fp.responseWaitGroup.Wait()
	
	if db.fp.response == "create collection success" {
		return Collection{db: db, Name: name}, nil
		
	} else {
		return Collection{db: db, Name: name}, fmt.Errorf("Failed to create new collection '%v' in database '%v'", name, db.Name)
	}
}

// Delete the collection in the database
// Returns an error (nil on success)
func (db *Db) DeleteCollection(name string) error {
	
	db.fp.responseWaitGroup.Add(1)
	db.fp.clientInstance.Send(hurlean.Message{"delete collection", name})
	db.fp.responseWaitGroup.Wait()
	
	if db.fp.response == "delete collection success" {
		return nil
		
	} else {
		return fmt.Errorf("Failed to delete collection '%v' in database '%v'", name, db.Name)
	}
}