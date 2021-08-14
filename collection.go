package AcornStore


import (
	"github.com/danielblagy/hurlean"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"fmt"
	"errors"
)


type Collection struct{
	db *Db
	Name string
}

// TODO	: abstract the GJSON Path Syntax away

// Send retrieve query to the database
// 'condition' must follow the GJSON Path Syntax
// (go to https://github.com/tidwall/gjson/blob/master/SYNTAX.md to find out more about the syntax)
// leave condition empty (i.e. "") to retrieve all the contents
// Returns:
// 	* the result of the retreive query in json format
// 	* the error occurred executing the retreive query, nil on success
func (c Collection) Retrieve(condition string) (string, error) {
	
	request, _ := sjson.Set("", "collection_name", c.Name)
	request, _ = sjson.Set(request, "condition", condition)
	
	c.db.fp.responseWaitGroup.Add(1)
	c.db.fp.clientInstance.Send(hurlean.Message{"retrieve", request})
	c.db.fp.responseWaitGroup.Wait()
	
	if gjson.Valid(c.db.fp.response) {
		return c.db.fp.response, nil
		
	} else {
		return c.db.fp.response, errors.New("Server returned invalid json '" + c.db.fp.response + "'")
	}
}

// Send insert query to the database
// 'document' must be a JSON object
// Returns the error occurred executing the insert query, nil on success
func (c Collection) Insert(document string) error {
	
	// TODO : check if document has 'id' property and if it's unique
	// 		(no document with that id exists in the collection)
	//		maybe auto generate id if the property doesn't exist ??
	
	// NOTE : for some reason the document string that is passed in ClientShell program
	//		is considered invalid by gjson if double quotes are used
	
	/*if gjson.Valid(document) {
		
		return fmt.Errorf(
			"Error on inserting into '%v' collection: '%v' is not a valid json document",
			c.Name, document)
	}*/
	
	request, _ := sjson.Set("", "collection_name", c.Name)
	request, _ = sjson.Set(request, "document", document)
	
	c.db.fp.responseWaitGroup.Add(1)
	c.db.fp.clientInstance.Send(hurlean.Message{"insert", request})
	c.db.fp.responseWaitGroup.Wait()
	
	if c.db.fp.response == "insert success" {
		return nil
		
	} else {
		return fmt.Errorf("Failed to insert '%v' into '%v'", document, c.Name)
	}
}

// Send update query to the database
// 'condition' must follow the GJSON Path Syntax
// 'path' must follow the GJSON Path Syntax
// 'value' must be a valid JSON value
// (go to https://github.com/tidwall/gjson/blob/master/SYNTAX.md to find out more about the syntax)
// Returns the error occurred executing the update query, nil on success
func (c Collection) Update(condition, path, value string) error {
	
	request, _ := sjson.Set("", "collection_name", c.Name)
	request, _ = sjson.Set(request, "condition", condition)
	request, _ = sjson.Set(request, "path", path)
	request, _ = sjson.Set(request, "value", value)
	
	c.db.fp.responseWaitGroup.Add(1)
	c.db.fp.clientInstance.Send(hurlean.Message{"update", request})
	c.db.fp.responseWaitGroup.Wait()
	
	if c.db.fp.response == "update success" {
		return nil
		
	} else {
		return fmt.Errorf("Failed to update '%v' in '%v' with the new value '%v'", path, c.Name, value)
	}
}

// Send delete query to the database
// 'condition' must follow the GJSON Path Syntax
// (go to https://github.com/tidwall/gjson/blob/master/SYNTAX.md to find out more about the syntax)
// Returns the error occurred executing the delete query, nil on success
func (c Collection) Delete(condition string) error {
	
	request, _ := sjson.Set("", "collection_name", c.Name)
	request, _ = sjson.Set(request, "condition", condition)
	
	c.db.fp.responseWaitGroup.Add(1)
	c.db.fp.clientInstance.Send(hurlean.Message{"delete", request})
	c.db.fp.responseWaitGroup.Wait()
	
	if c.db.fp.response == "delete success" {
		return nil
		
	} else {
		return fmt.Errorf("Failed to delete documents in '%v' on condition '%v'", c.Name, condition)
	}
}

// Create a new collection in the database
// Returns an error (nil on success)
func (c Collection) Create() error {
	
	_, err := c.db.CreateCollection(c.Name)
	return err
}

// Delete the collection in the database
// Returns an error (nil on success)
func (c Collection) DeleteIt() error {
	
	return c.db.DeleteCollection(c.Name)
}