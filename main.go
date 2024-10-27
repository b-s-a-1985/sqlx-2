package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	"github.com/inancgumus/screen" // clear screen
	"github.com/jmoiron/sqlx"      // sqlx library
	"github.com/joho/godotenv"     // .env library

	_ "github.com/lib/pq" // postgresql driver
)

var (
	connStr     string
	db          *sqlx.DB
	err         error
	choice      uint8
	pressAnyKey = "\nSuccess. Press any key to continue..."
)

func main() {
	// load connection string
	err := godotenv.Load()
	if err != nil {
		msg := "Error loading .env file" + err.Error()
		log.Fatal(msg)
		panic(msg)
	}
	connStr = os.Getenv("CONNSTR")

	// write log msgs into file
	f, err := os.OpenFile("log.txt", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	defer f.Close()
	log.SetOutput(f)

	// select function to run
	for {
		screen.Clear()
		screen.MoveTopLeft()
		choice = menu()
		switch choice {
		case 10:
			connect()
			db.Close()
		case 11:
			createSchema()
		case 12:
			selectSchema()
		case 13:
			createTable()
		case 14:
			insertRow()
		case 15:
			insertRowUsingStruct()
		case 16:
			queryRow()
		case 17:
			queryRows()
		case 18:
			getNumOfRows()
		case 19:
			deleteAllRows()
		default:
			fmt.Println("Bye")
			os.Exit(0)
		} // switch
	} // for
} // main

func menu() uint8 {
	var choice uint8
	menu := `
	10 connect
	11 create shema
	12 select schema
	13 create table
	14 insert row
	15 insert row using struct
	16 query row
	17 query rows
	18 get num of rows in table
	19 delete all rows
	20 quit
	`
	fmt.Println(menu)
	fmt.Print("Select 10..20: ")
	fmt.Scanln(&choice)
	fmt.Println()
	return choice
}

func connect() {
	// connect to database
	// Initialize a postgres database connection
	db, err = sqlx.Connect("postgres", connStr) // driver=postgres
	if err != nil {
		msg := "Failed to connect to the database: " + err.Error()
		log.Println(msg)
		panic(msg)
	}

	// Verify the connection to the database is still alive
	err = db.Ping()
	if err != nil {
		msg := "Failed to ping the database: " + err.Error()
		log.Println(msg)
		panic(msg)
	}
	//fmt.Print(pressAnyKey)
	//fmt.Scanln()
} // connect

func createSchema() {
	connect()
	defer db.Close()
	stmt := `CREATE SCHEMA IF NOT EXISTS test6;`
	_, err := db.Exec(stmt)
	if err != nil {
		msg := "func createSchema failed - " + err.Error()
		log.Fatal(msg)
		panic(msg)
	}
	fmt.Print(pressAnyKey)
	fmt.Scanln()
} // createSchema

func selectSchema() {
	// set current schema. does not work
	connect()
	defer db.Close()
	db.MustExec("SET search_path TO test6;")
} // selectSchema

func createTable() {
	// create table in schema tst6
	connect()
	defer db.Close()
	tableDef := `
		CREATE TABLE IF NOT EXISTS test6.place (
		id serial primary key,
		country text,
		city text,
		telcode integer)
	;`
	_, err := db.Exec(tableDef)
	if err != nil {
		msg := "func createTable failed - " + err.Error()
		log.Fatal(msg)
		panic(msg)
	}
	fmt.Println(pressAnyKey)
	fmt.Scanln()
} // createTable

func insertRow() {
	// insert single row
	connect()
	defer db.Close()

	cityState := `INSERT INTO test6.place (country, telcode) VALUES ($1, $2);`
	countryCity := `INSERT INTO test6.place (country, city, telcode) VALUES ($1, $2, $3);`

	//db.MustExec(cityState, "Hong Kong", 852)
	_, err := db.Exec(cityState, "Hong Kong", 852)
	if err != nil {
		msg := "func insertRow failed (Hong Kong) - " + err.Error()
		log.Fatal(msg)
		panic(msg)
	}

	db.MustExec(countryCity, "Hungary", "Budapest", 36)
	_, err = db.Exec(cityState, "Singapore", 65)
	if err != nil {
		msg := "func insertRow failed (Singapore) - " + err.Error()
		log.Fatal(msg)
		panic(msg)
	}

	db.MustExec(countryCity, "Ukraine", "Kiyv", 38)
	_, err = db.Exec(countryCity, "South Africa", "Johannesburg", 27)
	if err != nil {
		msg := "func insertRow failed (South Africa) - " + err.Error()
		log.Fatal(msg)
		panic(msg)
	}
} // insertRow

func insertRowUsingStruct() {
	connect()
	defer db.Close()

	type Place struct {
		//Id            int `db:"id"`
		Country       string
		City          string //sql.NullString // this field can be NULL, so we use the NullString type
		TelephoneCode int    `db:"telcode"`
	}
	//var p = Place{Country: "Austria", City: "Wien", TelephoneCode: 43}
	var p = Place{Country: "Germany", City: "Berlin", TelephoneCode: 49}

	// Insert it to products table by using struct scanning
	response, err := db.NamedExec("INSERT INTO test6.place (country, city, telcode) VALUES (:country, :city, :telcode)", &p) // OK

	if err != nil {
		msg := "func insertRowUsingStruct failed (NamedExec)- " + err.Error()
		log.Fatal(msg)
		panic(msg)
	}

	// Get affected rows count
	affectedRow, err := response.RowsAffected()
	if err != nil {
		msg := "func insertRowUsingStruct failed (RowsAffected)- " + err.Error()
		log.Fatal(msg)
		panic(msg)
	}

	log.Println("affectedRow: ", affectedRow)
} // insertRowUsingStruct

func queryRow() {
	// get a single row
	connect()
	defer db.Close()

	// the result will be fetched into this struct
	type Place struct {
		Id            int `db:"id"`
		Country       string
		City          sql.NullString // this field can be NULL, so we use the NullString type
		TelephoneCode int            `db:"telcode"`
	}
	var place Place

	// It fetches only one row from database
	err := db.Get(&place, "SELECT * FROM test6.place WHERE telcode = $1;", 852)
	if err != nil {
		msg := "func queryRow failed - " + err.Error()
		log.Fatal(msg)
		panic(msg)
	}

	// print result handling NULL value(s) in returned row
	fmt.Print(place.Country)
	fmt.Print(", ")
	if place.City.Valid { // not NULL
		fmt.Print(place.City.String)
		fmt.Print(", ")
	} else {
		fmt.Print("N.A, ") // field is NULL
	}
	fmt.Println(place.TelephoneCode)
	fmt.Println(pressAnyKey)
	fmt.Scanln()
} // queryRow

func queryRows() {
	// get multiple rows
	connect()
	defer db.Close()

	type Place struct {
		Id            int `db:"id"`
		Country       string
		City          sql.NullString // this field may be NULL
		TelephoneCode int            `db:"telcode"`
	}

	rows, err := db.Queryx("SELECT * FROM test6.place")
	if err != nil {
		msg := "func query failed - " + err.Error()
		log.Fatal(msg)
		panic(msg)
	}
	for rows.Next() {
		var p Place
		err = rows.StructScan(&p)
		if err != nil {
			msg := "func query failed (rows.Next) - " + err.Error()
			log.Fatal(msg)
			panic(msg)
		}
		//fmt.Println(p)	// print all fields
		//fmt.Printf("p.City: %v - p.City.String: %v - p.City.Valid: %v\n", p.City, p.City.String, p.City.Valid)
		fmt.Print(p.Country)
		fmt.Print(", ")
		if p.City.Valid {
			fmt.Print(p.City.String)
			fmt.Print(", ")
		} else {
			fmt.Print("N.A, ")
		}
		fmt.Println(p.TelephoneCode)

	} // for
	fmt.Println(pressAnyKey)
	fmt.Scanln()
} // queryRows

func getNumOfRows() {
	// get num of rows in table
	connect()
	defer db.Close()
	var n int // holds return value
	err = db.Get(&n, "SELECT COUNT (*) FROM test6.place;")
	if err != nil {
		msg := "func getNumOfRows failed - " + err.Error()
		log.Fatal(msg)
		panic(msg)
	}
	fmt.Println("Num of rows in table: ", n)
	fmt.Println(pressAnyKey)
	fmt.Scanln()
} // getNumOfRows

func deleteAllRows() {
	// delete all rows from table
	connect()
	defer db.Close()
	db.MustExec("DELETE FROM test6.place;")
} // deleteAllRows
