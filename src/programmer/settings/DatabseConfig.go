package settings

import (
	"database/sql"
	"fmt"
	"programmer/constant"

	// _ "github.com/go-sql-driver/mysql"
	// _ "github.com/godror/godror"
	"os"

	_ "github.com/lib/pq"
)

// const (
// 	host     = "localhost"
// 	port     = 5432
// 	user     = "postgres"
// 	password = "Iipkoko@34"
// 	dbname   = "postgres"
// )

func AccessDB() *sql.DB {
	host := os.Getenv(constant.DBHost)
	port := os.Getenv(constant.DBPort)
	user := os.Getenv(constant.DBUser)
	password := os.Getenv(constant.DBPaswword)
	dbname := os.Getenv(constant.DBName)
	psqlcom := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)

	db, err := sql.Open("postgres", psqlcom)

	if err != nil {
		panic(err)
	}

	return db
}

// func AccessDB_Oracle() *sql.DB {

// 	db, err := sql.Open("godror", "<username>/<password>@service_name")

// 	if err != nil {
// 		panic(err)
// 	}

// 	return db
// }

// func AccessDB_Mysql() *sql.DB {

// 	db, err := sql.Open("mysql", "user:password@/dbname")

// 	if err != nil {
// 		panic(err)
// 	}

// 	return db
// }
