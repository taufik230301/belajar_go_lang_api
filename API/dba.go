package main

import (
	"database/sql"
	"fmt"
	"net/http"

	"github.com/labstack/echo"
	_ "github.com/lib/pq"
)

const (
	host     = "localhost"
	port     = 5432
	user     = "postgres"
	password = "Iipkoko@34"
	dbname   = "postgres"
)

func main() {
	r := echo.New()

	r.GET("/testsql", func(ctx echo.Context) error {

		psqlconn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)

		// open database
		db, err := sql.Open("postgres", psqlconn)
		if err != nil {

			return ctx.String(http.StatusOK, err.Error())
		}

		// close database
		defer db.Close()

		insertStmt := `insert into "PEGAWAI_REKENING"("ID_PEGAWAI", "NAMA_REKENING","NO_REKENING") values('1', 'TAUFIKHAKIM','1234')`
		_, e := db.Exec(insertStmt)
		if e != nil {

			return ctx.String(http.StatusOK, e.Error())
		}

		// check db
		err = db.Ping()
		if err != nil {
			return ctx.String(http.StatusOK, err.Error())
		}

		data := "Connected"
		return ctx.String(http.StatusOK, data)
	})

	r.Start(":9000")
}
