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

type M map[string]interface{}

func main() {
	r := echo.New()

	r.POST("/insert", func(ctx echo.Context) error {

		psqlconn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)

		// open database
		db, err := sql.Open("postgres", psqlconn)
		if err != nil {
			data := M{"Massage": err.Error(), "Status": 400}
			return ctx.JSON(http.StatusOK, data)
		}

		// close database
		defer db.Close()

		id_pegawai := ctx.FormValue("id_pegawai")
		nama_rekening := ctx.FormValue("nama_rekening")
		no_rekening := ctx.FormValue("no_rekening")

		insertStmt := `insert into "PEGAWAI_REKENING"("ID_PEGAWAI", "NAMA_REKENING", "NO_REKENING") VALUES($1, $2, $3)`
		_, e := db.Exec(insertStmt, id_pegawai, nama_rekening, no_rekening)
		if e != nil {
			data := M{"Massage": err.Error(), "Status": 200}
			return ctx.JSON(http.StatusOK, data)
		}

		// check db
		err = db.Ping()
		if err != nil {
			return ctx.String(http.StatusOK, err.Error())
		}

		data := "Data Berhasil Di input"
		return ctx.String(http.StatusOK, data)
	})

	r.Start(":9000")
}
