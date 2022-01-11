package main

import (
	"database/sql"
	"fmt"
	"net/http"

	"github.com/labstack/echo"
	_ "github.com/lib/pq"
	"github.com/rs/xid"
)

const (
	host     = "localhost"
	port     = 5432
	user     = "postgres"
	password = "Iipkoko@34"
	dbname   = "postgres"
)

type M map[string]interface{}

type data_pegawai struct {
	Id     string `json:"Id"`
	Nip    string `json:"Nip"`
	Nama   string `json:"Nama"`
	Status string `json:"Status"`
}

type data_pegawai_Collection struct {
	data_pegawais []data_pegawai
}

func main() {
	r := echo.New()

	r.GET("pegawai/read", func(ctx echo.Context) error {
		psqlcom := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)
		db, err := sql.Open("postgres", psqlcom)
		if err != nil {
			data := M{"Message": err.Error(), "Status": 400}
			return ctx.JSON(http.StatusOK, data)
		}

		defer db.Close()

		insertStmt := "select * from read_all_pegawai()"
		rows, err := db.Query(insertStmt)

		// Exit Jika terjadi error
		if err != nil {
			panic(err)
		}

		// clean rows
		defer rows.Close()
		result := data_pegawai_Collection{}
		for rows.Next() {
			datapegawi := data_pegawai{}
			err2 := rows.Scan(&datapegawi.Id, &datapegawi.Nama, &datapegawi.Nip, &datapegawi.Status)

			if err2 != nil {
				panic(err2)
			}
			result.data_pegawais = append(result.data_pegawais, datapegawi)
		}

		data := M{"Data": result.data_pegawais, "Massage": "berhasil ambil data", "Status": 400}
		return ctx.JSON(http.StatusOK, data)
	})

	r.GET("pegawai/read_by_id/:id", func(ctx echo.Context) error {
		psqlcom := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)
		db, err := sql.Open("postgres", psqlcom)
		if err != nil {
			data := M{"Message": err.Error(), "Status": 400}
			return ctx.JSON(http.StatusOK, data)
		}

		defer db.Close()

		Id := ctx.Param("id")

		insertStmt := "select * from read_all_pegawai_by_id($1)"
		rows, err := db.Query(insertStmt, Id)

		// Exit Jika terjadi error
		if err != nil {
			panic(err)
		}

		// clean rows
		defer rows.Close()
		result := data_pegawai_Collection{}
		for rows.Next() {
			datapegawi := data_pegawai{}
			err2 := rows.Scan(&datapegawi.Id, &datapegawi.Nama, &datapegawi.Nip, &datapegawi.Status)

			if err2 != nil {
				panic(err2)
			}
			result.data_pegawais = append(result.data_pegawais, datapegawi)
		}

		data := M{"Data": result.data_pegawais, "Massage": "berhasil ambil data", "Status": 400}
		return ctx.JSON(http.StatusOK, data)
	})

	r.POST("/insert_data", func(ctx echo.Context) error {

		psqlconn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)

		// open database
		db, err := sql.Open("postgres", psqlconn)
		if err != nil {
			data := M{"Massage": err.Error(), "Status": 400}
			return ctx.JSON(http.StatusOK, data)
		}

		// close database
		defer db.Close()

		idPegawai := xid.New().String()
		nomorRekening := ctx.FormValue("nama_rekening")
		namaRekening := ctx.FormValue("no_rekening")

		insertStmt := `insert into "PEGAWAI_REKENING"("ID_PEGAWAI", "NAMA_REKENING", "NO_REKENING") values($1, $2, $3)`
		_, e := db.Exec(insertStmt, idPegawai, nomorRekening, namaRekening)
		if e != nil {
			data := M{"Massage": e.Error(), "Status": 300}
			return ctx.JSON(http.StatusOK, data)
		}

		// check db
		err = db.Ping()
		if err != nil {
			return ctx.String(http.StatusOK, err.Error())
		}

		data := M{"Massage": "BERHASIL", "Status": 200}
		return ctx.JSON(http.StatusOK, data)
	})

	r.PUT("/update_data/:id_pegawai", func(ctx echo.Context) error {

		psqlconn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)

		// open database
		db, err := sql.Open("postgres", psqlconn)
		if err != nil {
			data := M{"Massage": err.Error(), "Status": 400}
			return ctx.JSON(http.StatusOK, data)
		}

		// close database
		defer db.Close()

		idPegawai := ctx.Param("id_pegawai")
		nomorRekening := ctx.FormValue("nama_rekening")
		namaRekening := ctx.FormValue("no_rekening")

		insertStmt := `select * from update_pegawai_rekening($1, $2, $3)`
		_, e := db.Exec(insertStmt, idPegawai, nomorRekening, namaRekening)
		if e != nil {
			data := M{"Massage": e.Error(), "Status": 300}
			return ctx.JSON(http.StatusOK, data)
		}

		// check db
		err = db.Ping()
		if err != nil {
			return ctx.String(http.StatusOK, err.Error())
		}

		data := M{"Massage": "BERHASIL", "Status": 200}
		return ctx.JSON(http.StatusOK, data)
	})

	r.DELETE("/delete_data/:id_pegawai", func(ctx echo.Context) error {

		psqlconn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)

		// open database
		db, err := sql.Open("postgres", psqlconn)
		if err != nil {
			data := M{"Massage": err.Error(), "Status": 400}
			return ctx.JSON(http.StatusOK, data)
		}

		// close database
		defer db.Close()

		idPegawai := ctx.Param("id_pegawai")

		insertStmt := `select * from delete_pegawai_rekening($1)`
		_, e := db.Exec(insertStmt, idPegawai)
		if e != nil {
			data := M{"Massage": e.Error(), "Status": 300}
			return ctx.JSON(http.StatusOK, data)
		}

		// check db
		err = db.Ping()
		if err != nil {
			return ctx.String(http.StatusOK, err.Error())
		}

		data := M{"Massage": "BERHASIL", "Status": 200}
		return ctx.JSON(http.StatusOK, data)
	})
	r.Start(":9000")
}
