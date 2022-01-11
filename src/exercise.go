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
	host       = "localhost"
	port       = 5432
	user       = "postgres"
	password   = "gijang123"
	dbname     = "programmer"
	dbname_dba = "DBA"
)

type M map[string]interface{}

type data_diri struct {
	Id        string `json:"Id"`
	Nama_user string `json:"Nama_user"`
	Pass      string `json:"Pass"`
}

type data_diri_Collection struct {
	data_diris []data_diri
}

func main() {
	r := echo.New()

	r.GET("/user/read", func(ctx echo.Context) error {
		psqlcom := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)

		db, err := sql.Open("postgres", psqlcom)
		if err != nil {
			data := M{"Message": err.Error(), "Status": 400}
			return ctx.JSON(http.StatusOK, data)
		}
		defer db.Close()

		insertStmt := "select * from read_all_user() "
		rows, err := db.Query(insertStmt)
		// Exit jika terjadi error
		if err != nil {
			panic(err)
		}
		// clean rows
		defer rows.Close()

		result := data_diri_Collection{}
		for rows.Next() {
			datadiri := data_diri{}
			err2 := rows.Scan(&datadiri.Id, &datadiri.Nama_user, &datadiri.Pass)

			// Exit jika error
			if err2 != nil {
				panic(err2)
			}
			result.data_diris = append(result.data_diris, datadiri)

		}
		data := M{"Data": result.data_diris, "Message": "berhasil ambil data", "Status": 400}
		return ctx.JSON(http.StatusOK, data)
	})

	r.GET("/user/read/:id_user", func(ctx echo.Context) error {
		psqlcom := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)

		db, err := sql.Open("postgres", psqlcom)
		if err != nil {
			data := M{"Message": err.Error(), "Status": 400}
			return ctx.JSON(http.StatusOK, data)
		}
		defer db.Close()

		id_user := ctx.Param("id_user")

		insertStmt := "select * from read_all_user_by_id($1) "
		rows, err := db.Query(insertStmt, id_user)
		// Exit jika terjadi error
		if err != nil {
			panic(err)
		}
		// clean rows
		defer rows.Close()

		result := data_diri_Collection{}
		for rows.Next() {
			datadiri := data_diri{}
			err2 := rows.Scan(&datadiri.Id, &datadiri.Nama_user, &datadiri.Pass)

			// Exit jika error
			if err2 != nil {
				panic(err2)
			}
			result.data_diris = append(result.data_diris, datadiri)

		}
		data := M{"Data": result.data_diris, "Message": "berhasil ambil data", "Status": 400}
		return ctx.JSON(http.StatusOK, data)
	})

	r.POST("/user/", func(ctx echo.Context) error {
		psqlcom := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)

		db, err := sql.Open("postgres", psqlcom)
		if err != nil {
			data := M{"Message": err.Error(), "Status": 400}
			return ctx.JSON(http.StatusOK, data)
		}
		defer db.Close()

		id_user := xid.New().String()
		username := ctx.FormValue("username")
		password := ctx.FormValue("password")

		// insertStmt := "insert into "user" (id_user, username, password) values ($1, $2, $3)"

		insertStmt_funct := "select * from create_user ($1, $2, $3)"

		_, err = db.Exec(insertStmt_funct, id_user, username, password)
		if err != nil {
			data := M{"Message": err.Error(), "Status": 300}
			return ctx.JSON(http.StatusOK, data)
		}
		data := M{"Message": "berhasil insert data", "Status": 200}
		return ctx.JSON(http.StatusOK, data)
	})

	r.PUT("/user/update/:id_user", func(ctx echo.Context) error {
		psqlcom := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)

		db, err := sql.Open("postgres", psqlcom)
		if err != nil {
			data := M{"Message": err.Error(), "Status": 400}
			return ctx.JSON(http.StatusOK, data)
		}
		defer db.Close()

		id_user := ctx.Param("id_user")
		username := ctx.FormValue("username")
		password := ctx.FormValue("password")

		// insertStmt := "insert into "user" (id_user, username, password) values ($1, $2, $3)"

		insertStmt_funct := "select * from update_user ($1, $2, $3)"

		_, err = db.Exec(insertStmt_funct, id_user, username, password)
		if err != nil {
			data := M{"Message": err.Error(), "Status": 300}
			return ctx.JSON(http.StatusOK, data)
		}
		data := M{"Message": "berhasil update data", "Status": 200}
		return ctx.JSON(http.StatusOK, data)
	})

	r.DELETE("/user/delete", func(ctx echo.Context) error {
		psqlcom := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)

		db, err := sql.Open("postgres", psqlcom)
		if err != nil {
			data := M{"Message": err.Error(), "Status": 400}
			return ctx.JSON(http.StatusOK, data)
		}
		defer db.Close()

		id_user := ctx.Param("id_user")

		// insertStmt := "insert into "user" (id_user, username, password) values ($1, $2, $3)"

		insertStmt_funct := "select * from delete_user ($1)"

		_, err = db.Exec(insertStmt_funct, id_user)
		if err != nil {
			data := M{"Message": err.Error(), "Status": 300}
			return ctx.JSON(http.StatusOK, data)
		}
		data := M{"Message": "berhasil delete data", "Status": 200}
		return ctx.JSON(http.StatusOK, data)
	})

	r.Start(":9000")
	// r.POST("/updatedb", func(ctx echo.Context) error {
	// 	psqlcom := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)

	// 	db, err := sql.Open("postgres", psqlcom)
	// 	if err != nil {
	// 		return ctx.String(http.StatusOK, err.Error())
	// 	}
	// 	defer db.Close()

	// 	nama := ctx.FormValue("nama")
	// 	umur := ctx.FormValue("umur")
	// 	nomorktp := ctx.FormValue("nomorktp")

	// 	insertStmt := "update data_diri set nama = $1, umur = $2 where nomorktp = $3;"

	// 	_, e := db.Exec(insertStmt, nama, umur, nomorktp)
	// 	if e != nil {
	// 		return ctx.String(http.StatusOK, e.Error())
	// 	}

	// 	return ctx.String(http.StatusOK, "berhasil")
	// })

	// r.POST("/deletedb", func(ctx echo.Context) error {
	// 	psqlcom := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)

	// 	db, err := sql.Open("postgres", psqlcom)
	// 	if err != nil {
	// 		return ctx.String(http.StatusOK, err.Error())
	// 	}
	// 	defer db.Close()

	// 	nomorktp := ctx.FormValue("nomorktp")

	// 	insertStmt := "delete from data_diri where nomorktp = $1;"

	// 	_, e := db.Exec(insertStmt, nomorktp)
	// 	if e != nil {
	// 		return ctx.String(http.StatusOK, e.Error())
	// 	}

	// 	return ctx.String(http.StatusOK, "berhasil")
	// })

	// r.POST("/selectdb", func(ctx echo.Context) error {
	// 	psqlcom := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)

	// 	db, err := sql.Open("postgres", psqlcom)
	// 	if err != nil {
	// 		return ctx.String(http.StatusOK, err.Error())
	// 	}
	// 	defer db.Close()

	// 	nomorktp := ctx.FormValue("nomorktp")

	// 	insertStmt := "select * from data_diri where nomorktp = $1"

	// 	var ddr datadiri
	// 	e := db.QueryRow(insertStmt, nomorktp).Scan(&ddr.nomorktp, &ddr.nama, &ddr.umur)
	// 	if e != nil {
	// 		return ctx.String(http.StatusOK, e.Error())
	// 	}

	// 	return ctx.String(http.StatusOK, ddr.nama)
	// })

	// r.POST("/funcregisterdb", func(ctx echo.Context) error {
	// 	psqlcom := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)

	// 	db, err := sql.Open("postgres", psqlcom)
	// 	if err != nil {
	// 		return ctx.String(http.StatusOK, err.Error())
	// 	}
	// 	defer db.Close()

	// 	nomorktp := ctx.FormValue("nomorktp")
	// 	nama := ctx.FormValue("nama")
	// 	umur := ctx.FormValue("umur")

	// 	insertStmt := "select * from registerktp ($1, $2, $3)"

	// 	_, e := db.Exec(insertStmt, nomorktp, nama, umur)
	// 	if e != nil {
	// 		return ctx.String(http.StatusOK, e.Error())
	// 	}

	// 	return ctx.String(http.StatusOK, "berhasil")
	// })

	// r.POST("/funcupdatedb", func(ctx echo.Context) error {
	// 	psqlcom := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)

	// 	db, err := sql.Open("postgres", psqlcom)
	// 	if err != nil {
	// 		return ctx.String(http.StatusOK, err.Error())
	// 	}
	// 	defer db.Close()

	// 	nomorktp := ctx.FormValue("nomorktp")
	// 	nama := ctx.FormValue("nama")
	// 	umur := ctx.FormValue("umur")

	// 	insertStmt := "select * from updatektp ($1,$2,$3);"

	// 	_, e := db.Exec(insertStmt, nomorktp, nama, umur)
	// 	if e != nil {
	// 		return ctx.String(http.StatusOK, e.Error())
	// 	}

	// 	return ctx.String(http.StatusOK, "berhasil")
	// })

	// r.POST("/funcdeletedb", func(ctx echo.Context) error {
	// 	psqlcom := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)

	// 	db, err := sql.Open("postgres", psqlcom)
	// 	if err != nil {
	// 		return ctx.String(http.StatusOK, err.Error())
	// 	}
	// 	defer db.Close()

	// 	nomorktp := ctx.FormValue("nomorktp")

	// 	insertStmt := "select * from deletektp ($1)"

	// 	_, e := db.Exec(insertStmt, nomorktp)
	// 	if e != nil {
	// 		return ctx.String(http.StatusOK, e.Error())
	// 	}

	// 	return ctx.String(http.StatusOK, "berhasil")
	// })
	// r.Start(":9000")
}
