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
	dbname   = "aplikasi_mahasiswa"
)

type M map[string]interface{}

type mahasiswa_data struct {
	Id            string `json:"p_id_mahasiswa"` // json nama yang ada di database
	Nama          string `json:"p_nama_mahasiswa"`
	Tanggal_Lahir string `json:"p_tgl_lahir"`
	Email         string `json:"p_email"`
	Nama_Jurusan  string `json:"p_nama_jurusan"`
	Alamat        string `json:"p_alamat"`
	Foto          string `json:"p_foto_mahasiswa"`
}

type user_collect struct {
	users []mahasiswa_data `json:"mahasiswa_data"`
}

type jurusan_data struct {
	Id          string `json:"p_id_jurusan"` // json nama yang ada di database
	NamaJurusan string `json:"p_nama_jurusan"`
}

type jurusan_collect struct {
	jurusans []jurusan_data `json:"jurusan_data"`
}

func main() {

	r := echo.New()

	r.GET("/jurusan/getData/", func(ctx echo.Context) error {

		psqlcom := fmt.Sprintf("host=%s port=%d user =%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)

		db, err := sql.Open("postgres", psqlcom)

		if err != nil {
			data := M{"pesan": err.Error(), "Status": 400}
			return ctx.JSON(http.StatusOK, data)
		}
		defer db.Close()
		insertStmt := "select * from read_all_jurusan()"
		rows, error := db.Query(insertStmt)
		//exit jika terjadi error
		if error != nil {
			panic(error)
		}
		defer rows.Close()
		result := jurusan_collect{}

		for rows.Next() {
			jurusan := jurusan_data{}

			error2 := rows.Scan(&jurusan.Id, &jurusan.NamaJurusan)

			//exit jika error
			if error2 != nil {
				panic(error2)
			}

			result.jurusans = append(result.jurusans, jurusan)

		}
		//return ctx.JSON(http.StatusOK, result.users)
		data := M{"Data": result.jurusans, "pesan": "Berhasil", "Status": 200}
		return ctx.JSON(http.StatusOK, data)
	})

	r.GET("/mahasiswa/getData/", func(ctx echo.Context) error {
		psqlcom := fmt.Sprintf("host=%s port=%d user =%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)
		db, err := sql.Open("postgres", psqlcom)
		if err != nil {
			data := M{"pesan": err.Error(), "Status": 400}
			return ctx.JSON(http.StatusOK, data)
		}
		defer db.Close()

		insertStmt := "select * from read_all_mahasiswa()"
		rows, error := db.Query(insertStmt)

		//exit jika terjadi error
		if error != nil {
			panic(error)
		}

		defer rows.Close()
		result := user_collect{}

		for rows.Next() {
			mahasiswa := mahasiswa_data{}

			error2 := rows.Scan(&mahasiswa.Id, &mahasiswa.Nama, &mahasiswa.Tanggal_Lahir, &mahasiswa.Email, &mahasiswa.Nama_Jurusan, &mahasiswa.Alamat, &mahasiswa.Foto)
			//exit jika error
			if error2 != nil {
				panic(error2)
			}

			if len(mahasiswa.Tanggal_Lahir) >= 10 {
				mahasiswa.Tanggal_Lahir = mahasiswa.Tanggal_Lahir[:10]
			}

			result.users = append(result.users, mahasiswa)

		}

		//return ctx.JSON(http.StatusOK, result.users)
		data := M{"Data": result.users, "pesan": "Berhasil", "Status": 200}
		return ctx.JSON(http.StatusOK, data)
	})

	r.PUT("/mahasiswa/update/:id_mahasiswa", func(ctx echo.Context) error { // use params
		psqlcom := fmt.Sprintf("host=%s port=%d user =%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)
		db, err := sql.Open("postgres", psqlcom)
		if err != nil {
			//return ctx.String(http.StatusOK, err.Error())
			data := M{"pesan": err.Error(), "Status": 400}
			return ctx.JSON(http.StatusOK, data)
		}
		defer db.Close()

		//id_user := ctx.FormValue("id_user")
		// use [params]
		nama := ctx.FormValue("nama")
		tanggalLahir := ctx.FormValue("tanggal_lahir")
		email := ctx.FormValue("email")
		idJurusan := ctx.FormValue("id_jurusan")
		alamat := ctx.FormValue("alamat")
		FotoMahasiswa := ctx.FormValue("foto_mahasiswa")
		idMahasiswa := ctx.Param("id_mahasiswa")

		insertStmt_funct := "select * from update_mahasiswa ($1, $2, $3, $4, $5, $6, $7)"

		_, e := db.Exec(insertStmt_funct, nama, tanggalLahir, email, idJurusan, alamat, FotoMahasiswa, idMahasiswa)
		if e != nil {
			//return ctx.String(http.StatusOK, e.Error())
			data := M{"pesan": e.Error(), "Status": 400}
			return ctx.JSON(http.StatusOK, data)
		}
		data := M{"pesan": "Data berhasil di update", "Status": 200}
		return ctx.JSON(http.StatusOK, data)
	})

	r.POST("/mahasiswa/insert/", func(ctx echo.Context) error {
		psqlcom := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)

		db, err := sql.Open("postgres", psqlcom)
		if err != nil {
			data := M{"Message": err.Error(), "Status": 400}
			return ctx.JSON(http.StatusOK, data)
		}
		defer db.Close()

		id_mahasiswa := xid.New().Counter()
		nama := ctx.FormValue("nama")
		tanggalLahir := ctx.FormValue("tanggal_lahir")
		email := ctx.FormValue("email")
		idJurusan := ctx.FormValue("id_jurusan")
		alamat := ctx.FormValue("alamat")
		FotoMahasiswa := ctx.FormValue("foto_mahasiswa")

		insertStmt_func := "select * from create_mahasiswa ($1, $2, $3, $4, $5, $6, $7)"

		_, e := db.Exec(insertStmt_func, id_mahasiswa, nama, tanggalLahir, email, idJurusan, alamat, FotoMahasiswa)
		if e != nil {
			data := M{"pesan": err.Error(), "Status": 300}
			return ctx.JSON(http.StatusOK, data)
		}

		data := M{"pesan": "berhasil insert data", "Status": 200}
		return ctx.JSON(http.StatusOK, data)
	})

	r.DELETE("/mahasiswa/delete/:id_mahasiswa", func(ctx echo.Context) error {
		psqlcom := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)

		db, err := sql.Open("postgres", psqlcom)
		if err != nil {
			data := M{"Message": err.Error(), "Status": 400}
			return ctx.JSON(http.StatusOK, data)
		}
		defer db.Close()

		id_mahasiswa := ctx.Param("id_mahasiswa")
		//nama_merk := ctx.FormValue("nama_merk")
		// nama_merk := xid.New().String()

		deleteStmt_func := "select * from delete_mahasiswa($1)"
		//insertStmt_func := "select * from create_user ($1, $2, $3)"

		_, e := db.Exec(deleteStmt_func, id_mahasiswa)
		if e != nil {
			data := M{"Message": e.Error(), "Status": 300}
			return ctx.JSON(http.StatusOK, data)
		}

		data := M{"Message": "berhasil hapus data", "Status": 200}
		return ctx.JSON(http.StatusOK, data)
	})

	r.Start(":9000")
}
