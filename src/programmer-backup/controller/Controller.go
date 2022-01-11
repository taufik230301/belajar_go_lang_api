package controller

import (
	"net/http"
	"programmer/entity"
	"programmer/settings"

	"github.com/labstack/echo"
	_ "github.com/lib/pq"
)

func Read_all_user(ctx echo.Context) error {
	db := settings.AccessDB()

	insertStmt := "select * from read_all_pegawai() "
	rows, err := db.Query(insertStmt)
	// Exit jika terjadi error
	if err != nil {
		panic(err)
	}
	// clean rows
	defer rows.Close()

	result := entity.Data_pegawai_Collection{}
	for rows.Next() {
		datadiri := entity.Data_pegawai{}
		err2 := rows.Scan(&datadiri.Id, &datadiri.Nip, &datadiri.Nama, &datadiri.Status)

		// Exit jika error
		if err2 != nil {
			panic(err2)
		}
		result.Data_pegawais = append(result.Data_pegawais, datadiri)

	}
	data := entity.M{"Data": result.Data_pegawais, "Message": "berhasil ambil data", "Status": 400}
	return ctx.JSON(http.StatusOK, data)
}
