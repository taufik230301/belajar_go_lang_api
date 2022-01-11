package model

import (
	"database/sql"
	"programmer/entity"

	_ "github.com/lib/pq"
)

func GetUserAll(db *sql.DB) entity.Data_pegawai_Collection {

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
	return result
}
