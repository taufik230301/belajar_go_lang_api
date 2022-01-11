package model

import (
	"database/sql"
	"programmer/entity"

	_ "github.com/lib/pq"
)

func GetAllBarang(db *sql.DB) ([]entity.Barang, error) {

	insertStmt := "select * from read_all_barang()"
	rows, err := db.Query(insertStmt)
	// Exit jika terjadi error
	if err != nil {
		return []entity.Barang{}, err
	}
	// clean rows
	defer rows.Close()

	Barangresult := []entity.Barang{}
	for rows.Next() {
		barang := entity.Barang{}
		err2 := rows.Scan(&barang.IdBarang, &barang.NamaBarang, &barang.Deskripsi, &barang.Lokasi, &barang.FotoBarang)

		// Exit jika error
		if err2 != nil {
			return []entity.Barang{}, err
		}
		Barangresult = append(Barangresult, barang)

	}
	return Barangresult, err
}

func AddBarang(db *sql.DB, data entity.Barang) (bool, error) {

	insertStmt := " select * from create_barang($1,$2,$3,$4,$5) "
	_, err := db.Query(insertStmt,
		data.IdBarang,
		data.NamaBarang,
		data.Deskripsi,
		data.Lokasi,
		data.FotoBarang)
	// Exit jika terjadi error
	if err != nil {
		return false, err
	}

	return true, err
}

func UpdateBarang(db *sql.DB, data entity.Barang) (bool, error) {

	insertStmt := " select * from update_barang($1,$2,$3,$4,$5) "
	_, err := db.Query(insertStmt,
		data.IdBarang,
		data.NamaBarang,
		data.Deskripsi,
		data.Lokasi,
		data.FotoBarang)
	// Exit jika terjadi error
	if err != nil {
		return false, err
	}

	return true, err
}

func DeleteBarang(db *sql.DB, data entity.Barang) (bool, error) {

	insertStmt := " select * from delete_barang($1) "
	_, err := db.Query(insertStmt,
		data.IdBarang)
	// Exit jika terjadi error
	if err != nil {
		return false, err
	}

	return true, err
}
