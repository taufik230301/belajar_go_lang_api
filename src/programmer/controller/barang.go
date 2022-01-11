package controller

import (
	"net/http"
	"programmer/entity"
	"programmer/model"
	"programmer/settings"

	"github.com/labstack/echo"
	_ "github.com/lib/pq"
)

func GetBarang(ctx echo.Context) error {
	db := settings.AccessDB()
	result, err := model.GetAllBarang(db)
	if err != nil {
		data := entity.M{"Data": result, "Message": err.Error(), "Status": 400}
		return ctx.JSON(http.StatusOK, data)
	} else {
		data := entity.M{"Data": result, "Message": "berhasil ambil data", "Status": 400}
		return ctx.JSON(http.StatusOK, data)
	}

}
func AddBarang(ctx echo.Context) error {
	db := settings.AccessDB()
	var barang entity.Barang

	barang.IdBarang = ctx.FormValue("idbarang")
	barang.NamaBarang = ctx.FormValue("namabarang")
	barang.Deskripsi = ctx.FormValue("deskripsi")
	barang.Lokasi = ctx.FormValue("lokasi")
	barang.FotoBarang = ctx.FormValue("fotobarang")
	_, err := model.AddBarang(db, barang)
	if err != nil {
		data := entity.M{
			"Message": err.Error(),
			"Status":  500}
		return ctx.JSON(http.StatusOK, data)
	} else {
		data := entity.M{
			"Message": "Berhasil Memasukan Barang",
			"Status":  400}
		return ctx.JSON(http.StatusOK, data)
	}
}
func UpdateBarang(ctx echo.Context) error {
	db := settings.AccessDB()
	var barang entity.Barang

	barang.IdBarang = ctx.FormValue("idbarang")
	barang.NamaBarang = ctx.FormValue("namabarang")
	barang.Deskripsi = ctx.FormValue("deskripsi")
	barang.Lokasi = ctx.FormValue("lokasi")
	barang.FotoBarang = ctx.FormValue("fotobarang")
	_, err := model.UpdateBarang(db, barang)
	if err != nil {
		data := entity.M{
			"Message": err.Error(),
			"Status":  500}
		return ctx.JSON(http.StatusOK, data)
	} else {
		data := entity.M{
			"Message": "Berhasil Mengupdate Barang",
			"Status":  400}
		return ctx.JSON(http.StatusOK, data)
	}
}

func DeleteBarang(ctx echo.Context) error {
	db := settings.AccessDB()
	var barang entity.Barang

	barang.IdBarang = ctx.FormValue("idbarang")

	_, err := model.DeleteBarang(db, barang)
	if err != nil {
		data := entity.M{
			"Message": err.Error(),
			"Status":  500}
		return ctx.JSON(http.StatusOK, data)
	} else {
		data := entity.M{
			"Message": "Berhasil Menghapus Barang",
			"Status":  400}
		return ctx.JSON(http.StatusOK, data)
	}
}
