package controller

import (
	"net/http"
	"programmer/entity"
	"programmer/model"
	"programmer/settings"

	"github.com/labstack/echo"
	_ "github.com/lib/pq"
)

func Read_all_user(ctx echo.Context) error {
	db := settings.AccessDB()
	result := model.GetUserAll(db)

	data := entity.M{"Data": result.Data_pegawais, "Message": "berhasil ambil data", "Status": 400}
	return ctx.JSON(http.StatusOK, data)
}
