package main

import (
	"os"
	"programmer/controller"

	"github.com/labstack/echo"
)

func main() {
	r := echo.New()
	os.Setenv("DB_HOST", "localhost")
	os.Setenv("DB_PORT", "5432")
	os.Setenv("DB_NAME", "classico_tools")
	os.Setenv("DB_USER", "postgres")
	os.Setenv("DB_PASSWORD", "Iipkoko@34")
	// r.GET("/user/read", controller.Read_all_user)
	r.GET("/barang", controller.GetBarang)
	r.POST("/tambah_barang", controller.AddBarang)

	r.Start(":9000")
}
