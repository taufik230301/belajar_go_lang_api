package main

import (
	"programmer/controller"

	"github.com/labstack/echo"
)

func main() {
	r := echo.New()

	r.GET("/user/read", controller.Read_all_user)

	r.Start(":9000")
}
