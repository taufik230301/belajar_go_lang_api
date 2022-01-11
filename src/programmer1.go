package main

import (
	"fmt"
	"net/http"

	"github.com/labstack/echo"
)

type M map[string]interface{}

func main() {
	r := echo.New()
	r.GET("/string", func(ctx echo.Context) error {
		data := "Halo Dunia !"
		return ctx.String(http.StatusOK, data)
	})

	r.GET("/html", func(ctx echo.Context) error {
		data := "<h1>Halo Dunia !</h1>"
		return ctx.HTML(http.StatusOK, data)
	})

	r.GET("/redirect", func(ctx echo.Context) error {
		return ctx.Redirect(http.StatusTemporaryRedirect, "/html")
	})

	r.GET("/json", func(ctx echo.Context) error {
		data := M{"Message": "Taufik", "Counter": 1}
		return ctx.JSON(http.StatusOK, data)
	})

	r.GET("/hellostring", func(ctx echo.Context) error {
		name := ctx.QueryParam("name")
		data := fmt.Sprintf("Hello %s", name)
		return ctx.String(http.StatusOK, data)
	})

	r.GET("/hellojson", func(ctx echo.Context) error {
		name := ctx.QueryParam("name")
		data := M{"Name": name}
		return ctx.JSON(http.StatusOK, data)
	})

	r.GET("/hellourl/:name", func(ctx echo.Context) error {
		name := ctx.Param("name")
		data := M{"Name": name}
		return ctx.JSON(http.StatusOK, data)
	})
	r.GET("/helloform", func(ctx echo.Context) error {
		name := ctx.FormValue("name")
		data := M{"Name": name}
		return ctx.JSON(http.StatusOK, data)
	})

	r.Start(":9000")
}
