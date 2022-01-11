package main

import (
	"fmt"
)

func main() {
	i := 3
	if i == 3 {
		fmt.Println("angka 3")
	} else if i == 4 {
		fmt.Println("angka  4")
	} else {
		fmt.Println("angka bukan 3 dan 4")
	}

	var point = 6

	switch point {
	case 8:
		fmt.Println("perfect")
	case 7, 3, 1:
		fmt.Println("awesome")
	default:
		fmt.Println("not bad")
	}

	var fruits = [4]string{"apple", "grape", "banana", "melon"}

	for i, fruit := range fruits {
		fmt.Printf("elemen %d : %s\n", i, fruit)
	}

	fruits = [4]string{"apple", "grape", "banana", "melon"}

	for _, fruit := range fruits {
		fmt.Printf("elemen :%s\n", fruit)
	}
}
