package main

import "fmt"

var (
	myBytes = []byte{'H', 'e', 'l', 'l', 'o'}
)

func main() {
	arr1 := [][]byte{myBytes, {'o'}}
	arr2 := [][]byte{{'h'}, myBytes}
	fmt.Println(arr1)
	fmt.Println(arr2)
	arr1[0][1] = 'a'
	fmt.Println(arr2)
}
