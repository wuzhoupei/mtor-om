package main

import (
	"fmt"

	G "mtor-om/global"
	"mtor-om/linkDB"
)

func main() {
	G.InitConfig()
	linkDB.Init()
	defer linkDB.CloseDB()
	G.GetPosFile()
	linkDB.Dump()

	fmt.Printf("File : %s ; Pos : %v ;\n", G.GConf.BinlogFile, G.GConf.BinlogPos)
}