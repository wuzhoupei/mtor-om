package linkDB

import (
	"fmt"
	"log"
	// "net"

	G "mtor-om/global"

	// "github.com/flike/kingshard/mysql"
    "mtor-om/include/backend"
)

func newConn() {
	c := new(backend.Conn)
	
	if err := c.Connect(G.GConf.Address+":"+G.GConf.Port, G.GConf.User, G.GConf.Password, "mysql"); err != nil {
		log.Panicln("Connect failed  ",err)
	} else {
		fmt.Println("Connect success !")
	}

	G.GConf.DBc = c
}

func doChecksum() {
	// do checksum
	_, err := G.GConf.DBc.Execute("set @master_binlog_checksum= @@global.binlog_checksum")
	if err != nil {
		log.Panicln("Error : ", err)
	}
}

func Init() {
	newConn()
	doChecksum()
	return 
}

func CloseDB() {
	return 
	// G.GConf.DBc,Close()
}