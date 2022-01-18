package eventD

import (
	"fmt"

	G "mtor-om/global"
)

func BodyFORMAT_DESCRIPTION_EVENT(data []byte, len uint32) {
	fmt.Printf("This is a new binlogFile\n")
}

func BodyQUERY_EVENT(data []byte, len uint32) {
	var pos uint32 = 11
	Len := G.DecodeInt(data[pos:pos+2])
	pos += 2 + Len
	dataName := data[pos:pos+3]
	pos += 3 + 1;
	SQL := data[pos:]

	fmt.Printf("DB : %s; SQL : %s\n",dataName, SQL)
}

func BodyXID_EVENT(data []byte, len uint32) {
	fmt.Printf("This binlog event over.\n")
}

func BodyTABLE_MAP_EVENT(data []byte, len uint32) {
	var pos uint32 = 0
	tableID := G.DecodeInt(data[pos:pos+6])
	pos += 6 + 2
	Len := G.DecodeInt(data[pos:pos+1])
	pos += 1 + Len + 1
	Len = G.DecodeInt(data[pos:pos+1])
	pos += 1
	tableName := data[pos:pos+Len]

	fmt.Printf("A table Struct, ID : %v; Name : %s.\n", tableID, tableName)
}

func BodyWRITE_ROWS_EVENT(data []byte, len uint32) {

}

func BodyDELETE_ROWS_EVENT(data []byte, len uint32) {

}

func BodyUPDATE_ROWS_EVENT(data []byte, len uint32) {

}

func BodyGTID_LOG_EVENT(data []byte, len uint32) {

}

func BodyROTATE_EVENT(data []byte, len uint32) {
	fmt.Printf("This binlog file over.\n")
}