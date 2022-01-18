package eventH

import (
	"fmt"

	D "mtor-om/eventD"
	G "mtor-om/global"

	// "github.com/flike/kingshard/mysql"
)

func SplitHeader() {
	// fmt.Printf("event : %v\n",G.GConf.Event[5])
	// timeStamp := G.GConf.Event[1:5]
    eventType := G.GConf.Event[5]
    // serverId := G.GConf.Event[6:10]
    eventSize := G.DecodeInt(G.GConf.Event[10:14])
    // logPos := G.GConf.Event[14:18]
    flags := G.DecodeInt(G.GConf.Event[18:20])

	if flags == 1 {
		fmt.Printf("this binlog dont close.")
		return 
	}

	switch eventType {
	case G.FORMAT_DESCRIPTION_EVENT :
		D.BodyFORMAT_DESCRIPTION_EVENT(G.GConf.Event[20:], eventSize)

	case G.QUERY_EVENT :
		D.BodyQUERY_EVENT(G.GConf.Event[20:], eventSize)
		
	case G.XID_EVENT :
		D.BodyXID_EVENT(G.GConf.Event[20:], eventSize)
		
	case G.TABLE_MAP_EVENT :
		D.BodyTABLE_MAP_EVENT(G.GConf.Event[20:], eventSize)
		
	case G.WRITE_ROWS_EVENT :
		D.BodyWRITE_ROWS_EVENT(G.GConf.Event[20:], eventSize)

	case G.DELETE_ROWS_EVENT :
		D.BodyDELETE_ROWS_EVENT(G.GConf.Event[20:], eventSize)

	case G.UPDATE_ROWS_EVENT :
		D.BodyUPDATE_ROWS_EVENT(G.GConf.Event[20:], eventSize)

	case G.GTID_LOG_EVENT :
		D.BodyGTID_LOG_EVENT(G.GConf.Event[20:], eventSize)

	case G.ROTATE_EVENT :
		D.BodyROTATE_EVENT(G.GConf.Event[20:], eventSize)

	default :
		fmt.Printf("Sorry! this is a undefined logtype in code.\n")
	}
}