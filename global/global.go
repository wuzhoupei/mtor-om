package global

import (
	"os"
	"io"
	"log"
	"fmt"
	"bufio"
	"strconv"
	
	"github.com/flike/kingshard/mysql"
    "mtor-om/include/backend"
)

const ConfPath = "./global/config.conf"

const (
	UNKNOWN_EVENT= 0
	START_EVENT_V3= 1
	QUERY_EVENT= 2
	STOP_EVENT= 3
	ROTATE_EVENT= 4
	INTVAR_EVENT= 5
	LOAD_EVENT= 6
	SLAVE_EVENT= 7
	CREATE_FILE_EVENT= 8
	APPEND_BLOCK_EVENT= 9
	EXEC_LOAD_EVENT= 10
	DELETE_FILE_EVENT= 11
	/**
	  NEW_LOAD_EVENT is like LOAD_EVENT except that it has a longer
	  sql_ex, allowing multibyte TERMINATED BY etc; both types share the
	  same class (Load_event)
	*/
	NEW_LOAD_EVENT= 12
	RAND_EVENT= 13
	USER_VAR_EVENT= 14
	FORMAT_DESCRIPTION_EVENT= 15
	XID_EVENT= 16
	BEGIN_LOAD_QUERY_EVENT= 17
	EXECUTE_LOAD_QUERY_EVENT= 18
  
	TABLE_MAP_EVENT = 19
  
	/**
	  The PRE_GA event numbers were used for 5.1.0 to 5.1.15 and are
	  therefore obsolete.
	 */
	PRE_GA_WRITE_ROWS_EVENT = 20
	PRE_GA_UPDATE_ROWS_EVENT = 21
	PRE_GA_DELETE_ROWS_EVENT = 22
  
	/**
	  The V1 event numbers are used from 5.1.16 until mysql-trunk-xx
	*/
	WRITE_ROWS_EVENT_V1 = 23
	UPDATE_ROWS_EVENT_V1 = 24
	DELETE_ROWS_EVENT_V1 = 25
  
	/**
	  Something out of the ordinary happened on the master
	 */
	INCIDENT_EVENT= 26
  
	/**
	  Heartbeat event to be send by master at its idle time
	  to ensure master's online status to slave
	*/
	HEARTBEAT_LOG_EVENT= 27
  
	/**
	  In some situations, it is necessary to send over ignorable
	  data to the slave: data that a slave can handle in case there
	  is code for handling it, but which can be ignored if it is not
	  recognized.
	*/
	IGNORABLE_LOG_EVENT= 28
	ROWS_QUERY_LOG_EVENT= 29
  
	/** Version 2 of the Row events */
	WRITE_ROWS_EVENT = 30
	UPDATE_ROWS_EVENT = 31
	DELETE_ROWS_EVENT = 32
  
	GTID_LOG_EVENT= 33
	ANONYMOUS_GTID_LOG_EVENT= 34
  
	PREVIOUS_GTIDS_LOG_EVENT= 35
  
	TRANSACTION_CONTEXT_EVENT= 36
  
	VIEW_CHANGE_EVENT= 37
  
	/* Prepared XA transaction terminal event similar to Xid */
	XA_PREPARE_LOG_EVENT= 38
	/**
	  Add new events here - right above this comment!
	  Existing events (except ENUM_END_EVENT) should never change their numbers
	*/
	ENUM_END_EVENT /* end marker */
)

func DecodeInt(x []byte) uint32 {
	var Len uint32 = 0
	for i := len(x) - 1 ; i >= 0; i -- {
		Len = (Len << 8) + uint32(x[i])
	}
	return Len
}

type Conf struct {
	Address    string
	Port       string
	User       string
	Password   string
	ServerID   uint64
	BinlogPos  uint64
	BinlogFile string
	DBc        *backend.Conn
	
	Event      []byte
}

var GConf Conf

func loadConf() {
	f,err := os.Open(ConfPath)
	if err != nil {
		log.Panicln("Error : ", err)
	}
	defer f.Close()

	fr := bufio.NewReader(f)
	for {
		k,errk := fr.ReadString('=')
		if errk == io.EOF {
			break 
		}
		v,errv := fr.ReadString('\n')
		if errv == io.EOF {
			break 
		}

		k = k[0:len(k)-1]
		if len(v) != 0 {
			v = v[0:len(v)-1]
		}
		fmt.Printf("%v = %v \n",k,v)

		switch k {
		case "Address" :
			GConf.Address = v
		case "Port" :
			GConf.Port = v
		case "User" :
			GConf.User = v
		case "Password" :
			GConf.Password = v
		case "ServerID" :
			intNum, errAtoi := strconv.Atoi(v)
			if errAtoi == nil {
				GConf.ServerID = uint64(intNum)
			} else {
				fmt.Printf("Worning : ServerId string to uint64 error \n")
			}
		case "BinlogPos" :
			intNum, errAtoi := strconv.Atoi(v)
			if errAtoi == nil {
				GConf.BinlogPos = uint64(intNum)
			} else {
				fmt.Printf("Worning : Pos string to uint64 error \n")
			}
		case "BinlogFile" :
			GConf.BinlogFile = v
		default :
			fmt.Printf("Worning : this config kv dont exit (%s)\n", k)
		}
	}
} 

func InitConfig() {
	loadConf()
}

func showMasterStatus(com string) (string, uint64, error) {
	var res *mysql.Result
	res, err := GConf.DBc.Execute(com)
	if err != nil {
		log.Fatalln("Error : ", err)
	}

	var n = len(res.Resultset.Values)
	if n == 1 {
		fileStr, _ := res.Resultset.GetValue(0, 0)
		posStr, _ := res.Resultset.GetValue(0, 1)
		fileStrV, okf := fileStr.(string)

		if !okf {
			log.Panicln("Error : ", err)
		}

		posStrV, okp := posStr.(uint64)
		if !okp {
			log.Panicln("Error : ", err)
		}
		return fileStrV, posStrV, nil
	}
	return "", 0, fmt.Errorf("invalid resultset")
}

func GetPosFile() {
	binlogFileV, binlogPosV, err := showMasterStatus("show master status")
	if err != nil {
		log.Panicln("Error : ", err)
	}

	GConf.BinlogPos = binlogPosV
	GConf.BinlogFile = binlogFileV
}