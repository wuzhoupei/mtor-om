package linkDB

import (
	"fmt"
	// "log"

	G "mtor-om/global"
	H "mtor-om/eventH"

	"github.com/flike/kingshard/mysql"
    // "github.com/flike/kingshard/backend"
)

func readPacket() ([]byte,error) {
	backp, err := G.GConf.DBc.Pkg.ReadPacket()
	G.GConf.DBc.PkgErr = err
	return backp, err
}

func writePacket(data []byte) error {
	err := G.GConf.DBc.Pkg.WritePacket(data)
	G.GConf.DBc.PkgErr = err
	return err
}

func writeCommandBuf(command byte, arg []byte) error {
	// return nil
	G.GConf.DBc.Pkg.Sequence = 0
	length := len(arg) + 1
	data := make([]byte, length+4)
	data[4] = command
	copy(data[5:], arg)

	return writePacket(data)
}

func binlogDump(data []byte) {
	err := writeCommandBuf(data[0], data[1:])
	if err != nil {
		fmt.Printf("Error : %v\n",err)
	}
}

func makePacket() {
	binlogFile := []byte(G.GConf.BinlogFile)
	fileLen := len(binlogFile)
	binlogFlag := 0x0000
	// COM_BINLOG_DUMP + pos + flag + serverId + binlogFile
	data := make([]byte, 1+4+2+4+fileLen)

	data[0] = mysql.COM_BINLOG_DUMP

	// The small end
	data[1] = byte((G.GConf.BinlogPos & 0xFFFFFFFF) >> 0)
	data[2] = byte((G.GConf.BinlogPos & 0xFFFFFFFF) >> 8)
	data[3] = byte((G.GConf.BinlogPos & 0xFFFFFFFF) >> 16)
	data[4] = byte((G.GConf.BinlogPos & 0xFFFFFFFF) >> 24)

	data[5] = byte((binlogFlag & 0xFFFF) >> 0)
	data[6] = byte((binlogFlag & 0xFFFF) >> 8)
	
	data[7] = byte((G.GConf.ServerID & 0xFFFFFFFF) >> 0)
	data[8] = byte((G.GConf.ServerID & 0xFFFFFFFF) >> 8)
	data[9] = byte((G.GConf.ServerID & 0xFFFFFFFF) >> 16)
	data[10] = byte((G.GConf.ServerID & 0xFFFFFFFF) >> 24)

	copy(data[1+4+2+4:], binlogFile)

	binlogDump(data)
}

func Dump() {
	makePacket()

	for {
		backP, err := readPacket()
		if err != nil {
			fmt.Printf("Error : %v\n",err)
			return 
		}

		if backP[0] == mysql.EOF_HEADER && len(backP) < 9 {
			return 
		}

		if backP[0] == mysql.OK_HEADER {
			// Loss a Chack ! 
			G.GConf.Event = backP
			H.SplitHeader()
		}
	}
}