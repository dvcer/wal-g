// TEMP REVIEW BEGIN: flavor-specific intent collection and metadata calculation
package mysql

import (
	"fmt"
	"path"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// binlogMetadataHandler interprets the GTIDs field according to the database flavor.
// updateMetadata receives only uploaded entries, in intent order. It performs
// no I/O and must not mutate either input.
type binlogMetadataHandler interface {
	collectIntentInfoForBinlogFile(dir, name, nextName string) (BinlogUploadFile, error)
	updateMetadata(intent BinlogUploadIntent, oldMetadata BinlogSentinelDto) (BinlogSentinelDto, error)
}

func newBinlogMetadataHandler(flavor string) binlogMetadataHandler {
	if flavor == mysql.MariaDBFlavor {
		return mariadbBinlogMetadataHandler{}
	}
	return mysqlBinlogMetadataHandler{}
}

// readBinlogPreviousGTIDs reads the state preceding a file. For a successor
// file, this is also the state at the end of the current file.
func readBinlogPreviousGTIDs(dir, name, flavor string) (mysql.GTIDSet, error) {
	gtids, err := GetBinlogPreviousGTIDs(path.Join(dir, name), flavor)
	if err != nil {
		return nil, err
	}
	if gtids == nil {
		return nil, fmt.Errorf("missing GTID header in %s", name)
	}
	return gtids, nil
}

// TEMP REVIEW END: flavor-specific intent collection and metadata calculation
