// TEMP REVIEW BEGIN: MySQL intent collection and metadata calculation
package mysql

import (
	"fmt"

	"github.com/go-mysql-org/go-mysql/mysql"
)

type mysqlBinlogMetadataHandler struct{}

func (mysqlBinlogMetadataHandler) collectIntentInfoForBinlogFile(dir, name, nextName string) (BinlogUploadFile, error) {
	file := BinlogUploadFile{Name: name}
	end, err := readBinlogPreviousGTIDs(dir, nextName, mysql.MySQLFlavor)
	if err != nil {
		return file, err
	}
	start, err := readBinlogPreviousGTIDs(dir, name, mysql.MySQLFlavor)
	if err != nil {
		return file, err
	}
	before, ok := start.(*mysql.MysqlGTIDSet)
	if !ok || before == nil {
		return file, fmt.Errorf("missing MySQL GTID header in %s", name)
	}
	after, ok := end.(*mysql.MysqlGTIDSet)
	if !ok || !after.Contain(before) {
		return file, fmt.Errorf("inconsistent MySQL GTID headers for %s and %s", name, nextName)
	}
	contribution := after.Clone().(*mysql.MysqlGTIDSet)
	gtidSetMinus(contribution, before)
	file.GTIDs = contribution.String()
	return file, nil
}

func (mysqlBinlogMetadataHandler) updateMetadata(intent BinlogUploadIntent, oldMetadata BinlogSentinelDto) (BinlogSentinelDto, error) {
	result := oldMetadata
	archived, err := mysql.ParseMysqlGTIDSet(oldMetadata.GTIDArchived)
	if err != nil {
		return oldMetadata, fmt.Errorf("invalid archived GTID checkpoint: %w", err)
	}
	for _, file := range intent.Files {
		if err := archived.Update(file.GTIDs); err != nil {
			return oldMetadata, err
		}
		result.GTIDArchived = archived.String()
	}
	return result, nil
}

// TEMP REVIEW END: MySQL intent collection and metadata calculation
