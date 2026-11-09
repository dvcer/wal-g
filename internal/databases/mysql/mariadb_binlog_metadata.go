// TEMP REVIEW BEGIN: MariaDB intent collection and metadata calculation
package mysql

import (
	"fmt"

	"github.com/go-mysql-org/go-mysql/mysql"
)

type mariadbBinlogMetadataHandler struct{}

func (mariadbBinlogMetadataHandler) collectIntentInfoForBinlogFile(dir, name, nextName string) (BinlogUploadFile, error) {
	file := BinlogUploadFile{Name: name}
	position, err := readBinlogPreviousGTIDs(dir, nextName, mysql.MariaDBFlavor)
	if err != nil {
		return file, err
	}
	file.GTIDs = position.String()
	return file, nil
}

func (mariadbBinlogMetadataHandler) updateMetadata(intent BinlogUploadIntent, oldMetadata BinlogSentinelDto) (BinlogSentinelDto, error) {
	result := oldMetadata
	archived, err := mysql.ParseMariadbGTIDSet(oldMetadata.GTIDArchived)
	if err != nil {
		return oldMetadata, fmt.Errorf("invalid archived GTID checkpoint: %w", err)
	}
	for _, file := range intent.Files {
		position, err := mysql.ParseMariadbGTIDSet(file.GTIDs)
		if err != nil {
			return oldMetadata, err
		}
		// Match the MariaDB filter: leave an already covered position unchanged.
		if !archived.Contain(position) {
			if err := archived.Update(position.String()); err != nil {
				return oldMetadata, err
			}
		}
		result.GTIDArchived = archived.String()
	}
	return result, nil
}

// TEMP REVIEW END: MariaDB intent collection and metadata calculation
