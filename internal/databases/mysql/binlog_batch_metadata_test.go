package mysql

import (
	"path/filepath"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/require"
)

func TestApplyBatchMetadata(t *testing.T) {
	var anotherServerUuid = "00000000-0000-0000-0000-0000000000bb"
	cases := []struct {
		name                 string
		gtidArchivedInitial  string
		uploadedBinlogFiles  []string
		successorFile        string
		expectedGtidArchived string
	}{
		{
			name:                 "initialize checkpoint from first batch",
			gtidArchivedInitial:  "",
			uploadedBinlogFiles:  []string{"mysql-bin.000001", "mysql-bin.000002"},
			successorFile:        "mysql-bin.000003",
			expectedGtidArchived: testDataBinlogServerUuid + ":1-2",
		},
		{
			name:                 "initialize checkpoint from a later batch",
			gtidArchivedInitial:  "",
			uploadedBinlogFiles:  []string{"mysql-bin.000003", "mysql-bin.000004"},
			successorFile:        "mysql-bin.000005",
			expectedGtidArchived: testDataBinlogServerUuid + ":1-4",
		},
		{
			name:                 "advance checkpoint across uploaded batch",
			gtidArchivedInitial:  testDataBinlogServerUuid + ":1",
			uploadedBinlogFiles:  []string{"mysql-bin.000002", "mysql-bin.000003"},
			successorFile:        "mysql-bin.000004",
			expectedGtidArchived: testDataBinlogServerUuid + ":1-3",
		},
		{
			name:                 "preserve gaps",
			gtidArchivedInitial:  testDataBinlogServerUuid + ":1",
			uploadedBinlogFiles:  []string{"mysql-bin.000004"},
			successorFile:        "mysql-bin.000005",
			expectedGtidArchived: testDataBinlogServerUuid + ":1:4",
		},
		{
			name:                 "preserve transactions from another UUID",
			gtidArchivedInitial:  anotherServerUuid + ":1-5",
			uploadedBinlogFiles:  []string{"mysql-bin.000001", "mysql-bin.000002"},
			successorFile:        "mysql-bin.000003",
			expectedGtidArchived: anotherServerUuid + ":1-5," + testDataBinlogServerUuid + ":1-2",
		},
		{
			name:                 "empty batch leaves checkpoint unchanged",
			gtidArchivedInitial:  testDataBinlogServerUuid + ":1",
			uploadedBinlogFiles:  []string{},
			successorFile:        "mysql-bin.000002",
			expectedGtidArchived: testDataBinlogServerUuid + ":1",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			filenames := make([]string, 0, len(tc.uploadedBinlogFiles))
			for _, filename := range tc.uploadedBinlogFiles {
				filenames = append(filenames, filepath.Join("testdata/binlogs", filename))
			}
			files := openPushTestFiles(t, filenames...)
			metadata, err := extractBatchMetadata(files, filepath.Join("testdata/binlogs", tc.successorFile), mysql.MySQLFlavor)
			require.NoError(t, err)

			sentinel := BinlogSentinelDto{GTIDArchived: tc.gtidArchivedInitial}
			require.NoError(t, sentinel.ApplyBatchMetadata(metadata, mysql.MySQLFlavor))
			require.Equal(t, tc.expectedGtidArchived, sentinel.GTIDArchived)
		})
	}
}
