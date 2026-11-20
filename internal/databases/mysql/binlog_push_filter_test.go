package mysql

import (
	"slices"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/require"
)

func TestFilterBinlogsForUpload(t *testing.T) {
	const serverUUID = "da187401-e4f3-11f1-9478-92bd74c99a74"
	binlogs := []string{
		"mysql-bin.000001",
		"mysql-bin.000002",
		"mysql-bin.000003",
		"mysql-bin.000004",
		"mysql-bin.000005",
		"mysql-bin.000006",
	}
	cases := []struct {
		name         string
		inputBinlogs []string
		cached       string
		until        string
		gtidArchived string
		want         []string
	}{
		{
			name:         "no files",
			inputBinlogs: []string{},
			cached:       "",
			until:        "",
			gtidArchived: "",
			want:         []string{},
		},
		{
			name:         "only active file",
			inputBinlogs: binlogs[5:],
			cached:       "",
			until:        "",
			gtidArchived: "",
			want:         []string{},
		},
		{
			name:         "no filters",
			inputBinlogs: binlogs,
			cached:       "",
			until:        "",
			gtidArchived: "",
			want:         binlogs,
		},
		{
			name:         "skip cached prefix",
			inputBinlogs: binlogs,
			cached:       binlogs[1],
			until:        "",
			gtidArchived: "",
			want:         binlogs[2:],
		},
		{
			name:         "cache before first file",
			inputBinlogs: binlogs,
			cached:       "mysql-bin.000000",
			until:        "",
			gtidArchived: "",
			want:         binlogs,
		},
		{
			name:         "all completed files cached",
			inputBinlogs: binlogs,
			cached:       binlogs[4],
			until:        "",
			gtidArchived: "",
			want:         []string{},
		},
		{
			name:         "apply until filter",
			inputBinlogs: binlogs,
			cached:       "",
			until:        binlogs[2],
			gtidArchived: "",
			want:         binlogs[:3],
		},
		{
			name:         "until at first file",
			inputBinlogs: binlogs,
			cached:       "",
			until:        binlogs[0],
			gtidArchived: "",
			want:         []string{},
		},
		{
			name:         "until before first file",
			inputBinlogs: binlogs,
			cached:       "",
			until:        "mysql-bin.000000",
			gtidArchived: "",
			want:         []string{},
		},
		{
			name:         "until beyond active file",
			inputBinlogs: binlogs,
			cached:       "",
			until:        "mysql-bin.000100",
			gtidArchived: "",
			want:         binlogs,
		},
		{
			name:         "until before cache",
			inputBinlogs: binlogs,
			cached:       binlogs[1],
			until:        binlogs[0],
			gtidArchived: "",
			want:         []string{},
		},
		{
			name:         "skip GTID covered prefix only",
			inputBinlogs: binlogs,
			cached:       "",
			until:        "",
			gtidArchived: serverUUID + ":1-2",
			want:         binlogs[2:],
		},
		{
			name:         "multiple GTID upload candidates",
			inputBinlogs: binlogs,
			cached:       "",
			until:        "",
			gtidArchived: serverUUID + ":1",
			want:         binlogs[1:],
		},
		{
			name:         "all GTIDs covered",
			inputBinlogs: binlogs,
			cached:       "",
			until:        "",
			gtidArchived: serverUUID + ":1-5",
			want:         []string{},
		},
		{
			name:         "cache and until bound GTID checks",
			inputBinlogs: binlogs,
			cached:       binlogs[0],
			until:        binlogs[3],
			gtidArchived: serverUUID + ":1-2",
			want:         binlogs[2:4],
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			archived, err := mysql.ParseMysqlGTIDSet(tc.gtidArchived)
			require.NoError(t, err)
			filter := &mysqlGtidFilter{
				binlogsFolder: "testdata/binlogs",
				flavor:        mysql.MySQLFlavor,
				gtidArchived:  archived.(*mysql.MysqlGTIDSet),
			}
			original := slices.Clone(tc.inputBinlogs)
			got := filterBinlogsForUpload(tc.inputBinlogs, tc.cached, tc.until, filter)
			require.Equal(t, tc.want, got)
			require.Equal(t, original, tc.inputBinlogs, "filtering must not modify the binlog list")
		})
	}
}
