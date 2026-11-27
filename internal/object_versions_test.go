package internal_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/test/mocks"
	"go.uber.org/mock/gomock"
)

func TestPruneOldObjectVersions(t *testing.T) {
	const name = "metadata.json"
	object := func(name, id, info string) storage.Object {
		return storage.NewLocalObjectWithVersion(name, time.Time{}, 0, id, info)
	}
	latest := object(name, "current", "LATEST")
	latestMarker := object(name, "current-marker", "LATEST DELETE")
	old := object(name, "old", "")
	oldMarker := object(name, "old-marker", "DELETE")
	unrelated := object("other.json", "unrelated", "")
	testcases := []struct {
		name              string
		versioningEnabled bool
		limited           bool
		objects           []storage.Object
		expectedDeleted   []storage.Object
	}{
		{
			name:              "keep latest object version",
			versioningEnabled: true,
			objects:           []storage.Object{latest, old, oldMarker, unrelated, object(name+".other", "similar-name", "")},
			expectedDeleted:   []storage.Object{old, oldMarker},
		},
		{
			name:              "keep latest delete marker",
			versioningEnabled: true,
			objects:           []storage.Object{old, oldMarker, latestMarker},
			expectedDeleted:   []storage.Object{old, oldMarker},
		},
		{
			name:              "only latest object version exists",
			versioningEnabled: true,
			objects:           []storage.Object{latest},
			expectedDeleted:   []storage.Object{},
		},
		{
			name:              "only latest delete marker exists",
			versioningEnabled: true,
			objects:           []storage.Object{latestMarker},
			expectedDeleted:   []storage.Object{},
		},
		{
			name:              "object not found",
			versioningEnabled: true,
			objects:           []storage.Object{unrelated},
			expectedDeleted:   []storage.Object{},
		},
		{
			name:              "skip objects without version IDs",
			versioningEnabled: true,
			objects:           []storage.Object{old, object(name, "", ""), latestMarker},
			expectedDeleted:   []storage.Object{old},
		},
		{
			name:              "empty folder",
			versioningEnabled: true,
			expectedDeleted:   []storage.Object{},
		},
		{
			name:              "versioning disabled",
			versioningEnabled: false,
			expectedDeleted:   []storage.Object{},
		},
		{
			name:              "versioning disabled with rate limiter",
			versioningEnabled: false,
			limited:           true,
			expectedDeleted:   []storage.Object{},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			folder := mocks.NewMockFolder(ctrl)
			folder.EXPECT().GetVersioningEnabled(t.Context()).Return(tc.versioningEnabled)
			if tc.versioningEnabled {
				child := mocks.NewMockFolder(ctrl)
				folder.EXPECT().ListFolder(t.Context()).Return(tc.objects, []storage.Folder{child}, nil)
			}
			if len(tc.expectedDeleted) > 0 {
				folder.EXPECT().DeleteObjects(t.Context(), tc.expectedDeleted).Return(nil)
			}

			var target storage.Folder = folder
			if tc.limited {
				target = internal.NewLimitedFolder(folder, nil)
			}
			require.NoError(t, internal.PruneOldObjectVersions(t.Context(), target, name))
		})
	}
}
