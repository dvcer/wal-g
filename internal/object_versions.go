package internal

import (
	"context"
	"strings"

	"github.com/wal-g/wal-g/pkg/storages/storage"
)

// PruneOldObjectVersions deletes non-latest versions of a direct child object.
// Latest data versions and delete markers are preserved. All metadata versions at
// this folder level are listed in memory; versioning-disabled folders are skipped.
// Callers must serialize cleanup for the same object.
func PruneOldObjectVersions(ctx context.Context, folder storage.Folder, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !folder.GetVersioningEnabled(ctx) {
		return nil
	}
	objects, _, err := storage.WithAllVersions(folder).ListFolder(ctx)
	if err != nil {
		return err
	}
	oldObjects := objects[:0]
	for _, object := range objects {
		if object.GetName() != name {
			continue
		}
		if object.GetVersionID() != "" && !strings.Contains(object.GetAdditionalInfo(), "LATEST") {
			oldObjects = append(oldObjects, object)
		}
	}
	if len(oldObjects) == 0 {
		return nil
	}
	return folder.DeleteObjects(ctx, oldObjects)
}
