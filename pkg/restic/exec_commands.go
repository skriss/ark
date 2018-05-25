package restic

import (
	"encoding/json"

	"github.com/pkg/errors"
)

func GetSnapshotID(repoPrefix, repo, passwordFile string, tags map[string]string) (string, error) {
	cmd := GetSnapshotCommand(repoPrefix, repo, passwordFile, tags)

	res, err := cmd.Cmd().Output()
	if err != nil {
		return "", errors.WithStack(err)
	}

	type snapshotID struct {
		ShortID string `json:"short_id"`
	}

	var snapshots []snapshotID
	if err := json.Unmarshal(res, &snapshots); err != nil {
		return "", errors.Wrap(err, "error unmarshalling restic snapshots result")
	}

	if len(snapshots) != 1 {
		return "", errors.Errorf("expected one matching snapshot, got %d", len(snapshots))
	}

	return snapshots[0].ShortID, nil
}
