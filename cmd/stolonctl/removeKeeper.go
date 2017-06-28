package main

import (
	"github.com/sorintlab/stolon/pkg/cluster"
	"github.com/sorintlab/stolon/pkg/store"
	"github.com/spf13/cobra"
)

var removeKeeperCmd = &cobra.Command{
	Use:   "remove keeper",
	Short: "Removes keeper from cluster config",
	Run:   removeKeeper,
}

func init() {
	cmdStolonCtl.AddCommand(removeKeeperCmd)
}

func removeKeeper(cmd *cobra.Command, args []string) {
	if len(args) > 1 {
		die("too many arguments")
	}

	if len(args) == 0 {
		die("keeper id required")
	}

	keeperID := args[0]
	store, err := NewStore()
	if err != nil {
		die("cannot create store: %v", err)
	}
	removeKeeperData(store, keeperID)
}

func removeKeeperData(store *store.StoreManager, keeperID string) {
	cd, prevKvClusterData, err := store.GetClusterData()
	if err != nil {
		die("cannot get cluster data: %v", err)
	}
	newCd := cd.DeepCopy()
	keeperInfo := newCd.Keepers[keeperID]
	if keeperInfo == nil {
		die("No keeper info for provided keeper id")
	}

	keeperDb := getDbForKeeper(newCd.DBs, keeperID)

	if keeperDb == nil {
		die("Can not find db for specified keeper id")
	}

	delete(newCd.DBs, keeperDb.UID)
	delete(newCd.Keepers, keeperID)

	_, err = store.AtomicPutClusterData(newCd, prevKvClusterData)
	if err != nil {
		die("cannot update cluster data: %v", err)
	}
}

func getDbForKeeper(dbs cluster.DBs, keeperID string) *cluster.DB {
	for _, db := range dbs {
		if db.Spec.KeeperUID == keeperID {
			return db
		}
	}

	return nil
}
