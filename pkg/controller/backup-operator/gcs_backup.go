package controller

import (
	"fmt"
	"path"

	api "github.com/coreos/etcd-operator/pkg/apis/etcd/v1beta2"
	"github.com/coreos/etcd-operator/pkg/backup"
	"github.com/coreos/etcd-operator/pkg/backup/writer"

	"k8s.io/client-go/kubernetes"
)

func handleGCS(kubecli kubernetes.Interface, src *api.GCSSource, namespace, clusterName string) (string, error) {
	bm := backup.NewBackupManagerFromWriter(kubecli, writer.NewGCSWriter(src.BucketName), clusterName, namespace)
	fullPath, err := bm.SaveSnapWithPrefix(path.Join(namespace, clusterName))
	if err != nil {
		return "", fmt.Errorf("failed to save snapshot: %v", err)
	}
	return fmt.Sprintf("gs://%s/%s", src.BucketName, fullPath), nil
}
