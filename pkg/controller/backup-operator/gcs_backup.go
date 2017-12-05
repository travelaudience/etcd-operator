package controller

import (
	"fmt"
	"path"

	api "github.com/coreos/etcd-operator/pkg/apis/etcd/v1beta2"
	"github.com/coreos/etcd-operator/pkg/backup"
	"github.com/coreos/etcd-operator/pkg/backup/writer"

	"k8s.io/client-go/kubernetes"
	"crypto/tls"
	"github.com/coreos/etcd-operator/pkg/util/k8sutil"
	"github.com/coreos/etcd-operator/pkg/util/etcdutil"
)

func handleGCS(kubecli kubernetes.Interface, namespace string, spec *api.BackupSpec) (string, error) {
	var (
		tlsConfig *tls.Config
	)
	if len(spec.OperatorSecret) != 0 {
		d, err := k8sutil.GetTLSDataFromSecret(kubecli, namespace, spec.OperatorSecret)
		if err != nil {
			return "", fmt.Errorf("failed to get TLS data from secret %v: %v", spec.OperatorSecret, err)
		}
		tlsConfig, err = etcdutil.NewTLSConfig(d.CertData, d.KeyData, d.CAData, true)
		if err != nil {
			return "", fmt.Errorf("failed to build tls config: %v", err)
		}
	}
	bm := backup.NewBackupManagerFromWriter(kubecli, writer.NewGCSWriter(spec.GCS.BucketName), tlsConfig, spec.ClusterName, namespace)
	fullPath, err := bm.SaveSnapWithPrefix(path.Join(namespace, spec.ClusterName))
	if err != nil {
		return "", fmt.Errorf("failed to save snapshot: %v", err)
	}
	return fmt.Sprintf("gs://%s/%s", spec.GCS.BucketName, fullPath), nil
}
