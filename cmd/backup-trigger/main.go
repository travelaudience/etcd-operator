package main

import (
	"os"

	api "github.com/coreos/etcd-operator/pkg/apis/etcd/v1beta2"
	clientset "github.com/coreos/etcd-operator/pkg/generated/clientset/versioned"

	log "github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	EnvEtcdClusterName       = "ETCD_CLUSTER_NAME"
	EnvEtcdClusterNamespace  = "ETCD_CLUSTER_NAMESPACE"
	EnvEtcdOperatorTLSSecret = "ETCD_OPERATOR_TLS_SECRET"
	EnvGCSBucketName         = "GCS_BUCKET_NAME"
)

var (
	kubeconfig string
)

func main() {
	bucketName := os.Getenv(EnvGCSBucketName)
	if len(bucketName) == 0 {
		log.Fatalf("%s is not defined", EnvEtcdClusterName)
	}
	name := os.Getenv(EnvEtcdClusterName)
	if len(name) == 0 {
		log.Fatalf("%s is not defined", EnvEtcdClusterName)
	}
	namespace := os.Getenv(EnvEtcdClusterNamespace)
	if len(namespace) == 0 {
		log.Fatalf("%s is not defined", EnvEtcdClusterName)
	}
	secret := os.Getenv(EnvEtcdOperatorTLSSecret)

	loader := clientcmd.NewDefaultClientConfigLoadingRules()
	cfg, err := clientcmd.BuildConfigFromKubeconfigGetter("", loader.Load)
	if err != nil {
		log.Fatalf("failed to create configuration: %v", err)
	}

	client, err := clientset.NewForConfig(cfg)
	if err != nil {
		log.Fatalf("failed to create client: %v", err)
	}

	etcdBackup, err := client.EtcdV1beta2().EtcdBackups(namespace).Create(createEtcdBackupObj(name, secret, bucketName))
	if err != nil {
		log.Fatalf("failed to create etcdbackup resource: %v", err)
	}

	log.Infof("created etcdbackup resource: %s", etcdBackup.Name)
	log.Warnf("this does not mean the actual backup was successful")
	log.Warnf("check 'kubectl --namespace %s describe etcdbackup %s' for details", namespace, etcdBackup.Name)
}

func createEtcdBackupObj(etcdClusterName, secret, bucketName string) *api.EtcdBackup {
	return &api.EtcdBackup{
		TypeMeta: metav1.TypeMeta{
			Kind:       api.EtcdBackupResourceKind,
			APIVersion: api.SchemeGroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: etcdClusterName + "-",
		},
		Spec: api.BackupSpec{
			ClusterName: etcdClusterName,
			OperatorSecret: secret,
			StorageType: api.BackupStorageTypeGCS,
			BackupStorageSource: api.BackupStorageSource{
				GCS: &api.GCSSource{
					BucketName: bucketName,
				},
			},
		},
	}
}
