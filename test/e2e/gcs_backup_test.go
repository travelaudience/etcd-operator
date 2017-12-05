package e2e

import (
	"fmt"
	"os"
	"testing"
	"time"

	api "github.com/coreos/etcd-operator/pkg/apis/etcd/v1beta2"
	"github.com/coreos/etcd-operator/pkg/util/retryutil"
	"github.com/coreos/etcd-operator/test/e2e/e2eutil"
	"github.com/coreos/etcd-operator/test/e2e/framework"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"cloud.google.com/go/storage"
	"context"
	"github.com/coreos/etcd-operator/pkg/backup/reader"
)

func TestGCSBackupAndRestore(t *testing.T) {
	if os.Getenv(envParallelTest) == envParallelTestTrue {
		t.Parallel()
	}
	if err := verifyGCSEnvVars(); err != nil {
		t.Fatal(err)
	}
	path := testEtcdBackupOperatorForGCSBackup(t)
	if len(path) == 0 {
		t.Fatal("skipping restore test: GCS path not set despite testEtcdBackupOperatorForGCSBackup success")
	}
	testEtcdRestoreOperatorForGCSSource(t, path)
}

func verifyGCSEnvVars() error {
	if len(os.Getenv("TEST_GCS_BUCKET")) == 0 {
		return fmt.Errorf("TEST_GCS_BUCKET not set")
	}
	return nil
}

func testEtcdBackupOperatorForGCSBackup(t *testing.T) string {
	f := framework.Global
	testEtcd, err := e2eutil.CreateCluster(t, f.CRClient, f.Namespace, e2eutil.NewCluster("test-etcd-", 3))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := e2eutil.DeleteCluster(t, f.CRClient, f.KubeClient, testEtcd); err != nil {
			t.Fatal(err)
		}
	}()
	if _, err := e2eutil.WaitUntilSizeReached(t, f.CRClient, 3, 6, testEtcd); err != nil {
		t.Fatalf("failed to create 3 members etcd cluster: %v", err)
	}
	eb, err := f.CRClient.EtcdV1beta2().EtcdBackups(f.Namespace).Create(e2eutil.NewGCSBackup(testEtcd.Name, os.Getenv("TEST_GCS_BUCKET")))
	if err != nil {
		t.Fatalf("failed to create etcd backup cr: %v", err)
	}
	defer func() {
		if err := f.CRClient.EtcdV1beta2().EtcdBackups(f.Namespace).Delete(eb.Name, nil); err != nil {
			t.Fatalf("failed to delete etcd backup cr: %v", err)
		}
	}()

	path := ""
	client, err := storage.NewClient(context.Background())
	if err != nil {
		t.Fatalf("failed create GCS client: %v", err)
	}
	defer client.Close()
	err = retryutil.Retry(time.Second, 4, func() (bool, error) {
		reb, err := f.CRClient.EtcdV1beta2().EtcdBackups(f.Namespace).Get(eb.Name, metav1.GetOptions{})
		if err != nil {
			return false, fmt.Errorf("failed to retrieve backup CR: %v", err)
		}
		if reb.Status.Succeeded {
			bucketAndPath, err := reader.SplitGCSPath(reb.Status.Path)
			if err != nil {
				return false, err
			}
			if _, err := client.Bucket(bucketAndPath[1]).Object(bucketAndPath[2]).NewReader(context.TODO()); err != nil {
				return false, fmt.Errorf("failed to get backup %v from GCS : %v", reb.Status.Path, err)
			}
			path = reb.Status.Path
			return true, nil
		} else if len(reb.Status.Reason) != 0 {
			return false, fmt.Errorf("backup failed with reason: %v ", reb.Status.Reason)
		}
		return false, nil
	})
	if err != nil {
		t.Fatalf("failed to verify backup: %v", err)
	}
	return path
}

func testEtcdRestoreOperatorForGCSSource(t *testing.T, path string) {
	f := framework.Global

	restoreSource := api.RestoreSource{GCS: e2eutil.NewGCSRestoreSource(path)}
	er := e2eutil.NewEtcdRestore("test-etcd-restore-", "3.1.10", 3, restoreSource)
	er, err := f.CRClient.EtcdV1beta2().EtcdRestores(f.Namespace).Create(er)
	if err != nil {
		t.Fatalf("failed to create etcd restore cr: %v", err)
	}
	defer func() {
		if err := f.CRClient.EtcdV1beta2().EtcdRestores(f.Namespace).Delete(er.Name, nil); err != nil {
			t.Fatalf("failed to delete etcd restore cr: %v", err)
		}
	}()

	// Verify the EtcdRestore CR status "succeeded=true". In practice the time taken to update is 1 second.
	err = retryutil.Retry(time.Second, 5, func() (bool, error) {
		er, err := f.CRClient.EtcdV1beta2().EtcdRestores(f.Namespace).Get(er.Name, metav1.GetOptions{})
		if err != nil {
			return false, fmt.Errorf("failed to retrieve restore CR: %v", err)
		}
		if er.Status.Succeeded {
			return true, nil
		} else if len(er.Status.Reason) != 0 {
			return false, fmt.Errorf("restore failed with reason: %v ", er.Status.Reason)
		}
		return false, nil
	})
	if err != nil {
		t.Fatalf("failed to verify restore succeeded: %v", err)
	}

	// Verify that the restored etcd cluster scales to 3 ready members
	restoredCluster := &api.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      er.Name,
			Namespace: f.Namespace,
		},
		Spec: api.ClusterSpec{
			Size: 3,
		},
	}
	if _, err := e2eutil.WaitUntilSizeReached(t, f.CRClient, 3, 6, restoredCluster); err != nil {
		t.Fatalf("failed to see restored etcd cluster(%v) reach 3 members: %v", restoredCluster.Name, err)
	}
	if err := e2eutil.DeleteCluster(t, f.CRClient, f.KubeClient, restoredCluster); err != nil {
		t.Fatalf("failed to delete restored cluster(%v): %v", restoredCluster.Name, err)
	}
}
