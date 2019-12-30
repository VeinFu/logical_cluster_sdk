package logical_cluster

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

type logicalCluster struct {
	ClusterName string
	Hosts []string
}

type hostList []string

func main() {
	var clientset *kubernetes.Clientset

	k8s_config := flag.String("k8s_config", "/Users/vienfu/k8s_env/config", "kubernetes config")
	flag.Parse()

	config , err := clientcmd.BuildConfigFromFlags("", *k8s_config)
	if err != nil {
		log.Println(err)
	}
	clientset, err = kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatalln(err)
	} else {
		fmt.Println("connect k8s success")
	}

	// 获取node列表
	nodes, err := clientset.CoreV1().Nodes().List(metav1.ListOptions{})
	if err != nil {
		log.Println(err.Error())
	}

	clusterName := "mq-cluster"

	for key, value := range nodes.Items[0].Labels {
		fmt.Printf("%s:%s\n", key, value)
	}
	fmt.Println(nodes.Items[0].Spec.PodCIDR)

	var hostsInCluster []string
	hostsInCluster = append(hostsInCluster, nodes.Items[0].Name)

	// 添加逻辑集群
	if err := createCluster(clientset, clusterName, hostsInCluster); err != nil {
		log.Println(err.Error())
	}

	// 获取单个逻辑集群，并转换json格式输出
	var clusterMsg string
	if err := getCluster(clientset, clusterName, &clusterMsg); err != nil {
		log.Println(err.Error())
	}
	fmt.Println(clusterMsg)

	// 获取逻辑集群列表, 并转换json格式输出
	var clustersMsg string
	if err := listClusters(clientset, &clustersMsg); err != nil {
		log.Println(err.Error())
	}
	fmt.Println(clustersMsg)

	// 删除某个逻辑集群
	if err := deleteCluster(clientset, clusterName); err != nil {
		log.Println(err.Error())
	}
	if err := getCluster(clientset, clusterName, &clusterMsg); err != nil {
		log.Println(err.Error())
	}
	fmt.Println(clusterMsg)

	// 更新某个逻辑集群的名称、移除该集群中某些节点
	newClusterName := "redis-cluster"
	if err := updateClusterName(clientset, newClusterName, hostsInCluster, &clusterMsg); err != nil {
		log.Println(err.Error())
	}
	fmt.Println("更改集群名称后：")
	fmt.Println(clusterMsg)

	// 集群的扩缩容
	// 缩容
	var clusterMsgAfterDes string
	if err := scaleCluster(clientset, newClusterName, hostsInCluster, false, &clusterMsgAfterDes); err != nil {
		log.Println(err.Error())
	}
	fmt.Println("缩容后：")
	fmt.Println(clusterMsgAfterDes)
	// 扩容
	var clusterMsgAfterExp string
	if err := scaleCluster(clientset, newClusterName, hostsInCluster, true, &clusterMsgAfterExp); err != nil {
		log.Println(err.Error())
	}
	fmt.Println("扩容后：")
	fmt.Println(clusterMsgAfterExp)
}

// 创建逻辑集群
func createCluster(clientset *kubernetes.Clientset, clusterName string, hostsInCluster []string) error {
	if err := addOrUpdateNodeLabel(clientset, hostsInCluster, clusterName); err != nil {
		return err
	}
	return nil
}

// 获取所有逻辑集群及其主机列表
func listClusters(clientset *kubernetes.Clientset, clustersInfo *string) error {
	var LogicalClusters []logicalCluster
	labelSelectorRequirement := []metav1.LabelSelectorRequirement{
		{Key: "logical-cluster", Operator: metav1.LabelSelectorOpExists}}
	labelSelector := metav1.LabelSelector{MatchExpressions: labelSelectorRequirement}
	labelSelectorMap, _ := metav1.LabelSelectorAsMap(&labelSelector)
	listOptions := metav1.ListOptions{LabelSelector: labels.SelectorFromSet(labelSelectorMap).String()}
	ContainerNodes, err := clientset.CoreV1().Nodes().List(listOptions)
	if err != nil {
		fmt.Println(err.Error())
		return err
	}
	if len(ContainerNodes.Items) == 0 {
		err := errors.New("no logical cluster resource exists")
		return err
	}

	var clusterNames []string
	for _, Host := range ContainerNodes.Items {
		logicalClusterName := Host.Labels["logical-cluster"]
		if ret := isInSlice(logicalClusterName, clusterNames); !ret {
			clusterNames = append(clusterNames, logicalClusterName)
			hostsInCluster, err := getNodesFromCluster(clientset, logicalClusterName)
			if err != nil {
				return err
			}
			LogicalClusters = append(LogicalClusters, logicalCluster{
				ClusterName: logicalClusterName, Hosts: hostsInCluster})
		}
		continue
	}
	logicalClustersBytes, _ := json.Marshal(LogicalClusters)
	*clustersInfo = string(logicalClustersBytes)
	return nil
}

// 获取单个逻辑集群及其主机列表
func getCluster(clientset *kubernetes.Clientset, clusterName string, clusterInfo *string) error {
	hostsInCluster, err := getNodesFromCluster(clientset, clusterName)
	if err != nil {
		return err
	}
	if len(hostsInCluster) == 0 {
		err := errors.New("该逻辑群不存在")
		return err
	}
	singleLogicalClusterBytes, _ := json.Marshal(
		logicalCluster{ClusterName: clusterName, Hosts: hostsInCluster})
	*clusterInfo = string(singleLogicalClusterBytes)
	return nil
}

// 删除某个逻辑集群
func deleteCluster(clientset *kubernetes.Clientset, clusterName string) error {
	hostsLabelRemoved, err := getNodesFromCluster(clientset, clusterName)
	if err != nil {
		return err
	}
	if err := removeLabelFromNode(clientset, hostsLabelRemoved); err != nil {
		return err
	}
	return nil
}

// 更新逻辑集群名称
func updateClusterName(
	clientset *kubernetes.Clientset, clusterName string, hostsInCluster []string, clusterInfo *string) error {
	if err := addOrUpdateNodeLabel(clientset, hostsInCluster, clusterName); err != nil {
		return err
	}
	if err := getCluster(clientset, clusterName, clusterInfo); err != nil {
		return err
	}
	return nil
}

// 逻辑集群的扩缩容
func scaleCluster(clientset *kubernetes.Clientset, clusterName string, nodesList []string, isScale bool, clusterInfo *string) error{
	if isScale {// 扩容
		if err := addOrUpdateNodeLabel(clientset, nodesList, clusterName); err != nil {
			return err
		}
	} else {// 缩容
		 if err := removeLabelFromNode(clientset, nodesList); err != nil {
		 	return err
		 }
	}
	if err := getCluster(clientset, clusterName, clusterInfo); err != nil {
		return err
	}
	return nil
}

// 节点添加、更新标签，也是添加一个逻辑集群的关键入口
func addOrUpdateNodeLabel(clientset *kubernetes.Clientset, hostsUpdated []string, labelName string) error {
	patchData := map[string]interface{}{"metadata": map[string]map[string]string{
		"labels": {"logical-cluster": labelName}}}
	patchDataBytes, _ := json.Marshal(patchData)
	for _, targetNode := range hostsUpdated {
		_, err := clientset.CoreV1().Nodes().Patch(targetNode, types.StrategicMergePatchType, patchDataBytes)
		if err != nil {
			return err
		}
	}
	return nil
}

// 移除某个特定节点标签
func removeLabelFromNode(clientset *kubernetes.Clientset, targetNodes []string) error {
	patchDataMap := make(map[string]string)
	patchDataMap["op"] = "remove"
	patchDataMap["path"] = "/metadata/labels/logical-cluster"
	var patchData []map[string]string
	patchData = append(patchData, patchDataMap)
	patchDataBytes, _ := json.Marshal(patchData)
	for _, targetNode := range targetNodes {
		if _, err := clientset.CoreV1().Nodes().Patch(targetNode, types.JSONPatchType, patchDataBytes); err != nil {
			return err
		}
	}
	return nil
}

// 查找具有特定标签的所有主机
func getNodesFromCluster(clientset *kubernetes.Clientset, clusterName string) (hostList, error) {
	var hosts []string
	labelMap := make(map[string]string)
	labelMap["logical-cluster"] = clusterName
	labelSelector := metav1.LabelSelector{MatchLabels: labelMap}
	labelSelectorMap, _ := metav1.LabelSelectorAsMap(&labelSelector)
	listOptions := metav1.ListOptions{LabelSelector: labels.SelectorFromSet(labelSelectorMap).String()}
	ContainerNodes, err := clientset.CoreV1().Nodes().List(listOptions)
	if err != nil {
		log.Println(err.Error())
		return hosts, err
	}

	for _, node := range ContainerNodes.Items {
		hosts = append(hosts, node.Name)
	}

	return hosts, nil
}

// 判断切片里某个值是否存在
func isInSlice(data string, slice []string) bool {
	for _, val := range slice {
		if val == data {
			return true
		}
		continue
	}
	return false
}
