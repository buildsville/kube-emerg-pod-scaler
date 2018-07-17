package main

import (
	"flag"
	"fmt"
	"github.com/mitchellh/go-homedir"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"syscall"
	"time"

	"github.com/golang/glog"
	"k8s.io/api/core/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typed_corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/tools/reference"
	"k8s.io/client-go/util/workqueue"
)

/*
使用する環境変数
MULTIPLY_SPEC : スケール倍率
UPDATE_BORDER_SEC : スケールしてから次スケールできるようになるまでの秒数
UNHEALTHY_BORDER_SEC : Unhealthyなpodを検出する秒数
UNHEALTHY_RATE_FOR_SCALE : unhealthyなpodがこの数値パーセント以下の時にはscaleしない
UNHEALTHY_ONLY_LIVENESS : TRUEでlivenessがfailしたもののみをunhealthyとする
POD_NAME : このpodの名前
POD_NAMESPACE : このpodのnamespace
*/

const (
	maxRetries                 = 3
	defaultLastUpdateBorderSec = 300
	defaultMultiplySpec        = 3
	defaultUnhealthyBorderSec  = 180
	defaultUnhealthyRateForScale = 50
	defaultPodName             = "kube-emerg-pod-scaler"
	defaultPodNamespace        = "default"
)

var multiplySpec = func() int32 {
	i, err := strconv.ParseInt(os.Getenv("MULTIPLY_SPEC"), 10, 32)
	if err == nil {
		return int32(i)
	} else {
		return defaultMultiplySpec
	}
}()

var lastUpdateBorderSec = func() int64 {
	i, err := strconv.ParseInt(os.Getenv("UPDATE_BORDER_SEC"), 10, 64)
	if err == nil {
		return i
	} else {
		return defaultLastUpdateBorderSec
	}
}()

var unhealthyBorderSec = func() int64 {
	i, err := strconv.ParseInt(os.Getenv("UNHEALTHY_BORDER_SEC"), 10, 64)
	if err == nil {
		return i
	} else {
		return defaultUnhealthyBorderSec
	}
}()

var unhealthyRateForScale = func() int32 {
	i, err := strconv.ParseInt(os.Getenv("UNHEALTHY_RATE_FOR_SCALE"), 10, 32)
	if err == nil {
		return int32(i)
	} else {
		return defaultUnhealthyRateForScale
	}
}()

var podName = func() string {
	if os.Getenv("POD_NAME") == "" {
		return defaultPodName
	} else {
		return os.Getenv("POD_NAME")
	}
}()

var podNamespace = func() string {
	if os.Getenv("POD_NAMESPACE") == "" {
		return defaultPodNamespace
	} else {
		return os.Getenv("POD_NAMESPACE")
	}
}()

var eventRecorder = func() record.EventRecorder {
	cli := client.(*kubernetes.Clientset)
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartRecordingToSink(&typed_corev1.EventSinkImpl{Interface: cli.CoreV1().Events("")})
	return eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: podName})
}()

var serverStartTime time.Time

var client = kubeClient()

type Controller struct {
	indexer  cache.Indexer
	queue    workqueue.RateLimitingInterface
	informer cache.Controller
}

type Event struct {
	key       string
	eventType string
	send      bool
}

type HpaInfo struct {
	refKind         string
	refName         string
	namespace       string
	currentReplicas int32
}

func kubeClient() kubernetes.Interface {
	var ret kubernetes.Interface
	config, err := rest.InClusterConfig()
	if err != nil {
		var kubeconfigPath string
		if os.Getenv("KUBECONFIG") == "" {
			home, err := homedir.Dir()
			if err != nil {
				panic(err)
			}
			kubeconfigPath = home + "/.kube/config"
		} else {
			kubeconfigPath = os.Getenv("KUBECONFIG")
		}
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfigPath)
		if err != nil {
			panic(err)
		}
	}
	ret, err = kubernetes.NewForConfig(config)
	if err != nil {
		panic(err)
	}
	return ret
}

func NewController(queue workqueue.RateLimitingInterface, indexer cache.Indexer, informer cache.Controller) *Controller {
	return &Controller{
		informer: informer,
		indexer:  indexer,
		queue:    queue,
	}
}

func (c *Controller) processNextItem() bool {
	// Wait until there is a new item in the working queue
	ev, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(ev)
	err := c.processItem(ev.(Event))
	// Handle the error if something went wrong during the execution of the business logic
	c.handleErr(err, ev)
	return true
}

func (c *Controller) processItem(ev Event) error {
	obj, _, err := c.indexer.GetByKey(ev.key)
	if err != nil {
		glog.Errorf("Fetching object with key %s from store failed with %v", ev.key, err)
		return err
	}

	assertedObj, ok := obj.(*v1.Event)
	if !ok {
		return nil
	}
	hpaInfo := getHpaInfo(assertedObj)

	if hpaInfo.refKind != "Deployment" {
		glog.Warningln("Emergency scale supports only Deployment")
		return nil
	}

	switch ev.eventType {
	case "ADDED":
		objectMeta := assertedObj.ObjectMeta
		//createならスケール実行する
		//起動時に取得する既存のlistは出力させない
		if ev.send && objectMeta.CreationTimestamp.Sub(serverStartTime).Seconds() > 0 {
			glog.Infoln("detect FailedComputeMetricsReplicas (create)")
			if validateScale(hpaInfo) {
				err := emergencyScale(hpaInfo)
				if err != nil {
					return err
				}
			}
			return nil
		}
	case "MODIFIED":
		//updateでスケール実行の場合はわりとしっかりフィルタが必要そう（暴走によるスケール地獄が怖い…）
		//不定期に起こる謎のupdateを排除するためlastTimestampから1分未満の時だけpost
		if ev.send && time.Now().Local().Unix()-assertedObj.LastTimestamp.Unix() < 60 {
			glog.Infoln("detect FailedComputeMetricsReplicas (update)")
			if validateScale(hpaInfo) {
				err := emergencyScale(hpaInfo)
				if err != nil {
					return err
				}
				return nil
			}
		}
	case "DELETED":
		//なにもしない
		return nil
	}
	return nil
}

// handleErr checks if an error happened and makes sure we will retry later.
func (c *Controller) handleErr(err error, key interface{}) {
	if err == nil {
		c.queue.Forget(key)
		return
	}

	if c.queue.NumRequeues(key) < maxRetries {
		glog.Errorf("Error syncing Event %v: %v", key, err)
		c.queue.AddRateLimited(key)
		return
	}

	c.queue.Forget(key)
	runtime.HandleError(err)
	glog.Infof("Dropping Event %q out of the queue: %v", key, err)
}

func (c *Controller) Run(stopCh chan struct{}) {
	defer runtime.HandleCrash()
	defer c.queue.ShutDown()
	glog.Infoln("Starting Event controller")
	serverStartTime = time.Now().Local()

	go c.informer.Run(stopCh)

	// Wait for all involved caches to be synced, before processing items from the queue is started
	if !cache.WaitForCacheSync(stopCh, c.informer.HasSynced) {
		runtime.HandleError(fmt.Errorf("Timed out waiting for caches to sync"))
		return
	}

	go wait.Until(c.runWorker, time.Second, stopCh)

	<-stopCh
	glog.Infoln("Stopping Event controller")
}

func (c *Controller) runWorker() {
	for c.processNextItem() {
	}
}

func hpaFailedSelect() fields.Selector {
	var selectors []fields.Selector
	selectors = append(selectors, fields.OneTermEqualSelector("involvedObject.kind", "HorizontalPodAutoscaler"))
	selectors = append(selectors, fields.OneTermEqualSelector("reason", "FailedComputeMetricsReplicas"))
	return fields.AndSelectors(selectors...)
}

func watchStart() {
	//fieldSelector := makeFieldSelector(c.FieldSelectors)
	eventListWatcher := cache.NewListWatchFromClient(client.CoreV1().RESTClient(), "events", "", hpaFailedSelect())
	queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
	indexer, informer := cache.NewIndexerInformer(eventListWatcher, &v1.Event{}, 0, resourceEventHandlerFuncs(queue), cache.Indexers{})
	controller := NewController(queue, indexer, informer)
	stop := make(chan struct{})
	defer close(stop)
	go controller.Run(stop)

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGTERM)
	signal.Notify(sigterm, syscall.SIGINT)
	<-sigterm
}

func resourceEventHandlerFuncs(queue workqueue.RateLimitingInterface) cache.ResourceEventHandlerFuncs {
	var ev Event
	return cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(obj)
			ev.key = key
			ev.eventType = "ADDED"
			ev.send = true
			if err == nil {
				queue.Add(ev)
			}
		},
		UpdateFunc: func(old, new interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(old)
			ev.key = key
			ev.eventType = "MODIFIED"
			ev.send = true
			if err == nil {
				queue.Add(ev)
			}
		},
		DeleteFunc: func(obj interface{}) {
			// IndexerInformer uses a delta queue, therefore for deletes we have to use this
			// key function.
			key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
			ev.key = key
			ev.eventType = "DELETED"
			ev.send = true
			if err == nil {
				queue.Add(ev)
			}
		},
	}
}

func getHpaInfo(event *v1.Event) HpaInfo {
	ns := event.ObjectMeta.Namespace
	name := event.InvolvedObject.Name
	out, err := client.AutoscalingV1().HorizontalPodAutoscalers(ns).Get(name, meta_v1.GetOptions{})
	if err != nil {
		panic(err)
	}
	return HpaInfo{
		refKind:         out.Spec.ScaleTargetRef.Kind,
		refName:         out.Spec.ScaleTargetRef.Name,
		namespace:       out.ObjectMeta.Namespace,
		currentReplicas: out.Status.CurrentReplicas,
	}
}

//updateの場合にやるかどうかを判定する
//直近x分以内に対象DeploymentにScalingReplicaSetのScaled upイベントが起こったかどうかとか
func validateScale(hpa HpaInfo) bool {
	lastScaleUpTime := getLastScaleUpTime(hpa)
	lastUpdateBefore := time.Now().Local().Unix() - lastScaleUpTime.Unix()
	glog.Infof("last update time : %v\n", lastScaleUpTime)
	if lastUpdateBefore < lastUpdateBorderSec {
		glog.Infof("not execute scale since last scale was %v seconds before\n", lastUpdateBefore)
		return false
	}

	unhealthyEvent := getUnhealthyEvents(hpa.namespace)
	unhealthyEvent = filterEventByName(unhealthyEvent,hpa.refName)
	if os.Getenv("UNHEALTHY_ONLY_LIVENESS") == "TRUE" {
		unhealthyEvent = filterEventOnlyLivenessFail(unhealthyEvent)
	}
	unhealthyEvent = filterEventByTime(unhealthyEvent,unhealthyBorderSec)
	unhealthyPodCount := itemsUniqPodCount(unhealthyEvent)
	unhealthyPodPercentage := 100 * unhealthyPodCount / hpa.currentReplicas
	glog.Infof("unhealthy pod : %v\n", unhealthyPodCount)
	glog.Infof("current replicas : %v\n", hpa.currentReplicas)
	glog.Infof("unhealthy pod percentage : %v\n", unhealthyPodPercentage)
	if unhealthyPodPercentage < unhealthyRateForScale {
		glog.Infof("not execute scale since unhealthy pod percentage (%v%%) is below border for scale (%v%%)\n", unhealthyPodPercentage,unhealthyRateForScale)
		return false
	}

	glog.Infof("execute scale %v to %v\n", hpa.currentReplicas, hpa.currentReplicas*multiplySpec)
	return true
}

func getLastScaleUpTime(hpa HpaInfo) meta_v1.Time {
	opt := meta_v1.ListOptions{
		FieldSelector: "involvedObject.kind=Deployment,reason=ScalingReplicaSet,involvedObject.name=" + hpa.refName,
	}
	cli := client.CoreV1().Events(hpa.namespace)
	out, err := cli.List(opt)
	if err != nil {
		panic(err)
	}
	var lastScaleUpTime meta_v1.Time
	reg := regexp.MustCompile(`^Scaled up`)
	for _, e := range out.Items {
		if lastScaleUpTime.Before(&e.LastTimestamp) && reg.MatchString(e.Message) {
			lastScaleUpTime = e.LastTimestamp
		}
	}
	return lastScaleUpTime
}

func getUnhealthyEvents(namespace string) []v1.Event {
	opt := meta_v1.ListOptions{
		FieldSelector: "type=Warning,reason=Unhealthy",
	}
	cli := client.CoreV1().Events(namespace)
	out, err := cli.List(opt)
	if err != nil {
		panic(err)
	}
	return out.Items
}

func filterEventByName(events []v1.Event, name string) []v1.Event {
  var ret []v1.Event
  r := regexp.MustCompile(`^` + name)
  for _, e := range events {
    if r.MatchString(e.InvolvedObject.Name) {
      ret = append(ret,e)
    }
  }
  return ret
}

func filterEventOnlyLivenessFail(events []v1.Event) []v1.Event {
  var ret []v1.Event
  r := regexp.MustCompile(`^Liveness`)
  for _, e := range events {
    if r.MatchString(e.Message) {
      ret = append(ret,e)
    }
  }
  return ret
}

//lasttimestampがsec秒以内のeventだけを返す
func filterEventByTime(events []v1.Event, sec int64) []v1.Event {
  var ret []v1.Event
  for _, e := range events {
    if time.Now().Local().Unix() - e.LastTimestamp.Unix() < sec {
      ret = append(ret,e)
    }
  }
  return ret
}

func itemsUniqPodCount(events []v1.Event) int32 {
  var uniq = map[string]bool{}
  for _, e := range events {
    p := e.InvolvedObject.Name
    if !uniq[p] {
      uniq[p] = true
    }
  }
  return int32(len(uniq))
}

func emergencyScale(hpa HpaInfo) error {
	cli := client.AppsV1beta2().Deployments(hpa.namespace)
	out, err := cli.Get(hpa.refName, meta_v1.GetOptions{})
	if err != nil {
		return err
	}
	out.Spec.Replicas = func(i int32) *int32 {
		return &i
	}(hpa.currentReplicas * multiplySpec)
	_, err = cli.Update(out)
	if err != nil {
		return err
	}
	err = putEvent()
	return err
}

func putEvent() error {
	pod, err := client.CoreV1().Pods(podNamespace).Get(podName, meta_v1.GetOptions{})
	if err != nil {
		return err
	}
	ref, err := reference.GetReference(scheme.Scheme, pod)
	if err != nil {
		return err
	}
	eventRecorder.Event(ref, v1.EventTypeWarning, "EmergencyPodScaling", "exec emergency scale since HPA failed get resource metrics")
	return nil
}

func main() {
	flag.Parse()
	glog.Infof("MULTIPLY_SPEC : %v\n", multiplySpec)
	glog.Infof("UPDATE_BORDER_SEC : %v\n", lastUpdateBorderSec)
	glog.Infof("POD_NAME : %v\n", podName)
	glog.Infof("POD_NAMESPACE : %v\n", podNamespace)
	glog.Infof("UNHEALTHY_BORDER_SEC  : %v\n", unhealthyBorderSec)
	glog.Infof("UNHEALTHY_RATE_FOR_SCALE  : %v\n", unhealthyRateForScale)
	if os.Getenv("UNHEALTHY_ONLY_LIVENESS") == "TRUE" {
		glog.Infoln("UNHEALTHY_ONLY_LIVENESS  : TRUE")
	} else {
		glog.Infoln("UNHEALTHY_ONLY_LIVENESS  : FALSE")
	}
	watchStart()
}
