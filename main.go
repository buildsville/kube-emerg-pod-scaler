package main

import (
	"fmt"
	"github.com/mitchellh/go-homedir"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"syscall"
	"time"

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

const (
	maxRetries                 = 5
	defaultLastUpdateBorderSec = 300
	defaultMultiplySpec        = 3
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
		fmt.Printf("Fetching object with key %s from store failed with %v", ev.key, err)
		return err
	}

	assertedObj, ok := obj.(*v1.Event)
	if !ok {
		return nil
	}
	hpaInfo := getHpaInfo(assertedObj)

	if hpaInfo.refKind != "Deployment" {
		fmt.Println("Emergency scale supports only Deployment")
		return nil
	}

	switch ev.eventType {
	case "ADDED":
		objectMeta := assertedObj.ObjectMeta
		//createならスケール実行する
		//起動時に取得する既存のlistは出力させない
		if ev.send && objectMeta.CreationTimestamp.Sub(serverStartTime).Seconds() > 0 {
			fmt.Println("detect FailedGetResourceMetric (create)")
			fmt.Printf("execute scale %v to %v\n",hpaInfo.currentReplicas,hpaInfo.currentReplicas * multiplySpec)
			err := emergencyScale(hpaInfo)
			if err != nil {
				return err
			}
			return nil
		}
	case "MODIFIED":
		//updateでスケール実行の場合はわりとしっかりフィルタが必要そう（暴走によるスケール地獄が怖い…）
		//不定期に起こる謎のupdateを排除するためlastTimestampから1分未満の時だけpost
		if ev.send && time.Now().Local().Unix() - assertedObj.LastTimestamp.Unix() < 60 {
			fmt.Println("detect FailedGetResourceMetric (update)")
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
		fmt.Printf("Error syncing Event %v: %v", key, err)
		c.queue.AddRateLimited(key)
		return
	}

	c.queue.Forget(key)
	runtime.HandleError(err)
	fmt.Printf("Dropping Event %q out of the queue: %v", key, err)
}

func (c *Controller) Run(stopCh chan struct{}) {
	defer runtime.HandleCrash()
	defer c.queue.ShutDown()
	fmt.Println("Starting Event controller")
	serverStartTime = time.Now().Local()

	go c.informer.Run(stopCh)

	// Wait for all involved caches to be synced, before processing items from the queue is started
	if !cache.WaitForCacheSync(stopCh, c.informer.HasSynced) {
		runtime.HandleError(fmt.Errorf("Timed out waiting for caches to sync"))
		return
	}

	go wait.Until(c.runWorker, time.Second, stopCh)

	<-stopCh
	fmt.Println("Stopping Event controller")
}

func (c *Controller) runWorker() {
	for c.processNextItem() {
	}
}

func hpaFailedSelect() fields.Selector {
	var selectors []fields.Selector
	selectors = append(selectors, fields.OneTermEqualSelector("involvedObject.kind", "HorizontalPodAutoscaler"))
	selectors = append(selectors, fields.OneTermEqualSelector("reason", "FailedGetResourceMetric"))
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
	opt := meta_v1.ListOptions{
		FieldSelector: "involvedObject.kind=Deployment,reason=ScalingReplicaSet,involvedObject.name=" + hpa.refName,
	}
	cli := client.CoreV1().Events(hpa.namespace)
	out, err := cli.List(opt)
	if err != nil{
			panic(err)
	}
	var lastScaleUpTime meta_v1.Time
	reg := regexp.MustCompile(`^Scaled up`)
	for _, e := range out.Items {
		if lastScaleUpTime.Before(&e.LastTimestamp) && reg.MatchString(e.Message) {
			lastScaleUpTime = e.LastTimestamp
		}
	}
	lastUpdateBefore := time.Now().Local().Unix() - lastScaleUpTime.Unix()
	if lastUpdateBefore > lastUpdateBorderSec {
		fmt.Printf("execute scale %v to %v\n",hpa.currentReplicas,hpa.currentReplicas * multiplySpec)
		return true
	} else {
		fmt.Printf("not execute scale since last scale was %v seconds before\n", lastUpdateBefore)
		return false
	}
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
	fmt.Println("MULTIPLY_SPEC :", multiplySpec)
	fmt.Println("UPDATE_BORDER_SEC :", lastUpdateBorderSec)
	fmt.Println("POD_NAME :", podName)
	fmt.Println("POD_NAMESPACE :", podNamespace)
	watchStart()
}
