package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/mitchellh/go-homedir"

	"github.com/golang/glog"
	v1 "k8s.io/api/core/v1"
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
POD_NAME : このpodの名前
POD_NAMESPACE : このpodのnamespace
*/

const (
	maxRetries                   = 3
	defaultLastUpdateBorderSec   = 300
	defaultMultiplySpec          = 2
	defaultUnhealthyBorderSec    = 180
	defaultUnhealthyRateForScale = 50
	defaultUnhealthyOnlyLiveness = true
	defaultPodName               = "kube-emerg-pod-scaler"
	defaultPodNamespace          = "default"
	defaultCondFalsePctAbnormal  = 60
	defaultCondFalsePctRegular   = 80
	defaultRegularMonitoring     = false
	defaultMonitorIntervalSec    = 60
)

var (
	lastUpdateBorderSec   = flag.Int("lastUpdateBorderSec", defaultLastUpdateBorderSec, "Cooldown seconds since last scaling.")
	multiplySpec          = flag.Int("multiplySpec", defaultMultiplySpec, "Scaling magnification.")
	unhealthyBorderSec    = flag.Int("unhealthyBorderSec", defaultUnhealthyBorderSec, "Seconds to detect unhealthy.")
	unhealthyRateForScale = flag.Int("unhealthyRateForScale", defaultUnhealthyRateForScale, "Threshold of Unhealthy.")
	unhealthyOnlyLiveness = flag.Bool("unhealthyOnlyLiveness", defaultUnhealthyOnlyLiveness, "Judge Unhealthy only liveness")
	condFalsePctAbnormal  = flag.Int("condFalsePctAbnormal", defaultCondFalsePctAbnormal, "Threshold of `condition = false` (Abnormal time).")
	condFalsePctRegular   = flag.Int("condFalsePctRegular", defaultCondFalsePctRegular, "Threshold of `condition = false` (Regular time).")
	regularMonitoring     = flag.Bool("regularMonitoring", defaultRegularMonitoring, "Whether to regular monitoring.")
	monitorIntervalSec    = flag.Int("monitorIntervalSec", defaultMonitorIntervalSec, "Interval to regular monitoring.")
	regularMonitorTarget  = flag.String("regularMonitorTarget", "", "Target deployment of regular monitoring.")
)

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
	name            string //HPAの名前
	refKind         string //Referenceの種類（Deployment想定）
	refName         string //Referenceの名前
	namespace       string
	currentReplicas int
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
	hpaInfo, err := getHpaInfo(assertedObj)
	if err != nil {
		glog.Errorf("Error on get HPA info : %v", err)
		return nil
	}

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
				err := newEmergencyScale(hpaInfo)
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
				err := newEmergencyScale(hpaInfo)
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

func getHpaInfo(event *v1.Event) (HpaInfo, error) {
	ns := event.ObjectMeta.Namespace
	name := event.InvolvedObject.Name
	out, err := client.AutoscalingV1().HorizontalPodAutoscalers(ns).Get(name, meta_v1.GetOptions{})
	if err != nil {
		return HpaInfo{}, err
	}
	return HpaInfo{
		name:            name,
		refKind:         out.Spec.ScaleTargetRef.Kind,
		refName:         out.Spec.ScaleTargetRef.Name,
		namespace:       out.ObjectMeta.Namespace,
		currentReplicas: int(out.Status.CurrentReplicas),
	}, nil
}

//updateの場合にやるかどうかを判定する
//直近x分以内に対象DeploymentにScalingReplicaSetのScaled upイベントが起こったかどうかとか
func validateScale(hpa HpaInfo) bool {
	if validateLastScaleTime(hpa) {
		unhealthyEvent := getUnhealthyEvents(hpa.namespace)
		unhealthyEvent = filterEventByName(unhealthyEvent, hpa.refName)
		if *unhealthyOnlyLiveness {
			unhealthyEvent = filterEventOnlyLivenessFail(unhealthyEvent)
		}
		unhealthyEvent = filterEventByTime(unhealthyEvent, *unhealthyBorderSec)
		unhealthyPodCount := itemsUniqPodCount(unhealthyEvent)
		unhealthyPodPercentage := 100 * unhealthyPodCount / hpa.currentReplicas
		glog.Infof("unhealthy pod : %v\n", unhealthyPodCount)
		glog.Infof("current replicas : %v\n", hpa.currentReplicas)
		glog.Infof("unhealthy pod percentage : %v\n", unhealthyPodPercentage)
		if unhealthyPodPercentage < *unhealthyRateForScale {
			glog.Infof("not execute scale since unhealthy pod percentage (%v%%) is below border for scale (%v%%)\n", unhealthyPodPercentage, *unhealthyRateForScale)
			glog.Infof("try next evaluation\n")
		} else {
			glog.Infof("execute scale %v to %v\n", hpa.currentReplicas, hpa.currentReplicas**multiplySpec)
			return true
		}

		allPods, falsePods := conditionFalsePodInfo(hpa.refName)
		conditionFalsePodPercentage := 100 * falsePods / allPods
		glog.Infof("deployment %v pod : %v\n", hpa.refName, allPods)
		glog.Infof("conditionFalse pod : %v\n", falsePods)
		glog.Infof("conditionFalse pod percentage : %v\n", conditionFalsePodPercentage)
		if conditionFalsePodPercentage < *condFalsePctAbnormal {
			glog.Infof("not execute scale since conditionFalse pod percentage (%v%%) is below border for scale (%v%%)\n", conditionFalsePodPercentage, *condFalsePctAbnormal)
			glog.Infof("try next evaluation\n")
		} else {
			glog.Infof("execute scale %v to %v\n", hpa.currentReplicas, hpa.currentReplicas**multiplySpec)
			return true
		}

		glog.Infof("all evaluations ended\n")
	}
	return false
}

//DeploymentやHPAにはスケールした時間の記録がない（？）のでイベントから取る
//なのでイベントが期限切れで消えていたらゼロ値が返る
func getLastScaleUpTime(hpa HpaInfo) meta_v1.Time {
	var lastScaleUpTime meta_v1.Time
	opt := meta_v1.ListOptions{
		FieldSelector: "involvedObject.kind=Deployment,reason=ScalingReplicaSet,involvedObject.name=" + hpa.refName,
	}
	cli := client.CoreV1().Events(hpa.namespace)
	out, err := cli.List(opt)
	if err != nil {
		glog.Error(err)
		return lastScaleUpTime
	}
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
		glog.Error(err)
		return []v1.Event{}
	}
	return out.Items
}

func filterEventByName(events []v1.Event, name string) []v1.Event {
	var ret []v1.Event
	//podの名前はdeploymentの名前がサフィックスになるので正規表現でdeploymentが作ったpodをfilterできる
	//ただしこの方法は`deployment hoge`と`deployment hoge2`がdeployされている場合hogeだけをfilterできない
	r := regexp.MustCompile(`^` + name)
	for _, e := range events {
		if r.MatchString(e.InvolvedObject.Name) {
			ret = append(ret, e)
		}
	}
	return ret
}

func filterEventOnlyLivenessFail(events []v1.Event) []v1.Event {
	var ret []v1.Event
	r := regexp.MustCompile(`^Liveness`)
	for _, e := range events {
		if r.MatchString(e.Message) {
			ret = append(ret, e)
		}
	}
	return ret
}

//lasttimestampがsec秒以内のeventだけを返す
func filterEventByTime(events []v1.Event, sec int) []v1.Event {
	var ret []v1.Event
	for _, e := range events {
		if int(time.Now().Local().Unix()-e.LastTimestamp.Unix()) < sec {
			ret = append(ret, e)
		}
	}
	return ret
}

func itemsUniqPodCount(events []v1.Event) int {
	var uniq = map[string]bool{}
	for _, e := range events {
		p := e.InvolvedObject.Name
		if !uniq[p] {
			uniq[p] = true
		}
	}
	return len(uniq)
}

//DeploymentからPodListを取得してcondition=falseなpodを取得
func conditionFalsePodInfo(deploymentName string) (int, int) {
	allPods, err := client.CoreV1().Pods("").List(meta_v1.ListOptions{})
	if err != nil {
		glog.Errorf("Failed to get Pod list : %v", err)
		return 0, 0
	}
	var pods []v1.Pod
	var falsePods []v1.Pod
	r := regexp.MustCompile(`^` + deploymentName)
	for _, p := range allPods.Items {
		if r.MatchString(p.ObjectMeta.Name) {
			pods = append(pods, p)
		}
	}
	for _, p := range pods {
		//type:readyのstatusがfalseな場合falsePods入り
		for _, c := range p.Status.Conditions {
			if c.Type == v1.PodReady && c.Status == v1.ConditionFalse {
				falsePods = append(falsePods, p)
			}
		}
	}
	return len(pods), len(falsePods)
}

//DeploymentからPodListを取得してUnhealthyイベントを起こしたpodを取得
func conditionUnhealthPodInfo(deploymentName string) (int, int) {
	o := meta_v1.ListOptions{
		LabelSelector: labelSelectorStringFromDeployment(deploymentName),
	}
	ap, e := client.CoreV1().Pods("").List(o)
	if e != nil {
		glog.Errorf("Unable to get Pods list : %v", e)
		return 0, 0
	}
	var unhealthyPod []v1.Pod
	for _, p := range ap.Items {
		if isUnhealthyPod(p) {
			unhealthyPod = append(unhealthyPod, p)
		}
	}
	return len(ap.Items), len(unhealthyPod)
}

func labelSelectorStringFromDeployment(deploymentName string) string {
	var s []fields.Selector
	s = append(s, fields.OneTermEqualSelector("metadata.name", deploymentName))
	o := meta_v1.ListOptions{
		FieldSelector: fields.AndSelectors(s...).String(),
	}
	dl, e := client.AppsV1().Deployments("").List(o)
	if e != nil {
		glog.Errorf("Unable to get Deployments list : %v", e)
		return "dummykey=dummyvalue"
	}
	if len(dl.Items) == 0 {
		glog.Errorf("can't find Deployment : %v", deploymentName)
		return "dummykey=dummyvalue"
	}
	d := dl.Items[0]
	ret := ""
	for k, v := range d.Spec.Template.ObjectMeta.Labels {
		if ret == "" {
			ret = k + "=" + v
		} else {
			ret = ret + "," + k + "=" + v
		}
	}
	if ret == "" {
		return "dummykey=dummyvalue"
	}
	return ret
}

func isUnhealthyPod(pod v1.Pod) bool {
	var s []fields.Selector
	s = append(s, fields.OneTermEqualSelector("involvedObject.name", pod.Name))
	s = append(s, fields.OneTermEqualSelector("type", "Warning"))
	s = append(s, fields.OneTermEqualSelector("reason", "Unhealthy"))
	o := meta_v1.ListOptions{
		FieldSelector: fields.AndSelectors(s...).String(),
	}
	l, e := client.CoreV1().Events(pod.Namespace).List(o)
	if e != nil {
		glog.Errorf("Unable to get Events list : %v", e)
		return false
	}
	i := filterEventByTime(l.Items, *unhealthyBorderSec)
	if *unhealthyOnlyLiveness {
		i = filterEventOnlyLivenessFail(i)
	}
	if len(i) > 0 {
		return true
	}
	return false
}

// Deprecated
func emergencyScale(hpa HpaInfo) error {
	cli := client.AppsV1beta2().Deployments(hpa.namespace)
	out, err := cli.Get(hpa.refName, meta_v1.GetOptions{})
	if err != nil {
		return err
	}
	out.Spec.Replicas = func(i int32) *int32 {
		return &i
	}(int32(hpa.currentReplicas * *multiplySpec))
	_, err = cli.Update(out)
	if err != nil {
		return err
	}
	err = putEvent()
	return err
}

func newEmergencyScale(hpa HpaInfo) error {
	cli := client.AutoscalingV2beta1().HorizontalPodAutoscalers(hpa.namespace)
	tHpa, err := cli.Get(hpa.name, meta_v1.GetOptions{})
	if err != nil {
		return err
	}
	cmin := tHpa.Spec.MinReplicas
	cr := tHpa.Status.CurrentReplicas
	tmin := cr * int32(*multiplySpec)
	tHpa.Spec.MinReplicas = &tmin
	glog.Infof("current min replicas: %v", *cmin)
	glog.Infof("current current replicas: %v", cr)
	glog.Infof("target min replicas: %v", tmin)
	_, err = cli.Update(tHpa)
	if err != nil {
		return err
	}

	glog.Info("wait for scale...")
	for {
		nHpa, err := cli.Get(hpa.name, meta_v1.GetOptions{})
		if err != nil {
			glog.Error(err)
			continue
		}
		glog.Infof("target replicas: %v, current replicas: %v", tmin, nHpa.Status.CurrentReplicas)
		if nHpa.Status.CurrentReplicas >= tmin {
			break
		}
		time.Sleep(10 * time.Second)
	}
	glog.Info("scale completed.")
	glog.Info("restore previous min replicas.")

	fHpa, err := cli.Get(hpa.name, meta_v1.GetOptions{})
	if err != nil {
		return err
	}
	fHpa.Spec.MinReplicas = cmin
	_, err = cli.Update(fHpa)
	if err != nil {
		return err
	}

	eHpa, err := cli.Get(hpa.name, meta_v1.GetOptions{})
	if err != nil {
		return err
	}
	glog.Infof("finish restore for min replicas: %v", *eHpa.Spec.MinReplicas)

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

//通常の見守り
func execRegularMonitoring(targetDeployments string) {
	targets := strings.Split(targetDeployments, ",")
	for _, d := range targets {
		ua, uf := conditionUnhealthPodInfo(d)
		fa, ff := conditionFalsePodInfo(d)
		if ua == 0 || fa == 0 {
			glog.Errorf("pods of %v is zero", d)
			continue
		}
		ufp := 100 * uf / ua
		ffp := 100 * ff / fa
		var conditionFalsePodPercentage, allPods, falsePods int
		if ufp > ffp {
			conditionFalsePodPercentage = ufp
			allPods = ua
			falsePods = uf
		} else {
			conditionFalsePodPercentage = ffp
			allPods = fa
			falsePods = ff
		}
		glog.Infof("regular monitoring : Deployment = %v, AllPodCount = %v, ConditionFalsePodCount = %v, ConditionFalsePercentage = %v", d, allPods, falsePods, conditionFalsePodPercentage)
		if conditionFalsePodPercentage > *condFalsePctRegular {
			glog.Infof("ConditionFalsePercentage exceeds border\n")
			hpa, err := getHpaInfoFromDeploymentName(d)
			if err != nil {
				glog.Errorf("Failed to get HPA list : %v", err)
				continue
			}
			//前回のscale判定
			if validateLastScaleTime(hpa) {
				//scaleする処理
				glog.Infof("execute scale %v to %v\n", hpa.currentReplicas, hpa.currentReplicas**multiplySpec)
				err := newEmergencyScale(hpa)
				if err != nil {
					glog.Errorf("Failed to exec emergency scale : %v", err)
				}
			}
		}
	}
}

func getHpaInfoFromDeploymentName(deploymentName string) (HpaInfo, error) {
	var ret HpaInfo
	out, err := client.AutoscalingV1().HorizontalPodAutoscalers("").List(meta_v1.ListOptions{})
	if err != nil {
		return ret, err
	}
	for _, h := range out.Items {
		if h.Spec.ScaleTargetRef.Name == deploymentName {
			return HpaInfo{
				name:            h.ObjectMeta.Name,
				refKind:         h.Spec.ScaleTargetRef.Kind,
				refName:         h.Spec.ScaleTargetRef.Name,
				namespace:       h.ObjectMeta.Namespace,
				currentReplicas: int(h.Status.CurrentReplicas),
			}, nil
		}
	}
	return ret, fmt.Errorf("failed to get HPA. %v is not exist in HPA list", deploymentName)
}

func validateLastScaleTime(hpa HpaInfo) bool {
	lastScaleUpTime := getLastScaleUpTime(hpa)
	lastUpdateBefore := int(time.Now().Local().Unix() - lastScaleUpTime.Unix())
	if lastScaleUpTime.IsZero() {
		glog.Infof("last update was over 1 hour ago")
	} else {
		glog.Infof("last update time : %v\n", lastScaleUpTime)
	}
	if lastUpdateBefore < *lastUpdateBorderSec {
		glog.Infof("not execute scale since last scale was %v seconds before\n", lastUpdateBefore)
		return false
	} else {
		return true
	}
}

func main() {
	flag.Parse()
	if *regularMonitoring && *regularMonitorTarget == "" {
		glog.Errorf("require option regularMonitorTarget")
		os.Exit(1)
	}
	glog.Infof("MULTIPLY_SPEC : %v\n", *multiplySpec)
	glog.Infof("UPDATE_BORDER_SEC : %v\n", *lastUpdateBorderSec)
	glog.Infof("POD_NAME : %v\n", podName)
	glog.Infof("POD_NAMESPACE : %v\n", podNamespace)
	glog.Infof("UNHEALTHY_BORDER_SEC : %v\n", *unhealthyBorderSec)
	glog.Infof("UNHEALTHY_RATE_FOR_SCALE : %v\n", *unhealthyRateForScale)
	glog.Infof("UNHEALTHY_ONLY_LIVENESS : %v\n", *unhealthyOnlyLiveness)
	glog.Infof("COND_FALSE_PCT_ABNORMAL : %v\n", *condFalsePctAbnormal)
	glog.Infof("REGULAR_MONITORING : %v\n", *regularMonitoring)
	if *regularMonitoring {
		glog.Infof("COND_FALSE_PCT_REGULAR : %v\n", *condFalsePctRegular)
		glog.Infof("MONITOR_INTERVAL_SEC : %v\n", *monitorIntervalSec)
		glog.Infof("REGULAR_MONITOR_TARGET : %v\n", *regularMonitorTarget)
	}

	if *regularMonitoring {
		go func() {
			for {
				execRegularMonitoring(*regularMonitorTarget)
				time.Sleep(time.Duration(*monitorIntervalSec) * time.Second)
			}
		}()
	}

	watchStart()
}
