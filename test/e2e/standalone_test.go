package e2e

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	k8syaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	testutils "github.com/llm-d/llm-d-router/test/utils"
)

const standaloneChart = "../../config/charts/llm-d-router-standalone"

type standaloneRouter struct {
	objects  []*unstructured.Unstructured
	selector map[string]string
	poolName string
}

func standaloneImageValues(image string) (map[string]string, error) {
	if image == "" || strings.ContainsAny(image, "@ \t\n") {
		return nil, fmt.Errorf("EPP_IMAGE must be a tagged image name, got %q", image)
	}
	name, tag := image, "latest"
	if colon := strings.LastIndex(image, ":"); colon > strings.LastIndex(image, "/") {
		name, tag = image[:colon], image[colon+1:]
	}
	registry, repository := "docker.io", name
	if first, rest, found := strings.Cut(name, "/"); found {
		if strings.ContainsAny(first, ".:") || first == "localhost" {
			registry, repository = first, rest
		}
	} else {
		repository = "library/" + name
	}
	if name == "" || repository == "" || tag == "" {
		return nil, fmt.Errorf("invalid EPP_IMAGE %q", image)
	}
	return map[string]string{"registry": registry, "repository": repository, "tag": tag, "pullPolicy": "IfNotPresent"}, nil
}

func renderStandaloneRouter(ctx context.Context, namespace, image, plugins string, replicas int, targetPorts []int32) (*standaloneRouter, error) {
	imageValues, err := standaloneImageValues(image)
	if err != nil {
		return nil, err
	}
	ports := make([]map[string]int32, len(targetPorts))
	for i, port := range targetPorts {
		ports[i] = map[string]int32{"number": port}
	}
	values, err := yaml.Marshal(map[string]any{
		"router": map[string]any{
			"modelServers": map[string]any{"matchLabels": podSelector, "targetPorts": ports},
			"epp": map[string]any{
				"image": imageValues, "replicas": replicas,
				"pluginsCustomConfig": map[string]string{"epp-config.yaml": plugins},
			},
		},
	})
	if err != nil {
		return nil, err
	}
	command := exec.CommandContext(ctx, "helm", "template", poolName, standaloneChart,
		"--namespace", namespace, "-f", "standalone-values.yaml", "-f", "-")
	command.Stdin = bytes.NewReader(values)
	var stderr bytes.Buffer
	command.Stderr = &stderr
	output, err := command.Output()
	if err != nil {
		return nil, fmt.Errorf("render standalone chart: %w: %s", err, stderr.String())
	}
	objects, err := decodeCaseObjects(output, namespace)
	if err != nil {
		return nil, err
	}
	router := &standaloneRouter{objects: objects}
	for _, obj := range objects {
		switch obj.GetKind() {
		case "Deployment":
			if router.selector != nil {
				return nil, errors.New("standalone chart must have one router Deployment")
			}
			router.selector, _, err = unstructured.NestedStringMap(obj.Object, "spec", "selector", "matchLabels")
			if err != nil {
				return nil, err
			}
		case "InferencePool":
			router.poolName = obj.GetName()
		}
	}
	if len(router.selector) == 0 || router.poolName == "" {
		return nil, errors.New("standalone chart must render a router selector and InferencePool")
	}
	return router, nil
}

func decodeCaseObjects(data []byte, namespace string) ([]*unstructured.Unstructured, error) {
	var objects []*unstructured.Unstructured
	decoder := k8syaml.NewYAMLOrJSONDecoder(bytes.NewReader(data), 4096)
	for {
		obj := &unstructured.Unstructured{}
		if err := decoder.Decode(obj); err != nil {
			if errors.Is(err, io.EOF) {
				return objects, nil
			}
			return nil, fmt.Errorf("decode case resource: %w", err)
		}
		if len(obj.Object) == 0 {
			continue
		}
		obj.SetNamespace(namespace)
		objects = append(objects, obj)
	}
}

func (r *standaloneRouter) accessService(namespace string, httpPort, metricsPort int) *corev1.Service {
	return &corev1.Service{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Service"},
		ObjectMeta: metav1.ObjectMeta{Name: "router-access", Namespace: namespace},
		Spec: corev1.ServiceSpec{
			Type: corev1.ServiceTypeNodePort, Selector: r.selector,
			Ports: []corev1.ServicePort{
				{Name: "http", Port: 8081, TargetPort: intstr.FromInt32(8081), NodePort: int32(httpPort)},
				{Name: "metrics", Port: 9090, TargetPort: intstr.FromInt32(9090), NodePort: int32(metricsPort)},
			},
		},
	}
}

type caseResources struct {
	client  client.Client
	created []*unstructured.Unstructured
}

func (r *caseResources) create(ctx context.Context, objects []*unstructured.Unstructured) error {
	for _, obj := range objects {
		if err := r.client.Create(ctx, obj); err != nil {
			return fmt.Errorf("create %s/%s: %w", obj.GetKind(), obj.GetName(), err)
		}
		r.created = append(r.created, obj.DeepCopy())
	}
	return nil
}

func (r *caseResources) delete(ctx context.Context) error {
	var errs []error
	for i := len(r.created) - 1; i >= 0; i-- {
		obj := r.created[i]
		uid := obj.GetUID()
		err := r.client.Delete(ctx, obj, client.PropagationPolicy(metav1.DeletePropagationForeground),
			client.Preconditions{UID: &uid})
		if err != nil && !apierrors.IsNotFound(err) {
			errs = append(errs, fmt.Errorf("delete %s/%s: %w", obj.GetKind(), obj.GetName(), err))
		}
	}
	return errors.Join(errs...)
}

func (r *caseResources) deleted(ctx context.Context) (bool, error) {
	for _, obj := range r.created {
		current := &unstructured.Unstructured{}
		current.SetGroupVersionKind(obj.GroupVersionKind())
		err := r.client.Get(ctx, client.ObjectKeyFromObject(obj), current)
		if err == nil && current.GetUID() == obj.GetUID() {
			return false, nil
		}
		if err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}
		if obj.GetKind() == "Deployment" {
			selector, _, err := unstructured.NestedStringMap(obj.Object, "spec", "selector", "matchLabels")
			if err != nil {
				return false, err
			}
			pods := &corev1.PodList{}
			if err := r.client.List(ctx, pods, client.InNamespace(obj.GetNamespace()), client.MatchingLabels(selector)); err != nil {
				return false, err
			}
			if len(pods.Items) != 0 {
				return false, nil
			}
		}
	}
	return true, nil
}

func deferCaseCleanup(resources *caseResources, namespace string, stop func()) {
	ginkgo.DeferCleanup(func() {
		if stop != nil {
			stop()
		}
		if ginkgo.CurrentSpecReport().Failed() && keepClusterOnFailure {
			testutils.DumpPodsAndLogs(testConfig, namespace)
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), readyTimeout)
		defer cancel()
		gomega.Expect(resources.delete(ctx)).To(gomega.Succeed())
		gomega.Eventually(func() (bool, error) {
			return resources.deleted(ctx)
		}, readyTimeout, interval).Should(gomega.BeTrue(), "case resources and their Pods must terminate before names are reused")
	})
}

func createStandaloneRouter(plugins string, replicas int, targetPorts ...int32) *standaloneRouter {
	namespace := getNamespace()
	ctx, cancel := context.WithTimeout(testConfig.Context, readyTimeout)
	defer cancel()
	router, err := renderStandaloneRouter(ctx, namespace, eppImage, plugins, replicas, targetPorts)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	access, err := runtime.DefaultUnstructuredConverter.ToUnstructured(router.accessService(namespace, getPort(), getMetricsPort()))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	router.objects = append(router.objects, &unstructured.Unstructured{Object: access})
	resources := &caseResources{client: testConfig.K8sClient}
	var stopForward func()
	deferCaseCleanup(resources, namespace, func() {
		if stopForward != nil {
			stopForward()
		}
	})
	ginkgo.By("Creating router resources from " + standaloneChart)
	gomega.Expect(resources.create(ctx, router.objects)).To(gomega.Succeed())
	waitForReadyLeader(replicas, namespace, router.selector)
	if k8sContext != "" {
		stopForward = startRouterPortForward(namespace, router.selector)
	}
	router.waitForRouting()
	return router
}

func (r *standaloneRouter) waitForRouting() {
	// Envoy's active health check can lag behind Kubernetes readiness.
	ginkgo.By("Waiting for the standalone proxy to reach EPP")
	probe := &http.Client{Timeout: 5 * time.Second}
	gomega.Eventually(func() bool {
		resp, err := probe.Get(fmt.Sprintf("http://localhost:%d/v1/models", getPort()))
		if err != nil {
			return false
		}
		defer func() { _ = resp.Body.Close() }()
		body, err := io.ReadAll(resp.Body)
		return err == nil && (resp.StatusCode == http.StatusOK || len(body) > 0)
	}, readyTimeout, time.Second).Should(gomega.BeTrue())
	waitForEPPToDiscoverPods(r.poolName)
}

func readyRouterPod(pod *corev1.Pod) bool {
	if pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning || len(pod.Status.ContainerStatuses) != 2 {
		return false
	}
	for _, container := range pod.Status.ContainerStatuses {
		if !container.Ready || container.State.Running == nil {
			return false
		}
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func standaloneReadyLeader(pods []corev1.Pod, replicas int) (*corev1.Pod, error) {
	live, ready := 0, 0
	var leader *corev1.Pod
	for _, pod := range pods {
		if pod.DeletionTimestamp != nil {
			continue
		}
		live++
		if pod.Status.Phase != corev1.PodRunning || len(pod.Status.ContainerStatuses) != 2 {
			return nil, fmt.Errorf("router Pod %s has not started both containers", pod.Name)
		}
		for _, container := range pod.Status.ContainerStatuses {
			if container.State.Running == nil || (container.Name == "envoy-proxy" && !container.Ready) {
				return nil, fmt.Errorf("router container %s/%s is not running", pod.Name, container.Name)
			}
		}
		if readyRouterPod(&pod) {
			ready++
			leader = pod.DeepCopy()
		}
	}
	if live != replicas || ready != 1 {
		return nil, fmt.Errorf("router has %d live Pods and %d Ready Pods, want %d and 1", live, ready, replicas)
	}
	return leader, nil
}

type forwardProcess struct {
	done <-chan struct{}
	stop func()
}

type routerPortForward struct {
	podUID  types.UID
	process *forwardProcess
	start   func(context.Context, *corev1.Pod) (*forwardProcess, error)
}

func (f *routerPortForward) close() {
	if f.process != nil {
		f.process.stop()
		<-f.process.done
		f.process = nil
		f.podUID = ""
	}
}

func (f *routerPortForward) reconcile(ctx context.Context, pods []corev1.Pod) error {
	var leader *corev1.Pod
	for _, pod := range pods {
		if readyRouterPod(&pod) {
			if leader != nil {
				f.close()
				return errors.New("multiple Ready router Pods")
			}
			leader = pod.DeepCopy()
		}
	}
	if f.process != nil {
		select {
		case <-f.process.done:
			f.close()
		default:
		}
	}
	if leader == nil || leader.UID != f.podUID {
		f.close()
	}
	if leader == nil || f.process != nil {
		return nil
	}
	process, err := f.start(ctx, leader)
	if err != nil {
		return err
	}
	f.process, f.podUID = process, leader.UID
	return nil
}

func startRouterPortForward(namespace string, selector map[string]string) func() {
	ctx, cancel := context.WithCancel(testConfig.Context)
	done := make(chan struct{})
	forward := &routerPortForward{start: func(ctx context.Context, pod *corev1.Pod) (*forwardProcess, error) {
		// #nosec G204 -- Fixed kubectl executable; API Pod names, integer ports and test settings are separate argv, without a shell.
		command := exec.CommandContext(ctx, "kubectl", "port-forward", "pod/"+pod.Name,
			fmt.Sprintf("%d:8081", getPort()), fmt.Sprintf("%d:9090", getMetricsPort()),
			"--context="+k8sContext, "--namespace="+namespace, "--address=127.0.0.1")
		command.Stdout, command.Stderr = ginkgo.GinkgoWriter, ginkgo.GinkgoWriter
		if err := command.Start(); err != nil {
			return nil, err
		}
		exited := make(chan struct{})
		go func() {
			_ = command.Wait()
			close(exited)
		}()
		return &forwardProcess{done: exited, stop: func() { _ = command.Process.Kill() }}, nil
	}}
	go func() {
		defer close(done)
		defer forward.close()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			pods := &corev1.PodList{}
			err := testConfig.K8sClient.List(ctx, pods, client.InNamespace(namespace), client.MatchingLabels(selector))
			if err == nil {
				err = forward.reconcile(ctx, pods.Items)
			}
			if err != nil && ctx.Err() == nil {
				ginkgo.GinkgoLogr.Error(err, "Router port-forward will retry")
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}
