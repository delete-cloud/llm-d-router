package e2e

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	infextv1 "sigs.k8s.io/gateway-api-inference-extension/api/v1"
)

func TestStandaloneChart(t *testing.T) {
	for _, tc := range []struct {
		name      string
		namespace string
		replicas  int
		ports     []int32
	}{
		{"single", "e2e-1", 1, []int32{8000}},
		{"leader-election", "e2e-2", 3, []int32{8000}},
		{"data-parallel", "e2e-3", 1, []int32{8000, 8001}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			image := "registry.example:5000/team/router:dev"
			router, err := renderStandaloneRouter(context.Background(), tc.namespace, image, simpleConfig, tc.replicas, tc.ports)
			if err != nil {
				t.Fatal(err)
			}
			objects := make(map[string]*unstructured.Unstructured)
			for _, obj := range router.objects {
				if obj.GetNamespace() != tc.namespace {
					t.Fatalf("%s/%s has namespace %q", obj.GetKind(), obj.GetName(), obj.GetNamespace())
				}
				objects[obj.GetKind()+"/"+obj.GetName()] = obj
			}
			for _, name := range []string{
				"Deployment/" + eppName, "Service/" + eppName,
				"ServiceAccount/" + eppName, "ConfigMap/" + eppName,
				"ConfigMap/envoy", "InferencePool/" + poolName,
				"Role/" + eppName + "-sa", "RoleBinding/" + eppName + "-sa",
				"Role/" + eppName + "-non-sa", "RoleBinding/" + eppName + "-non-sa",
			} {
				if objects[name] == nil {
					t.Fatalf("chart did not render %s", name)
				}
			}
			deploy := objects["Deployment/"+eppName]
			replicas, _, _ := unstructured.NestedInt64(deploy.Object, "spec", "replicas")
			if replicas != int64(tc.replicas) {
				t.Fatalf("replicas = %d, want %d", replicas, tc.replicas)
			}
			containers, _, err := unstructured.NestedSlice(deploy.Object, "spec", "template", "spec", "containers")
			if err != nil || len(containers) != 2 {
				t.Fatalf("expected Envoy and EPP containers, got %v (%v)", containers, err)
			}
			var epp map[string]any
			for _, container := range containers {
				c := container.(map[string]any)
				if c["name"] == "epp" {
					epp = c
				}
				if _, ok := c["readinessProbe"]; !ok {
					t.Fatalf("%s has no readiness probe", c["name"])
				}
			}
			if epp["image"] != image {
				t.Fatalf("image = %v, want %s", epp["image"], image)
			}
			if epp["imagePullPolicy"] != "IfNotPresent" {
				t.Fatalf("unexpected EPP pull policy: %v", epp["imagePullPolicy"])
			}
			args, _, _ := unstructured.NestedStringSlice(epp, "args")
			for _, flag := range []string{"--allow-experimental-plugins=true", "--drain-timeout=0", "--metrics-endpoint-auth=false"} {
				if !strings.Contains(strings.Join(args, "\n"), flag) {
					t.Errorf("missing EPP flag %s", flag)
				}
			}
			hasElection := strings.Contains(strings.Join(args, "\n"), "--ha-enable-leader-election")
			if hasElection != (tc.replicas > 1) {
				t.Fatalf("leader election enabled = %t for %d replicas", hasElection, tc.replicas)
			}
			if hasElection && objects["Role/"+eppName+"-leader-election"] == nil {
				t.Fatal("missing leader election RBAC")
			}
			config, _, _ := unstructured.NestedString(objects["ConfigMap/"+eppName].Object, "data", "epp-config.yaml")
			if strings.TrimSpace(config) != strings.TrimSpace(simpleConfig) {
				t.Fatal("plugin configuration changed during rendering")
			}
			envoy, _, _ := unstructured.NestedString(objects["ConfigMap/envoy"].Object, "data", "envoy.yaml")
			if !strings.Contains(envoy, "failure_mode_allow: false") || !strings.Contains(envoy, "address: 127.0.0.1") {
				t.Fatal("expected chart fail-closed sidecar proxy configuration")
			}
			pool := objects["InferencePool/"+poolName]
			ports, _, _ := unstructured.NestedSlice(pool.Object, "spec", "targetPorts")
			if len(ports) != len(tc.ports) {
				t.Fatalf("target ports = %v, want %v", ports, tc.ports)
			}
			for i, port := range ports {
				if port.(map[string]any)["number"] != int64(tc.ports[i]) {
					t.Fatalf("target port = %v, want %d", port, tc.ports[i])
				}
			}
			picker, _, _ := unstructured.NestedString(pool.Object, "spec", "endpointPickerRef", "name")
			if picker != eppName || router.poolName != poolName {
				t.Fatalf("pool or EPP service identity changed: %s, %s", router.poolName, picker)
			}
			service := router.accessService(tc.namespace, 30080, 32090)
			if !reflect.DeepEqual(service.Spec.Selector, router.selector) || service.Spec.Ports[0].NodePort != 30080 || service.Spec.Ports[1].NodePort != 32090 {
				t.Fatalf("invalid test access service: %+v", service.Spec)
			}
			ports, _, _ = unstructured.NestedSlice(objects["Service/"+eppName].Object, "spec", "ports")
			for _, want := range []int64{8081, 5557, 9090} {
				found := false
				for _, port := range ports {
					found = found || port.(map[string]any)["port"] == want
				}
				if !found {
					t.Errorf("EPP service missing port %d", want)
				}
			}
		})
	}
}

func TestStandaloneImageValues(t *testing.T) {
	for _, tc := range []struct{ image, registry, repository, tag string }{
		{"registry.example:5000/team/router:dev", "registry.example:5000", "team/router", "dev"},
		{"llm-d/router:dev", "docker.io", "llm-d/router", "dev"},
		{"router", "docker.io", "library/router", "latest"},
		{"localhost:5000/router", "localhost:5000", "router", "latest"},
	} {
		t.Run(tc.image, func(t *testing.T) {
			image, err := standaloneImageValues(tc.image)
			if err != nil {
				t.Fatal(err)
			}
			if image["registry"] != tc.registry || image["repository"] != tc.repository || image["tag"] != tc.tag {
				t.Fatalf("image values = %v", image)
			}
		})
	}
	if _, err := standaloneImageValues("router@sha256:abcd"); err == nil {
		t.Fatal("digest images must fail explicitly because the chart requires a tag")
	}
}

type failingCreateClient struct {
	client.Client
	failName string
	created  []string
	reads    int
}

func (c *failingCreateClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	c.reads++
	return c.Client.Get(ctx, key, obj, opts...)
}

func (c *failingCreateClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	c.reads++
	return c.Client.List(ctx, list, opts...)
}

func (c *failingCreateClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if obj.GetName() == c.failName {
		return errors.New("injected create failure")
	}
	if err := c.Client.Create(ctx, obj, opts...); err != nil {
		return err
	}
	c.created = append(c.created, obj.GetName())
	return nil
}

func TestStandaloneCreatesAllResourcesBeforeWaiting(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := infextv1.Install(scheme); err != nil {
		t.Fatal(err)
	}
	cli := &failingCreateClient{Client: fake.NewClientBuilder().WithScheme(scheme).Build()}
	resources := &caseResources{client: cli}
	objects, err := decodeCaseObjects([]byte(`apiVersion: apps/v1
kind: Deployment
metadata:
  name: router
spec:
  replicas: 3
---
apiVersion: inference.networking.k8s.io/v1
kind: InferencePool
metadata:
  name: router-pool
`), "test")
	if err != nil {
		t.Fatal(err)
	}
	if err := resources.create(ctx, objects); err != nil {
		t.Fatal(err)
	}
	if cli.reads != 0 || !reflect.DeepEqual(cli.created, []string{"router", "router-pool"}) {
		t.Fatalf("creation waited for the unready Deployment before creating dependencies: reads=%d created=%v", cli.reads, cli.created)
	}
}

func TestStandalonePartialCreationCleanup(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	foreign := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "foreign", Namespace: "test"}}
	for _, failure := range []string{"injected", "foreign"} {
		t.Run(failure, func(t *testing.T) {
			cli := &failingCreateClient{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(foreign.DeepCopy()).Build(), failName: "injected"}
			resources := &caseResources{client: cli}
			obj := func(name string) *unstructured.Unstructured {
				return &unstructured.Unstructured{Object: map[string]any{
					"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": name, "namespace": "test"},
				}}
			}
			if err := resources.create(ctx, []*unstructured.Unstructured{obj("owned"), obj(failure), obj("unattempted")}); err == nil {
				t.Fatal("expected create failure")
			}
			if !reflect.DeepEqual(cli.created, []string{"owned"}) || len(resources.created) != 1 {
				t.Fatalf("creation continued after failure: %v", cli.created)
			}
			if err := resources.delete(ctx); err != nil {
				t.Fatal(err)
			}
			deleted, err := resources.deleted(ctx)
			if err != nil || !deleted {
				t.Fatalf("created resources were not cleaned up: %t, %v", deleted, err)
			}
			if err := cli.Get(ctx, types.NamespacedName{Namespace: "test", Name: "foreign"}, &corev1.ConfigMap{}); err != nil {
				t.Fatalf("cleanup affected the preexisting object: %v", err)
			}
		})
	}
}

func TestStandaloneCleanupWaitsForPods(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	deployment := &appsv1.Deployment{
		TypeMeta:   metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"},
		ObjectMeta: metav1.ObjectMeta{Name: "router", Namespace: "test", UID: "deployment"},
		Spec:       appsv1.DeploymentSpec{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"router": "owned"}}},
	}
	oldPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "old-router", Namespace: "test", Labels: map[string]string{"router": "owned"},
		DeletionTimestamp: &metav1.Time{Time: metav1.Now().Time}, Finalizers: []string{"test.example/hold"},
	}}
	cli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(deployment, oldPod).Build()
	obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(deployment)
	if err != nil {
		t.Fatal(err)
	}
	resources := &caseResources{client: cli, created: []*unstructured.Unstructured{{Object: obj}}}
	if err := resources.delete(ctx); err != nil {
		t.Fatal(err)
	}
	if deleted, err := resources.deleted(ctx); deleted || err != nil {
		t.Fatalf("cleanup must wait for terminating router Pods: %t, %v", deleted, err)
	}
	if err := cli.Get(ctx, client.ObjectKeyFromObject(oldPod), oldPod); err != nil {
		t.Fatal(err)
	}
	oldPod.Finalizers = nil
	if err := cli.Update(ctx, oldPod); err != nil {
		t.Fatal(err)
	}
	if deleted, err := resources.deleted(ctx); !deleted || err != nil {
		t.Fatalf("cleanup did not finish after router Pod deletion: %t, %v", deleted, err)
	}
}

func readyTestRouterPod(name string, ready bool) corev1.Pod {
	condition := corev1.ConditionFalse
	if ready {
		condition = corev1.ConditionTrue
	}
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID(name)},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: condition}},
			ContainerStatuses: []corev1.ContainerStatus{
				{Name: "envoy-proxy", Ready: true, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}},
				{Name: "epp", Ready: ready, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}},
			},
		},
	}
}

func TestStandaloneReadyLeader(t *testing.T) {
	leader := readyTestRouterPod("leader", true)
	standby := readyTestRouterPod("standby", false)
	for _, tc := range []struct {
		name     string
		pods     []corev1.Pod
		replicas int
		wantErr  bool
	}{
		{"single", []corev1.Pod{leader}, 1, false},
		{"three with standbys", []corev1.Pod{leader, standby, standby}, 3, false},
		{"missing standby", []corev1.Pod{leader, standby}, 3, true},
		{"two leaders", []corev1.Pod{leader, leader, standby}, 3, true},
		{"no leader", []corev1.Pod{standby, standby, standby}, 3, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := standaloneReadyLeader(tc.pods, tc.replicas)
			if (err != nil) != tc.wantErr {
				t.Fatalf("readiness error = %v, wantErr = %t", err, tc.wantErr)
			}
		})
	}
	leader.DeletionTimestamp = &metav1.Time{Time: metav1.Now().Time}
	if _, err := standaloneReadyLeader([]corev1.Pod{leader}, 1); err == nil {
		t.Fatal("terminating leader must not be Ready")
	}
	leader = readyTestRouterPod("leader", true)
	leader.Status.ContainerStatuses[0].Ready = false
	if _, err := standaloneReadyLeader([]corev1.Pod{leader}, 1); err == nil {
		t.Fatal("unready Envoy must not be Ready")
	}
}

func TestStandalonePortForwardRecovery(t *testing.T) {
	ctx := context.Background()
	var events []string
	var exited chan struct{}
	forward := &routerPortForward{start: func(_ context.Context, pod *corev1.Pod) (*forwardProcess, error) {
		events = append(events, "start "+pod.Name)
		exited = make(chan struct{})
		done := exited
		return &forwardProcess{done: done, stop: func() {
			events = append(events, "stop "+pod.Name)
			select {
			case <-done:
			default:
				close(done)
			}
		}}, nil
	}}
	leader := readyTestRouterPod("leader", true)
	standby := readyTestRouterPod("standby", false)
	for _, pods := range [][]corev1.Pod{{standby}, {standby, leader}, {leader, standby}} {
		if err := forward.reconcile(ctx, pods); err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(events, []string{"start leader"}) {
		t.Fatalf("standby or duplicate forwarding: %v", events)
	}
	close(exited)
	if err := forward.reconcile(ctx, []corev1.Pod{leader}); err != nil {
		t.Fatal(err)
	}
	leader.DeletionTimestamp = &metav1.Time{Time: metav1.Now().Time}
	newLeader := readyTestRouterPod("new-leader", true)
	if err := forward.reconcile(ctx, []corev1.Pod{leader, standby, newLeader}); err != nil {
		t.Fatal(err)
	}
	forward.close()
	want := []string{"start leader", "stop leader", "start leader", "stop leader", "start new-leader", "stop new-leader"}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("forward lifecycle = %v, want %v", events, want)
	}
	select {
	case <-exited:
	default:
		t.Fatal("cleanup returned before the process exited")
	}
	forward.start = func(context.Context, *corev1.Pod) (*forwardProcess, error) {
		return nil, errors.New("start failed")
	}
	if err := forward.reconcile(ctx, []corev1.Pod{newLeader}); err == nil {
		t.Fatal("expected start failure")
	}
}

func TestStandalonePortForwardWaitsForExit(t *testing.T) {
	exited := make(chan struct{})
	stopped := make(chan struct{})
	closed := make(chan struct{})
	forward := &routerPortForward{process: &forwardProcess{done: exited, stop: func() { close(stopped) }}}
	go func() {
		forward.close()
		close(closed)
	}()
	<-stopped
	select {
	case <-closed:
		t.Fatal("port-forward cleanup returned before process exit")
	default:
	}
	close(exited)
	<-closed
}
