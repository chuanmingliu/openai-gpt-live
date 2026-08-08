// Command validate performs fail-closed, document-structured validation of
// rendered deployment resources. It also provides named single-invariant
// mutations used only by the repository's negative policy tests.
package main

import (
	_ "crypto/sha256"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"

	liveconfig "github.com/chuanmingliu/openai-gpt-live/internal/config"
	"github.com/distribution/reference"
	"gopkg.in/yaml.v3"
)

var (
	hashPattern   = regexp.MustCompile(`^v1:[0-9a-f]{64}$`)
	digestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
	hostPattern   = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?(\.[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?)+$`)
	namePattern   = regexp.MustCompile(`^[a-z0-9]([-a-z0-9.]{0,251}[a-z0-9])?$`)
)

type document = map[string]any

func main() {
	overlay := flag.String("overlay", "", "local or production")
	readiness := flag.String("readiness", "deploy", "deploy, fixture, or local")
	checkInputs := flag.Bool("check-inputs", false, "validate production render environment")
	mutation := flag.String("mutate", "", "emit a named negative-test mutation")
	split := flag.String("split", "", "after validation, emit job or workloads documents")
	flag.Parse()
	if *checkInputs {
		if err := validateProductionInputs(*readiness); err != nil {
			fatal(err)
		}
		return
	}
	if *overlay != "local" && *overlay != "production" {
		fatal(errors.New("--overlay must be local or production"))
	}
	if flag.NArg() != 1 {
		fatal(errors.New("exactly one rendered YAML path is required"))
	}
	docs, err := readDocuments(flag.Arg(0))
	if err != nil {
		fatal(err)
	}
	if *mutation != "" {
		docs, err = mutateDocuments(docs, *mutation)
		if err != nil {
			fatal(err)
		}
		encoder := yaml.NewEncoder(os.Stdout)
		encoder.SetIndent(2)
		defer encoder.Close()
		for _, doc := range docs {
			if err := encoder.Encode(doc); err != nil {
				fatal(err)
			}
		}
		return
	}
	if err := validate(docs, *overlay, *readiness); err != nil {
		fatal(err)
	}
	if *split != "" {
		if *split != "job" && *split != "workloads" {
			fatal(errors.New("--split must be job or workloads"))
		}
		encoder := yaml.NewEncoder(os.Stdout)
		encoder.SetIndent(2)
		defer encoder.Close()
		for _, doc := range docs {
			isJob := stringAt(doc, "kind") == "Job"
			if (*split == "job") != isJob {
				continue
			}
			if err := encoder.Encode(doc); err != nil {
				fatal(err)
			}
		}
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "deployment policy failed:", err)
	os.Exit(1)
}

func readDocuments(path string) ([]document, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	decoder := yaml.NewDecoder(file)
	var docs []document
	for {
		var node yaml.Node
		err := decoder.Decode(&node)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("decode YAML: %w", err)
		}
		if err := validateYAMLNode(&node); err != nil {
			return nil, err
		}
		var doc document
		if err := node.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode YAML document: %w", err)
		}
		if len(doc) != 0 {
			docs = append(docs, doc)
		}
	}
	return docs, nil
}

func validateYAMLNode(node *yaml.Node) error {
	if node == nil {
		return nil
	}
	if node.Kind == yaml.AliasNode {
		return errors.New("ambiguous YAML aliases are forbidden")
	}
	if node.Kind == yaml.MappingNode {
		seen := map[string]bool{}
		for index := 0; index < len(node.Content); index += 2 {
			key := node.Content[index]
			if key.Kind != yaml.ScalarNode || key.Value == "<<" {
				return errors.New("ambiguous YAML mapping key is forbidden")
			}
			identity := key.Tag + "\x00" + key.Value
			if seen[identity] {
				return fmt.Errorf("duplicate YAML mapping key %q", key.Value)
			}
			seen[identity] = true
		}
	}
	for _, child := range node.Content {
		if err := validateYAMLNode(child); err != nil {
			return err
		}
	}
	return nil
}

func validate(docs []document, overlay, readiness string) error {
	if overlay == "local" && readiness != "local" {
		return errors.New("local render must use local readiness")
	}
	if overlay == "production" && readiness != "deploy" && readiness != "fixture" {
		return errors.New("production readiness must be deploy or fixture")
	}
	if err := validateObjectSet(docs, overlay); err != nil {
		return err
	}
	if find(docs, "Namespace", "gpt-live") == nil {
		return errors.New("explicit gpt-live Namespace is missing")
	}
	if findKind(docs, "Secret") != nil {
		return errors.New("render contains a Secret resource")
	}
	for _, doc := range docs {
		if stringAt(doc, "kind") != "Namespace" && stringAt(mapAt(doc, "metadata"), "namespace") != "gpt-live" {
			return fmt.Errorf("%s/%s must be explicitly namespaced to gpt-live", stringAt(doc, "kind"), stringAt(mapAt(doc, "metadata"), "name"))
		}
		if containsToken(doc) {
			return fmt.Errorf("%s/%s contains an unresolved deployment token", stringAt(doc, "kind"), stringAt(mapAt(doc, "metadata"), "name"))
		}
		if overlay == "production" && containsExactString(doc, "LIVE_STATIC_BEARER_TOKEN") {
			return fmt.Errorf("%s/%s exposes forbidden production static bearer configuration", stringAt(doc, "kind"), stringAt(mapAt(doc, "metadata"), "name"))
		}
	}
	metadata := find(docs, "ConfigMap", "live-deploy-metadata")
	if metadata == nil {
		return errors.New("live-deploy-metadata ConfigMap is missing")
	}
	data := mapAt(metadata, "data")
	if !exactKeys(data, "config-hash", "render-readiness", "production-host", "tls-secret", "egress-proxy-cidr", "trusted-proxy-cidr", "https-proxy") {
		return errors.New("deployment metadata has missing or extra graph inputs")
	}
	if stringAt(data, "render-readiness") != readiness {
		return fmt.Errorf("render readiness %q does not match %q", stringAt(data, "render-readiness"), readiness)
	}
	liveConfig := find(docs, "ConfigMap", "live-config")
	if liveConfig == nil || !exactKeys(mapAt(liveConfig, "data"), "config.yaml") || stringAt(mapAt(liveConfig, "data"), "config.yaml") == "" {
		return errors.New("live-config must contain exactly config.yaml")
	}
	computedHash, err := hashRenderedConfig(stringAt(mapAt(liveConfig, "data"), "config.yaml"))
	if err != nil {
		return fmt.Errorf("live-config content is invalid: %w", err)
	}
	if computedHash != stringAt(data, "config-hash") {
		return errors.New("live-config content/hash wiring is invalid")
	}
	if overlay == "local" && (stringAt(data, "production-host") != "" || stringAt(data, "tls-secret") != "" || stringAt(data, "egress-proxy-cidr") != "" || stringAt(data, "trusted-proxy-cidr") != "" || stringAt(data, "https-proxy") != "") {
		return errors.New("local deployment metadata must not retain production wiring")
	}

	workloads := []struct {
		kind, name string
		grace      int64
		probes     bool
	}{
		{"Deployment", "live-gateway", 45, true},
		{"Deployment", "live-worker", 135, true},
		{"Job", "live-migrate", 30, false},
	}
	images := map[string]string{}
	var sharedHash string
	for _, expected := range workloads {
		doc := find(docs, expected.kind, expected.name)
		if doc == nil {
			return fmt.Errorf("%s/%s is missing", expected.kind, expected.name)
		}
		pod, podMeta := podSpec(doc)
		if err := validateWorkloadGraph(doc, expected.name, pod, podMeta); err != nil {
			return err
		}
		if err := validateWorkloadLabels(expected.name, podMeta); err != nil {
			return err
		}
		if err := validatePod(expected.name, pod, expected.grace); err != nil {
			return err
		}
		containers := sliceAt(pod, "containers")
		if len(containers) != 1 {
			return fmt.Errorf("%s must have exactly one container", expected.name)
		}
		if len(sliceAt(pod, "initContainers")) != 0 {
			return fmt.Errorf("%s must not expose secrets through init containers", expected.name)
		}
		for _, raw := range containers {
			container, ok := raw.(map[string]any)
			if !ok {
				return fmt.Errorf("%s has an invalid container", expected.name)
			}
			if err := validateContainer(expected.name, container, overlay == "production"); err != nil {
				return err
			}
		}
		container := containers[0].(map[string]any)
		images[expected.name] = stringAt(container, "image")
		if err := validateEnvironment(expected.name, container, overlay); err != nil {
			return err
		}
		if expected.probes {
			if err := validateServiceContainer(expected.name, podMeta, container); err != nil {
				return err
			}
			hash := stringAt(mapAt(podMeta, "annotations"), "live.openai.com/config-hash")
			if !hashPattern.MatchString(hash) {
				return fmt.Errorf("%s config-hash annotation is malformed", expected.name)
			}
			if sharedHash == "" {
				sharedHash = hash
			} else if hash != sharedHash {
				return errors.New("service pod config hashes differ")
			}
			if envValue(container, "LIVE_EXPECTED_CONFIG_HASH") != hash {
				return fmt.Errorf("%s expected config hash differs from annotation", expected.name)
			}
		}
	}
	if stringAt(data, "config-hash") != sharedHash {
		return errors.New("metadata config hash differs from workload hash")
	}
	if err := validateSupportingResources(docs, overlay); err != nil {
		return err
	}

	if err := validateNetworkPolicies(docs, overlay, stringAt(data, "egress-proxy-cidr")); err != nil {
		return err
	}
	if overlay == "local" {
		if findKind(docs, "Ingress") != nil {
			return errors.New("local render must not contain Ingress")
		}
		for _, name := range []string{"live-gateway", "live-worker"} {
			container := firstContainer(find(docs, "Deployment", name))
			if !secretEnv(container, "LIVE_STATIC_BEARER_TOKEN", "live-secrets", "LIVE_STATIC_BEARER_TOKEN") {
				return fmt.Errorf("%s static bearer must come from live-secrets/LIVE_STATIC_BEARER_TOKEN", name)
			}
		}
		return nil
	}
	if err := validateProduction(docs, data, images, readiness); err != nil {
		return err
	}
	return nil
}

func hashRenderedConfig(raw string) (string, error) {
	file, err := os.CreateTemp("", "gpt-live-rendered-config-*.yaml")
	if err != nil {
		return "", err
	}
	path := file.Name()
	defer os.Remove(path)
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return "", err
	}
	if _, err := io.WriteString(file, raw); err != nil {
		_ = file.Close()
		return "", err
	}
	if err := file.Close(); err != nil {
		return "", err
	}
	parsed, err := liveconfig.LoadUnresolved(path)
	if err != nil {
		return "", err
	}
	return liveconfig.Hash(parsed)
}

func validateObjectSet(docs []document, overlay string) error {
	want := []string{
		"v1|Namespace||gpt-live",
		"v1|ConfigMap|gpt-live|live-config",
		"v1|ConfigMap|gpt-live|live-deploy-metadata",
		"v1|Service|gpt-live|live-gateway",
		"apps/v1|Deployment|gpt-live|live-gateway",
		"apps/v1|Deployment|gpt-live|live-worker",
		"policy/v1|PodDisruptionBudget|gpt-live|live-gateway",
		"policy/v1|PodDisruptionBudget|gpt-live|live-worker",
		"autoscaling/v2|HorizontalPodAutoscaler|gpt-live|live-gateway",
		"autoscaling/v2|HorizontalPodAutoscaler|gpt-live|live-worker",
		"batch/v1|Job|gpt-live|live-migrate",
		"networking.k8s.io/v1|NetworkPolicy|gpt-live|application-egress",
		"networking.k8s.io/v1|NetworkPolicy|gpt-live|default-deny",
		"networking.k8s.io/v1|NetworkPolicy|gpt-live|gateway-ingress",
		"networking.k8s.io/v1|NetworkPolicy|gpt-live|migrate-egress",
	}
	if overlay == "production" {
		want = append(want, "networking.k8s.io/v1|Ingress|gpt-live|live-gateway")
	}
	sort.Strings(want)
	seen := map[string]bool{}
	got := make([]string, 0, len(docs))
	for _, doc := range docs {
		metadata := mapAt(doc, "metadata")
		identity := strings.Join([]string{stringAt(doc, "apiVersion"), stringAt(doc, "kind"), stringAt(metadata, "namespace"), stringAt(metadata, "name")}, "|")
		if seen[identity] {
			return fmt.Errorf("duplicate Kubernetes object identity %s", identity)
		}
		seen[identity] = true
		got = append(got, identity)
	}
	sort.Strings(got)
	if !sameStrings(got, want) {
		return fmt.Errorf("rendered object identity set is %v, want exactly %v", got, want)
	}
	return nil
}

func validateWorkloadLabels(workload string, podMeta map[string]any) error {
	labels := mapAt(podMeta, "labels")
	want := map[string]string{"app.kubernetes.io/name": workload, "app.kubernetes.io/part-of": "gpt-live"}
	if workload == "live-gateway" {
		want["app.kubernetes.io/component"] = "gateway"
	}
	if workload == "live-worker" {
		want["app.kubernetes.io/component"] = "worker"
	}
	if len(labels) != len(want) {
		return fmt.Errorf("%s pod labels do not exactly match policy targets", workload)
	}
	for key, value := range want {
		if stringAt(labels, key) != value {
			return fmt.Errorf("%s pod label %s does not match policy target", workload, key)
		}
	}
	return nil
}

func validateWorkloadGraph(doc document, workload string, pod, podMeta map[string]any) error {
	component := ""
	if workload == "live-gateway" {
		component = "gateway"
	}
	if workload == "live-worker" {
		component = "worker"
	}
	wantLabels := map[string]string{"app.kubernetes.io/name": workload, "app.kubernetes.io/part-of": "gpt-live"}
	if component != "" {
		wantLabels["app.kubernetes.io/component"] = component
	}
	if !exactStringMap(mapAt(mapAt(doc, "metadata"), "labels"), wantLabels) || !exactStringMap(mapAt(podMeta, "labels"), wantLabels) {
		return fmt.Errorf("%s workload and pod-template labels must be exact and consistent", workload)
	}
	if workload != "live-migrate" {
		if !exactLabels(mapAt(mapAt(doc, "spec"), "selector"), "app.kubernetes.io/name", workload) {
			return fmt.Errorf("%s Deployment selector does not match its pod template", workload)
		}
	}
	container := firstContainer(doc)
	if stringAt(container, "name") != workload {
		return fmt.Errorf("%s container name is inconsistent", workload)
	}
	if workload == "live-migrate" {
		if !sameStrings(orderedStringSlice(container, "args"), []string{"-direction", "up"}) || len(sliceAt(pod, "volumes")) != 0 || len(sliceAt(container, "volumeMounts")) != 0 {
			return errors.New("migration command/config wiring is invalid")
		}
		return nil
	}
	if !sameStrings(orderedStringSlice(container, "args"), []string{"-config", "/etc/live/config.yaml"}) {
		return fmt.Errorf("%s config argument is invalid", workload)
	}
	volumes := sliceAt(pod, "volumes")
	mounts := sliceAt(container, "volumeMounts")
	if len(volumes) != 1 || len(mounts) != 1 {
		return fmt.Errorf("%s must wire exactly one config volume and mount", workload)
	}
	volume := volumes[0].(map[string]any)
	configMap := mapAt(volume, "configMap")
	items := sliceAt(configMap, "items")
	if !exactKeys(volume, "name", "configMap") || stringAt(volume, "name") != "config" || !exactKeys(configMap, "name", "items") || stringAt(configMap, "name") != "live-config" || len(items) != 1 {
		return fmt.Errorf("%s config volume reference is invalid", workload)
	}
	item := items[0].(map[string]any)
	if !exactKeys(item, "key", "path") || stringAt(item, "key") != "config.yaml" || stringAt(item, "path") != "config.yaml" {
		return fmt.Errorf("%s config projection is invalid", workload)
	}
	mount := mounts[0].(map[string]any)
	if !exactKeys(mount, "name", "mountPath", "subPath", "readOnly") || stringAt(mount, "name") != "config" || stringAt(mount, "mountPath") != "/etc/live/config.yaml" || stringAt(mount, "subPath") != "config.yaml" || !boolAt(mount, "readOnly", false) {
		return fmt.Errorf("%s config mount/subPath is invalid", workload)
	}
	ports := sliceAt(container, "ports")
	if workload == "live-gateway" {
		if len(ports) != 1 {
			return errors.New("gateway must expose exactly one container port")
		}
		port := ports[0].(map[string]any)
		if !exactKeys(port, "name", "protocol", "containerPort") || stringAt(port, "name") != "realtime" || stringAt(port, "protocol") != "TCP" || intAt(port, "containerPort") != 8080 {
			return errors.New("gateway realtime container port is invalid")
		}
	} else if len(ports) != 0 {
		return errors.New("worker must not expose a service port")
	}
	return nil
}

func exactStringMap(got map[string]any, want map[string]string) bool {
	if len(got) != len(want) {
		return false
	}
	for key, value := range want {
		if stringAt(got, key) != value {
			return false
		}
	}
	return true
}

func validateEnvironment(workload string, container map[string]any, overlay string) error {
	expectedSecrets := map[string][]string{
		"live-gateway": {"DATABASE_URL", "S2S_API_KEY", "DEEPSEEK_API_KEY", "LIVE_ADMIN_BEARER_TOKEN", "LIVE_PRIVACY_HMAC_KEY", "LIVE_RETENTION_HMAC_KEY", "LIVE_TOOL_ENCRYPTION_KEY"},
		"live-worker":  {"DATABASE_URL", "DEEPSEEK_API_KEY", "LIVE_ADMIN_BEARER_TOKEN", "LIVE_PRIVACY_HMAC_KEY", "LIVE_RETENTION_HMAC_KEY", "LIVE_TOOL_ENCRYPTION_KEY"},
		"live-migrate": {"DATABASE_URL"},
	}
	secrets := expectedSecrets[workload]
	literals := []string{}
	if workload != "live-migrate" {
		literals = append(literals, "LIVE_EXPECTED_CONFIG_HASH")
		if overlay == "local" {
			secrets = append(secrets, "LIVE_STATIC_BEARER_TOKEN")
		} else {
			literals = append(literals, "HTTPS_PROXY")
		}
	}
	wantSecret := make(map[string]bool, len(secrets))
	wantLiteral := make(map[string]bool, len(literals))
	for _, name := range secrets {
		wantSecret[name] = true
	}
	for _, name := range literals {
		wantLiteral[name] = true
	}
	seen := map[string]bool{}
	for _, raw := range sliceAt(container, "env") {
		env, ok := raw.(map[string]any)
		if !ok {
			return fmt.Errorf("%s has an invalid env entry", workload)
		}
		name := stringAt(env, "name")
		if name == "" || seen[name] {
			return fmt.Errorf("%s has duplicate or unnamed env %q", workload, name)
		}
		seen[name] = true
		if wantSecret[name] {
			ref := mapAt(mapAt(env, "valueFrom"), "secretKeyRef")
			if len(env) != 2 || len(mapAt(env, "valueFrom")) != 1 || len(ref) != 2 || stringAt(ref, "name") != "live-secrets" || stringAt(ref, "key") != name {
				return fmt.Errorf("%s must source %s only from live-secrets/%s", workload, name, name)
			}
		} else if wantLiteral[name] {
			if len(env) != 2 || stringAt(env, "value") == "" {
				return fmt.Errorf("%s literal env %s is malformed", workload, name)
			}
		} else {
			return fmt.Errorf("%s exposes unexpected env %s", workload, name)
		}
	}
	for _, name := range append(append([]string{}, secrets...), literals...) {
		if !seen[name] {
			return fmt.Errorf("%s is missing required env %s", workload, name)
		}
	}
	return nil
}

func validateSupportingResources(docs []document, overlay string) error {
	service := find(docs, "Service", "live-gateway")
	serviceSpec := mapAt(service, "spec")
	ports := sliceAt(serviceSpec, "ports")
	if !exactStringMap(mapAt(mapAt(service, "metadata"), "labels"), map[string]string{"app.kubernetes.io/name": "live-gateway"}) || !exactKeys(serviceSpec, "type", "selector", "ports") || stringAt(serviceSpec, "type") != "ClusterIP" || !exactStringMap(mapAt(serviceSpec, "selector"), map[string]string{"app.kubernetes.io/name": "live-gateway"}) || len(ports) != 1 {
		return errors.New("gateway Service selector/type/ports wiring is invalid")
	}
	servicePort := ports[0].(map[string]any)
	if !exactKeys(servicePort, "name", "protocol", "port", "targetPort") || stringAt(servicePort, "name") != "realtime" || stringAt(servicePort, "protocol") != "TCP" || intAt(servicePort, "port") != 80 || stringAt(servicePort, "targetPort") != "realtime" {
		return errors.New("gateway Service port must map TCP/80 to named realtime")
	}
	for _, name := range []string{"live-gateway", "live-worker"} {
		pdb := find(docs, "PodDisruptionBudget", name)
		pdbSpec := mapAt(pdb, "spec")
		if !exactKeys(pdbSpec, "minAvailable", "selector") || intAt(pdbSpec, "minAvailable") != 1 || !exactLabels(mapAt(pdbSpec, "selector"), "app.kubernetes.io/name", name) {
			return fmt.Errorf("%s PDB selector/minAvailable wiring is invalid", name)
		}
		hpa := find(docs, "HorizontalPodAutoscaler", name)
		if err := validateHPA(hpa, name, overlay); err != nil {
			return err
		}
	}
	return nil
}

func validateHPA(hpa document, workload, overlay string) error {
	spec := mapAt(hpa, "spec")
	if !exactKeys(spec, "scaleTargetRef", "minReplicas", "maxReplicas", "behavior", "metrics") {
		return fmt.Errorf("%s HPA spec has missing or extra wiring", workload)
	}
	target := mapAt(spec, "scaleTargetRef")
	if !exactStringMap(target, map[string]string{"apiVersion": "apps/v1", "kind": "Deployment", "name": workload}) {
		return fmt.Errorf("%s HPA target is invalid", workload)
	}
	minReplicas, maxReplicas := int64(2), int64(20)
	if workload == "live-worker" {
		maxReplicas = 50
	}
	if overlay == "local" {
		minReplicas, maxReplicas = 1, 4
	}
	if intAt(spec, "minReplicas") != minReplicas || intAt(spec, "maxReplicas") != maxReplicas {
		return fmt.Errorf("%s HPA replica bounds are invalid", workload)
	}
	behavior := mapAt(spec, "behavior")
	if !exactKeys(behavior, "scaleDown") || !exactKeys(mapAt(behavior, "scaleDown"), "stabilizationWindowSeconds") || intAt(mapAt(behavior, "scaleDown"), "stabilizationWindowSeconds") != 300 {
		return fmt.Errorf("%s HPA behavior is invalid", workload)
	}
	want := []string{"Resource:cpu:Utilization:"}
	if workload == "live-gateway" {
		want[0] += "65"
	} else {
		want[0] += "70"
	}
	if overlay == "production" && workload == "live-gateway" {
		want = append(want, "Pods:live_sessions_active:AverageValue:50")
	}
	if overlay == "production" && workload == "live-worker" {
		want = append(want, "External:live_handoff_queue_oldest_age_seconds:AverageValue:5")
	}
	var got []string
	for _, raw := range sliceAt(spec, "metrics") {
		signature, ok := metricSignature(raw)
		if !ok {
			return fmt.Errorf("%s HPA contains malformed metric wiring", workload)
		}
		got = append(got, signature)
	}
	sort.Strings(got)
	sort.Strings(want)
	if !sameStrings(got, want) {
		return fmt.Errorf("%s HPA metrics are %v, want exactly %v", workload, got, want)
	}
	return nil
}

func metricSignature(raw any) (string, bool) {
	metric, ok := raw.(map[string]any)
	if !ok {
		return "", false
	}
	typeName := stringAt(metric, "type")
	class := strings.ToLower(typeName)
	if !exactKeys(metric, "type", class) {
		return "", false
	}
	body := mapAt(metric, class)
	if typeName == "Resource" {
		if !exactKeys(body, "name", "target") {
			return "", false
		}
		target := mapAt(body, "target")
		if !exactKeys(target, "type", "averageUtilization") {
			return "", false
		}
		return typeName + ":" + stringAt(body, "name") + ":" + stringAt(target, "type") + ":" + strconv.FormatInt(intAt(target, "averageUtilization"), 10), true
	}
	if typeName != "Pods" && typeName != "External" {
		return "", false
	}
	if !exactKeys(body, "metric", "target") || !exactKeys(mapAt(body, "metric"), "name") {
		return "", false
	}
	target := mapAt(body, "target")
	if !exactKeys(target, "type", "averageValue") {
		return "", false
	}
	return typeName + ":" + stringAt(mapAt(body, "metric"), "name") + ":" + stringAt(target, "type") + ":" + stringAt(target, "averageValue"), true
}

func metricNames(hpa document) []string {
	var names []string
	for _, raw := range sliceAt(mapAt(hpa, "spec"), "metrics") {
		metric := raw.(map[string]any)
		for _, class := range []string{"pods", "external", "object"} {
			if name := stringAt(mapAt(mapAt(metric, class), "metric"), "name"); name != "" {
				names = append(names, name)
			}
		}
	}
	return names
}

func containsString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func containsToken(value any) bool {
	switch current := value.(type) {
	case string:
		upper := strings.ToUpper(current)
		return strings.Contains(upper, "LIVE_PRODUCTION_") || strings.Contains(upper, "PRODUCTION_CONFIG_HASH") || strings.Contains(upper, "PRODUCTION_RENDER_READINESS") || strings.Contains(upper, "CONFIG_HASH_REPLACED") || strings.Contains(upper, "REPLACE_ME") || strings.Contains(upper, "CHANGEME") || strings.Contains(upper, "PLACEHOLDER")
	case map[string]any:
		for _, child := range current {
			if containsToken(child) {
				return true
			}
		}
	case []any:
		for _, child := range current {
			if containsToken(child) {
				return true
			}
		}
	}
	return false
}

func containsExactString(value any, wanted string) bool {
	switch current := value.(type) {
	case string:
		return current == wanted
	case map[string]any:
		for _, child := range current {
			if containsExactString(child, wanted) {
				return true
			}
		}
	case []any:
		for _, child := range current {
			if containsExactString(child, wanted) {
				return true
			}
		}
	}
	return false
}

func validatePod(name string, pod map[string]any, grace int64) error {
	if boolAt(pod, "automountServiceAccountToken", true) {
		return fmt.Errorf("%s must disable service-account token automount", name)
	}
	if intAt(pod, "terminationGracePeriodSeconds") != grace {
		return fmt.Errorf("%s termination grace must be %d", name, grace)
	}
	security := mapAt(pod, "securityContext")
	if !boolAt(security, "runAsNonRoot", false) || intAt(security, "runAsUser") != 65532 || intAt(security, "runAsGroup") != 65532 {
		return fmt.Errorf("%s pod must run as numeric non-root 65532:65532", name)
	}
	if stringAt(mapAt(security, "seccompProfile"), "type") != "RuntimeDefault" {
		return fmt.Errorf("%s pod must use RuntimeDefault seccomp", name)
	}
	return nil
}

func validateContainer(workload string, container map[string]any, production bool) error {
	name := stringAt(container, "name")
	security := mapAt(container, "securityContext")
	if !boolAt(security, "readOnlyRootFilesystem", false) {
		return fmt.Errorf("%s/%s root filesystem must be read-only", workload, name)
	}
	if boolAt(security, "allowPrivilegeEscalation", true) {
		return fmt.Errorf("%s/%s must disable privilege escalation", workload, name)
	}
	drop := stringSlice(mapAt(security, "capabilities"), "drop")
	if len(drop) != 1 || drop[0] != "ALL" {
		return fmt.Errorf("%s/%s must drop exactly ALL capabilities", workload, name)
	}
	resources := mapAt(container, "resources")
	for _, class := range []string{"requests", "limits"} {
		values := mapAt(resources, class)
		if stringAt(values, "cpu") == "" || stringAt(values, "memory") == "" {
			return fmt.Errorf("%s/%s must set CPU and memory %s", workload, name, class)
		}
	}
	if production {
		_, digest, ok := parseImage(stringAt(container, "image"))
		if !ok || obviousDigest(digest) {
			return fmt.Errorf("%s/%s must use a non-obvious immutable image digest", workload, name)
		}
	}
	return nil
}

func validateServiceContainer(workload string, podMeta, container map[string]any) error {
	probes := []struct {
		name, path      string
		period, failure int64
	}{
		{"readinessProbe", "/health/ready", 3, 3},
		{"livenessProbe", "/health/live", 10, 3},
		{"startupProbe", "/health/live", 2, 30},
	}
	for _, expected := range probes {
		probe := mapAt(container, expected.name)
		command := orderedStringSlice(mapAt(probe, "exec"), "command")
		want := []string{"/bin/busybox", "wget", "-q", "-O", "/dev/null", "http://127.0.0.1:9090" + expected.path}
		if len(probe) != 4 || !sameStrings(command, want) || intAt(probe, "periodSeconds") != expected.period || intAt(probe, "timeoutSeconds") != 2 || intAt(probe, "failureThreshold") != expected.failure {
			return fmt.Errorf("%s %s listener/path/port/scheme/timing contract is invalid", workload, expected.name)
		}
	}
	preStop := mapAt(mapAt(container, "lifecycle"), "preStop")
	budget := "35s"
	if workload == "live-worker" {
		budget = "125s"
	}
	wantPreStop := []string{"/usr/local/bin/live-prestop", "--health-port", "9090", "--budget", budget}
	if len(preStop) != 1 || !sameStrings(orderedStringSlice(mapAt(preStop, "exec"), "command"), wantPreStop) {
		return fmt.Errorf("%s must use the authenticated bounded drain-and-readiness preStop contract", workload)
	}
	if len(mapAt(podMeta, "annotations")) == 0 {
		return fmt.Errorf("%s pod annotations are missing", workload)
	}
	return nil
}

func validateDefaultDeny(docs []document) error {
	policy := find(docs, "NetworkPolicy", "default-deny")
	if policy == nil {
		return errors.New("default-deny NetworkPolicy is missing")
	}
	spec := mapAt(policy, "spec")
	if !exactKeys(spec, "podSelector", "policyTypes") || len(mapAt(spec, "podSelector")) != 0 || !sameStrings(stringSlice(spec, "policyTypes"), []string{"Egress", "Ingress"}) {
		return errors.New("default-deny must select all pods and deny ingress/egress")
	}
	if len(sliceAt(spec, "ingress")) != 0 || len(sliceAt(spec, "egress")) != 0 {
		return errors.New("default-deny must not contain allow rules")
	}
	return nil
}

func validateNetworkPolicies(docs []document, overlay, proxyCIDR string) error {
	wantNames := []string{"application-egress", "default-deny", "gateway-ingress", "migrate-egress"}
	var gotNames []string
	for _, doc := range docs {
		if stringAt(doc, "kind") == "NetworkPolicy" {
			gotNames = append(gotNames, stringAt(mapAt(doc, "metadata"), "name"))
		}
	}
	sort.Strings(gotNames)
	if !sameStrings(gotNames, wantNames) {
		return fmt.Errorf("NetworkPolicy set is %v, want exactly %v", gotNames, wantNames)
	}
	if err := validateDefaultDeny(docs); err != nil {
		return err
	}
	if err := validateMigrationEgress(docs); err != nil {
		return err
	}
	if err := validateGatewayIngress(docs, overlay); err != nil {
		return err
	}
	return validateApplicationEgress(docs, overlay, proxyCIDR)
}

func validateGatewayIngress(docs []document, overlay string) error {
	policy := find(docs, "NetworkPolicy", "gateway-ingress")
	spec := mapAt(policy, "spec")
	if !exactKeys(spec, "podSelector", "policyTypes", "ingress") || !exactLabels(mapAt(spec, "podSelector"), "app.kubernetes.io/name", "live-gateway") || !sameStrings(stringSlice(spec, "policyTypes"), []string{"Ingress"}) || len(sliceAt(spec, "egress")) != 0 {
		return errors.New("gateway-ingress selector or policyTypes is invalid")
	}
	rules := sliceAt(spec, "ingress")
	if len(rules) != 1 {
		return errors.New("gateway-ingress must contain exactly one rule")
	}
	rule, ok := rules[0].(map[string]any)
	if !ok || !exactKeys(rule, "from", "ports") || portSet(rule) != "TCP/8080" {
		return errors.New("gateway-ingress port must be exactly TCP/8080")
	}
	peers := sliceAt(rule, "from")
	if len(peers) != 1 {
		return errors.New("gateway-ingress must have exactly one source")
	}
	peer := peers[0].(map[string]any)
	namespace, podKey, podValue := "ingress-nginx", "app.kubernetes.io/name", "ingress-nginx"
	if overlay == "local" {
		namespace, podKey, podValue = "gpt-live", "app.kubernetes.io/part-of", "gpt-live"
	}
	if !exactKeys(peer, "namespaceSelector", "podSelector") || !exactLabels(mapAt(peer, "namespaceSelector"), "kubernetes.io/metadata.name", namespace) || !exactLabels(mapAt(peer, "podSelector"), podKey, podValue) {
		return errors.New("gateway-ingress source selectors are invalid")
	}
	return nil
}

func validateApplicationEgress(docs []document, overlay, proxyCIDR string) error {
	policy := find(docs, "NetworkPolicy", "application-egress")
	spec := mapAt(policy, "spec")
	selector := mapAt(spec, "podSelector")
	expressions := sliceAt(selector, "matchExpressions")
	if !exactKeys(spec, "podSelector", "policyTypes", "egress") || !exactKeys(selector, "matchExpressions") || len(expressions) != 1 || !exactExpression(expressions[0], "app.kubernetes.io/component", "In", []string{"gateway", "worker"}) || !sameStrings(stringSlice(spec, "policyTypes"), []string{"Egress"}) || len(sliceAt(spec, "ingress")) != 0 {
		return errors.New("application-egress must select exactly gateway and worker pods for Egress")
	}
	want := []string{
		"ns:kube-system|TCP/53,UDP/53",
		"nspod:gpt-live:postgres|TCP/5432",
		"nspod:identity:oidc|TCP/443",
		"nspod:observability:otel-collector|TCP/4318",
	}
	if overlay == "local" {
		want = append(want, "nspod:gpt-live:speech-to-speech|TCP/8000")
	} else {
		want = append(want, "nspod:platform:egress-proxy|TCP/3128", "ip:"+proxyCIDR+"|TCP/3128")
	}
	var got []string
	for _, raw := range sliceAt(spec, "egress") {
		rule, ok := raw.(map[string]any)
		if !ok || !exactKeys(rule, "to", "ports") {
			return errors.New("application-egress contains a malformed or permissive rule")
		}
		peers := sliceAt(rule, "to")
		if len(peers) != 1 {
			return errors.New("application-egress rules must have exactly one destination")
		}
		peer, ok := peers[0].(map[string]any)
		if !ok {
			return errors.New("application-egress destination is malformed")
		}
		signature, ok := peerSignature(peer)
		if !ok {
			return errors.New("application-egress destination selector is malformed or permissive")
		}
		ports := portSet(rule)
		if ports == "" {
			return errors.New("application-egress ports are malformed")
		}
		got = append(got, signature+"|"+ports)
	}
	sort.Strings(got)
	sort.Strings(want)
	if !sameStrings(got, want) {
		return fmt.Errorf("application-egress rules are %v, want exactly %v", got, want)
	}
	return nil
}

func peerSignature(peer map[string]any) (string, bool) {
	if exactKeys(peer, "namespaceSelector") {
		if name, ok := oneLabel(mapAt(peer, "namespaceSelector"), "kubernetes.io/metadata.name"); ok {
			return "ns:" + name, true
		}
	}
	if exactKeys(peer, "podSelector") {
		if name, ok := oneLabel(mapAt(peer, "podSelector"), "app.kubernetes.io/name"); ok {
			return "pod:" + name, true
		}
	}
	if exactKeys(peer, "namespaceSelector", "podSelector") {
		nsSelector := mapAt(peer, "namespaceSelector")
		ns := ""
		if len(nsSelector) != 0 {
			var ok bool
			ns, ok = oneLabel(nsSelector, "kubernetes.io/metadata.name")
			if !ok {
				return "", false
			}
		}
		pod, ok := oneLabel(mapAt(peer, "podSelector"), "app.kubernetes.io/name")
		if ok {
			return "nspod:" + ns + ":" + pod, true
		}
	}
	if exactKeys(peer, "ipBlock") && exactKeys(mapAt(peer, "ipBlock"), "cidr") {
		cidr := stringAt(mapAt(peer, "ipBlock"), "cidr")
		if cidr != "" {
			return "ip:" + cidr, true
		}
	}
	return "", false
}

func portSet(rule map[string]any) string {
	var ports []string
	for _, raw := range sliceAt(rule, "ports") {
		port, ok := raw.(map[string]any)
		if !ok || !exactKeys(port, "port", "protocol") || intAt(port, "port") < 1 || stringAt(port, "protocol") == "" {
			return ""
		}
		ports = append(ports, stringAt(port, "protocol")+"/"+strconv.FormatInt(intAt(port, "port"), 10))
	}
	sort.Strings(ports)
	return strings.Join(ports, ",")
}

func exactLabels(selector map[string]any, key, value string) bool {
	return exactKeys(selector, "matchLabels") && exactKeys(mapAt(selector, "matchLabels"), key) && stringAt(mapAt(selector, "matchLabels"), key) == value
}

func oneLabel(selector map[string]any, key string) (string, bool) {
	if !exactKeys(selector, "matchLabels") || !exactKeys(mapAt(selector, "matchLabels"), key) {
		return "", false
	}
	value := stringAt(mapAt(selector, "matchLabels"), key)
	return value, value != ""
}

func exactExpression(raw any, key, operator string, values []string) bool {
	expression, ok := raw.(map[string]any)
	return ok && exactKeys(expression, "key", "operator", "values") && stringAt(expression, "key") == key && stringAt(expression, "operator") == operator && sameStrings(stringSlice(expression, "values"), values)
}

func exactKeys(value map[string]any, keys ...string) bool {
	if len(value) != len(keys) {
		return false
	}
	for _, key := range keys {
		if _, ok := value[key]; !ok {
			return false
		}
	}
	return true
}

func validateMigrationEgress(docs []document) error {
	policy := find(docs, "NetworkPolicy", "migrate-egress")
	if policy == nil {
		return errors.New("migrate-egress NetworkPolicy is missing")
	}
	spec := mapAt(policy, "spec")
	if !exactKeys(spec, "podSelector", "policyTypes", "egress") || !exactLabels(mapAt(spec, "podSelector"), "app.kubernetes.io/name", "live-migrate") || !sameStrings(stringSlice(spec, "policyTypes"), []string{"Egress"}) || len(sliceAt(spec, "ingress")) != 0 {
		return errors.New("migrate-egress selector or policyTypes is invalid")
	}
	rules := sliceAt(spec, "egress")
	if len(rules) != 2 {
		return errors.New("migration egress must contain exactly DNS and PostgreSQL rules")
	}
	want := []string{"ns:kube-system|TCP/53,UDP/53", "nspod:gpt-live:postgres|TCP/5432"}
	got := make([]string, 0, 2)
	for _, raw := range rules {
		rule := raw.(map[string]any)
		if !exactKeys(rule, "to", "ports") {
			return errors.New("migration egress contains a malformed or permissive rule")
		}
		ports := portSet(rule)
		peers := sliceAt(rule, "to")
		if len(peers) != 1 {
			return errors.New("each migration egress rule must have exactly one destination")
		}
		peer := peers[0].(map[string]any)
		signature, ok := peerSignature(peer)
		if !ok || ports == "" {
			return errors.New("migration egress destination or ports are malformed")
		}
		got = append(got, signature+"|"+ports)
	}
	sort.Strings(got)
	sort.Strings(want)
	if !sameStrings(got, want) {
		return fmt.Errorf("migration egress rules are %v, want %v", got, want)
	}
	return nil
}

func validateProduction(docs []document, metadata map[string]any, images map[string]string, readiness string) error {
	if err := validateImages(images, readiness); err != nil {
		return err
	}
	ingress := find(docs, "Ingress", "live-gateway")
	if ingress == nil {
		return errors.New("production Ingress is missing")
	}
	host := stringAt(metadata, "production-host")
	tlsSecret := stringAt(metadata, "tls-secret")
	if readiness == "deploy" {
		if err := validateHost(host, false); err != nil {
			return fmt.Errorf("production host: %w", err)
		}
		if !namePattern.MatchString(tlsSecret) || obvious(tlsSecret) || tlsSecret == "gpt-live-production-tls" {
			return errors.New("production TLS secret name is invalid or obvious")
		}
	}
	if err := validateIngressGraph(ingress, host, tlsSecret); err != nil {
		return err
	}
	proxy := stringAt(metadata, "https-proxy")
	proxyURL, err := url.Parse(proxy)
	if err != nil || proxyURL.Scheme != "http" || proxyURL.Port() != "3128" || proxyURL.Hostname() == "" {
		return errors.New("HTTPS proxy must be an explicit HTTP endpoint on port 3128")
	}
	for _, name := range []string{"live-gateway", "live-worker"} {
		if envValue(firstContainer(find(docs, "Deployment", name)), "HTTPS_PROXY") != proxy {
			return fmt.Errorf("%s HTTPS_PROXY differs from deployment metadata", name)
		}
	}
	cidr := stringAt(metadata, "egress-proxy-cidr")
	_, egressNetwork, err := net.ParseCIDR(cidr)
	if err != nil || readiness == "deploy" && (reservedNetwork(egressNetwork) || cidr == "10.240.0.0/16") {
		return errors.New("egress proxy CIDR is invalid")
	}
	configMap := find(docs, "ConfigMap", "live-config")
	var config document
	if configMap == nil || yaml.Unmarshal([]byte(stringAt(mapAt(configMap, "data"), "config.yaml")), &config) != nil || stringAt(config, "mode") != "production" {
		return errors.New("rendered production config is missing or invalid")
	}
	trusted := stringSlice(mapAt(config, "gateway"), "trusted_proxies")
	if len(trusted) != 1 || trusted[0] == cidr || trusted[0] != stringAt(metadata, "trusted-proxy-cidr") {
		return errors.New("production trusted ingress and egress proxy CIDRs must be distinct")
	}
	_, trustedNetwork, err := net.ParseCIDR(trusted[0])
	if err != nil || readiness == "deploy" && (reservedNetwork(trustedNetwork) || trusted[0] == "10.240.0.0/16") {
		return errors.New("trusted ingress proxy CIDR is invalid or reserved")
	}
	origins := stringSlice(mapAt(config, "auth"), "allowed_origins")
	if len(origins) != 1 || origins[0] != "https://"+host {
		return errors.New("production allowed origin differs from Ingress host")
	}
	for _, key := range []string{"static_bearer_env", "static_tenant_id", "static_user_id", "static_scopes"} {
		if _, present := mapAt(config, "auth")[key]; present {
			return fmt.Errorf("production auth must not contain %s", key)
		}
	}
	for field, endpoint := range map[string]string{"voice": stringAt(mapAt(mapAt(config, "gateway"), "voice"), "url"), "oidc": stringAt(mapAt(config, "auth"), "oidc_issuer")} {
		u, parseErr := url.Parse(endpoint)
		if parseErr != nil || u.Host == "" || readiness == "deploy" && obvious(u.Hostname()) {
			return fmt.Errorf("production %s endpoint is invalid or obvious", field)
		}
	}
	policy := find(docs, "NetworkPolicy", "application-egress")
	if policy == nil || !networkRule(policy, cidr, "TCP", 3128) {
		return errors.New("application egress does not allow the configured proxy CIDR on TCP/3128")
	}
	if !networkPodRule(policy, "platform", "egress-proxy", "TCP", 3128) {
		return errors.New("application egress does not allow the configured platform proxy endpoint on TCP/3128")
	}
	if networkRule(policy, cidr, "TCP", 443) {
		return errors.New("configured proxy CIDR must not use generic TCP/443 egress")
	}
	return nil
}

func validateIngressGraph(ingress document, host, tlsSecret string) error {
	wantAnnotations := map[string]string{
		"nginx.ingress.kubernetes.io/proxy-read-timeout":    "3600",
		"nginx.ingress.kubernetes.io/proxy-send-timeout":    "3600",
		"nginx.ingress.kubernetes.io/proxy-connect-timeout": "10",
		"nginx.ingress.kubernetes.io/proxy-buffering":       "off",
		"nginx.ingress.kubernetes.io/proxy-body-size":       "1m",
	}
	if !exactStringMap(mapAt(mapAt(ingress, "metadata"), "annotations"), wantAnnotations) {
		return errors.New("Ingress WebSocket/proxy annotations are invalid")
	}
	spec := mapAt(ingress, "spec")
	if !exactKeys(spec, "ingressClassName", "tls", "rules") || stringAt(spec, "ingressClassName") != "nginx" {
		return errors.New("Ingress class/spec wiring is invalid")
	}
	tls := sliceAt(spec, "tls")
	if len(tls) != 1 {
		return errors.New("Ingress must contain exactly one TLS entry")
	}
	tlsEntry := tls[0].(map[string]any)
	if !exactKeys(tlsEntry, "hosts", "secretName") || !sameStrings(stringSlice(tlsEntry, "hosts"), []string{host}) || stringAt(tlsEntry, "secretName") != tlsSecret {
		return errors.New("Ingress TLS host/secret differs from metadata")
	}
	rules := sliceAt(spec, "rules")
	if len(rules) != 1 {
		return errors.New("Ingress must contain exactly one host rule")
	}
	rule := rules[0].(map[string]any)
	if !exactKeys(rule, "host", "http") || stringAt(rule, "host") != host || !exactKeys(mapAt(rule, "http"), "paths") {
		return errors.New("Ingress rule host differs from TLS/metadata")
	}
	paths := sliceAt(mapAt(rule, "http"), "paths")
	if len(paths) != 1 {
		return errors.New("Ingress must contain exactly one path")
	}
	path := paths[0].(map[string]any)
	if !exactKeys(path, "path", "pathType", "backend") || stringAt(path, "path") != "/" || stringAt(path, "pathType") != "Prefix" {
		return errors.New("Ingress path contract is invalid")
	}
	backend := mapAt(path, "backend")
	service := mapAt(backend, "service")
	if !exactKeys(backend, "service") || !exactKeys(service, "name", "port") || stringAt(service, "name") != "live-gateway" || !exactKeys(mapAt(service, "port"), "name") || stringAt(mapAt(service, "port"), "name") != "realtime" {
		return errors.New("Ingress backend must target live-gateway/realtime")
	}
	return nil
}

func networkPodRule(policy document, namespace, podName, protocol string, port int64) bool {
	for _, raw := range sliceAt(mapAt(policy, "spec"), "egress") {
		rule := raw.(map[string]any)
		matches := false
		for _, peerRaw := range sliceAt(rule, "to") {
			peer := peerRaw.(map[string]any)
			namespaceLabels := mapAt(mapAt(peer, "namespaceSelector"), "matchLabels")
			podLabels := mapAt(mapAt(peer, "podSelector"), "matchLabels")
			if stringAt(namespaceLabels, "kubernetes.io/metadata.name") == namespace && stringAt(podLabels, "app.kubernetes.io/name") == podName {
				matches = true
			}
		}
		if matches {
			for _, portRaw := range sliceAt(rule, "ports") {
				value := portRaw.(map[string]any)
				if stringAt(value, "protocol") == protocol && intAt(value, "port") == port {
					return true
				}
			}
		}
	}
	return false
}

func validateImages(images map[string]string, readiness string) error {
	digests := map[string]string{}
	for _, name := range []string{"live-gateway", "live-worker", "live-migrate"} {
		repository, digest, ok := parseImage(images[name])
		if !ok {
			return fmt.Errorf("%s production image must be repository@sha256:digest", name)
		}
		if obviousDigest(digest) {
			return fmt.Errorf("%s image uses an obvious repeated digest", name)
		}
		if previous := digests[digest]; previous != "" {
			return fmt.Errorf("%s and %s image digests repeat", previous, name)
		}
		digests[digest] = name
		if readiness == "deploy" && obvious(repository) {
			return fmt.Errorf("%s image repository is an example/test value", name)
		}
	}
	return nil
}

// parseImage accepts exactly registry/repository-path@sha256:lowercase-digest.
// It intentionally rejects URL syntax, tags, implicit registries, whitespace,
// empty path components, and uppercase/non-canonical variants.
func parseImage(raw string) (string, string, bool) {
	if raw == "" || strings.TrimSpace(raw) != raw || strings.Count(raw, "@sha256:") != 1 || strings.Contains(raw, "://") {
		return "", "", false
	}
	parsed, err := reference.Parse(raw)
	if err != nil || parsed.String() != raw {
		return "", "", false
	}
	canonical, ok := parsed.(reference.Canonical)
	if !ok {
		return "", "", false
	}
	if _, tagged := parsed.(reference.Tagged); tagged {
		return "", "", false
	}
	repository := canonical.Name()
	registry := reference.Domain(canonical)
	if len(repository) > reference.RepositoryNameTotalLengthMax || registry == "" || !strings.ContainsAny(registry, ".:") || reference.Path(canonical) == "" {
		return "", "", false
	}
	if strings.Contains(registry, ":") && !(strings.HasPrefix(registry, "[") && strings.HasSuffix(registry, "]")) {
		_, portText, err := net.SplitHostPort(registry)
		port, conversionErr := strconv.Atoi(portText)
		if err != nil || conversionErr != nil || port < 1 || port > 65535 {
			return "", "", false
		}
	}
	digest := canonical.Digest()
	if digest.Algorithm().String() != "sha256" || !digestPattern.MatchString(digest.Encoded()) {
		return "", "", false
	}
	return repository, digest.Encoded(), true
}

func imageWithRepositoryLength(length int) string {
	const prefix = "registry.acme.com/"
	if length < len(prefix)+1 {
		return ""
	}
	return prefix + strings.Repeat("a", length-len(prefix)) + "@sha256:4d1b72371c42a69bd949048883897aa673059f2e7b1cc992a650584132aae8f3"
}

func obviousDigest(digest string) bool {
	if !digestPattern.MatchString(digest) {
		return true
	}
	for period := 1; period <= 16; period++ {
		if len(digest)%period == 0 && strings.Repeat(digest[:period], len(digest)/period) == digest {
			return true
		}
	}
	return false
}

func validateProductionInputs(readiness string) error {
	if readiness != "deploy" && readiness != "fixture" {
		return errors.New("input readiness must be deploy or fixture")
	}
	required := []string{"LIVE_PRODUCTION_HOST", "LIVE_PRODUCTION_TLS_SECRET", "LIVE_PRODUCTION_TRUSTED_PROXY_CIDR", "LIVE_PRODUCTION_EGRESS_PROXY_CIDR", "LIVE_PRODUCTION_VOICE_URL", "LIVE_PRODUCTION_OIDC_ISSUER", "LIVE_PRODUCTION_GATEWAY_IMAGE", "LIVE_PRODUCTION_WORKER_IMAGE", "LIVE_PRODUCTION_MIGRATE_IMAGE"}
	for _, name := range required {
		value := os.Getenv(name)
		if value == "" || strings.ContainsAny(value, "\r\n&\\|") {
			return fmt.Errorf("%s is missing or contains unsafe characters", name)
		}
	}
	if err := validateHost(os.Getenv("LIVE_PRODUCTION_HOST"), readiness == "fixture"); err != nil {
		return err
	}
	tlsSecret := os.Getenv("LIVE_PRODUCTION_TLS_SECRET")
	if !namePattern.MatchString(tlsSecret) || readiness == "deploy" && (obvious(tlsSecret) || tlsSecret == "gpt-live-production-tls") {
		return errors.New("LIVE_PRODUCTION_TLS_SECRET is invalid or obvious")
	}
	for _, name := range []string{"LIVE_PRODUCTION_TRUSTED_PROXY_CIDR", "LIVE_PRODUCTION_EGRESS_PROXY_CIDR"} {
		_, network, err := net.ParseCIDR(os.Getenv(name))
		if err != nil || readiness == "deploy" && (reservedNetwork(network) || os.Getenv(name) == "10.240.0.0/16") {
			return fmt.Errorf("%s is invalid or reserved", name)
		}
	}
	if os.Getenv("LIVE_PRODUCTION_TRUSTED_PROXY_CIDR") == os.Getenv("LIVE_PRODUCTION_EGRESS_PROXY_CIDR") {
		return errors.New("trusted ingress and egress proxy CIDRs must be distinct")
	}
	for _, pair := range []struct{ name, scheme string }{{"LIVE_PRODUCTION_VOICE_URL", "wss"}, {"LIVE_PRODUCTION_OIDC_ISSUER", "https"}} {
		u, err := url.Parse(os.Getenv(pair.name))
		if err != nil || u.Scheme != pair.scheme || u.Host == "" || readiness == "deploy" && obvious(u.Hostname()) {
			return fmt.Errorf("%s must use reviewed %s endpoint", pair.name, pair.scheme)
		}
	}
	return validateImages(map[string]string{"live-gateway": os.Getenv("LIVE_PRODUCTION_GATEWAY_IMAGE"), "live-worker": os.Getenv("LIVE_PRODUCTION_WORKER_IMAGE"), "live-migrate": os.Getenv("LIVE_PRODUCTION_MIGRATE_IMAGE")}, readiness)
}

func validateHost(host string, fixture bool) error {
	if !hostPattern.MatchString(host) || !fixture && obvious(host) {
		return errors.New("host must be a non-reserved fully qualified DNS name")
	}
	return nil
}

func obvious(value string) bool {
	lower := strings.ToLower(value)
	for _, marker := range []string{"placeholder", "replace", "changeme", "dummy", "fixture", "example", ".test", ".invalid", "localhost", ".local", ".internal"} {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
}

func reservedNetwork(network *net.IPNet) bool {
	if network == nil {
		return true
	}
	ip := network.IP
	if ip.IsUnspecified() || ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsMulticast() {
		return true
	}
	for _, raw := range []string{"192.0.2.0/24", "198.51.100.0/24", "203.0.113.0/24", "2001:db8::/32"} {
		_, reserved, _ := net.ParseCIDR(raw)
		if reserved.Contains(ip) {
			return true
		}
	}
	return false
}

func networkRule(policy document, cidr, protocol string, port int64) bool {
	for _, raw := range sliceAt(mapAt(policy, "spec"), "egress") {
		rule := raw.(map[string]any)
		matchesCIDR := false
		for _, peerRaw := range sliceAt(rule, "to") {
			peer := peerRaw.(map[string]any)
			if stringAt(mapAt(peer, "ipBlock"), "cidr") == cidr {
				matchesCIDR = true
			}
		}
		if !matchesCIDR {
			continue
		}
		for _, portRaw := range sliceAt(rule, "ports") {
			value := portRaw.(map[string]any)
			if stringAt(value, "protocol") == protocol && intAt(value, "port") == port {
				return true
			}
		}
	}
	return false
}

func mutateDocuments(docs []document, mutation string) ([]document, error) {
	parts := strings.SplitN(mutation, ":", 2)
	if len(parts) != 2 || parts[0] != "identity" {
		return docs, mutate(docs, mutation)
	}
	targets := map[string][2]string{
		"duplicate-safe":       {"ConfigMap", "live-config"},
		"duplicate-namespace":  {"Namespace", "gpt-live"},
		"duplicate-configmap":  {"ConfigMap", "live-deploy-metadata"},
		"duplicate-service":    {"Service", "live-gateway"},
		"duplicate-deployment": {"Deployment", "live-gateway"},
		"duplicate-job":        {"Job", "live-migrate"},
		"duplicate-pdb":        {"PodDisruptionBudget", "live-gateway"},
		"duplicate-hpa":        {"HorizontalPodAutoscaler", "live-gateway"},
		"duplicate-ingress":    {"Ingress", "live-gateway"},
		"duplicate-policy":     {"NetworkPolicy", "application-egress"},
	}
	target, ok := targets[parts[1]]
	if !ok {
		return docs, errors.New("unknown identity mutation")
	}
	original := find(docs, target[0], target[1])
	if original == nil {
		return docs, errors.New("identity mutation target is missing")
	}
	encoded, err := yaml.Marshal(original)
	if err != nil {
		return docs, err
	}
	var duplicate document
	if err := yaml.Unmarshal(encoded, &duplicate); err != nil {
		return docs, err
	}
	switch parts[1] {
	case "duplicate-configmap":
		mapAt(duplicate, "data")["render-readiness"] = "unsafe"
	case "duplicate-service":
		mapAt(mapAt(duplicate, "spec"), "selector")["app.kubernetes.io/name"] = "unsafe"
	case "duplicate-deployment":
		mapAt(firstContainer(duplicate), "securityContext")["readOnlyRootFilesystem"] = false
	case "duplicate-job":
		mapAt(firstContainer(duplicate), "securityContext")["readOnlyRootFilesystem"] = false
	case "duplicate-pdb":
		mapAt(duplicate, "spec")["minAvailable"] = 0
	case "duplicate-hpa":
		mapAt(duplicate, "spec")["maxReplicas"] = 1000
	case "duplicate-ingress":
		sliceAt(mapAt(duplicate, "spec"), "rules")[0].(map[string]any)["host"] = "unsafe.invalid"
	case "duplicate-policy":
		mapAt(duplicate, "spec")["egress"] = append(sliceAt(mapAt(duplicate, "spec"), "egress"), document{})
	}
	return append(docs, duplicate), nil
}

func mutate(docs []document, mutation string) error {
	parts := strings.SplitN(mutation, ":", 2)
	if len(parts) != 2 {
		return errors.New("mutation must be workload:invariant")
	}
	name, invariant := parts[0], parts[1]
	if name == "service" {
		service := find(docs, "Service", "live-gateway")
		spec := mapAt(service, "spec")
		port := sliceAt(spec, "ports")[0].(map[string]any)
		switch invariant {
		case "selector":
			mapAt(spec, "selector")["app.kubernetes.io/name"] = "live-worker"
		case "port":
			port["port"] = 81
		case "target-port":
			port["targetPort"] = "wrong"
		case "protocol":
			port["protocol"] = "UDP"
		case "extra-port":
			spec["ports"] = append(sliceAt(spec, "ports"), document{"name": "extra", "port": 82, "targetPort": "realtime"})
		default:
			return errors.New("unknown service mutation")
		}
		return nil
	}
	if strings.HasPrefix(name, "pdb-") {
		workload := "live-" + strings.TrimPrefix(name, "pdb-")
		pdb := find(docs, "PodDisruptionBudget", workload)
		spec := mapAt(pdb, "spec")
		switch invariant {
		case "selector":
			mapAt(mapAt(spec, "selector"), "matchLabels")["app.kubernetes.io/name"] = map[string]string{"live-gateway": "live-worker", "live-worker": "live-gateway"}[workload]
		case "minimum":
			spec["minAvailable"] = 0
		case "extra":
			spec["maxUnavailable"] = 1
		default:
			return errors.New("unknown PDB mutation")
		}
		return nil
	}
	if strings.HasPrefix(name, "hpa-") {
		workload := "live-" + strings.TrimPrefix(name, "hpa-")
		hpa := find(docs, "HorizontalPodAutoscaler", workload)
		spec := mapAt(hpa, "spec")
		switch invariant {
		case "target":
			mapAt(spec, "scaleTargetRef")["name"] = map[string]string{"live-gateway": "live-worker", "live-worker": "live-gateway"}[workload]
		case "replicas":
			spec["maxReplicas"] = 999
		case "behavior":
			mapAt(mapAt(spec, "behavior"), "scaleDown")["stabilizationWindowSeconds"] = 0
		case "metric-name":
			metric := sliceAt(spec, "metrics")[len(sliceAt(spec, "metrics"))-1].(map[string]any)
			class := strings.ToLower(stringAt(metric, "type"))
			if class == "resource" {
				mapAt(metric, class)["name"] = "memory"
			} else {
				mapAt(mapAt(metric, class), "metric")["name"] = "wrong"
			}
		case "metric-value":
			metric := sliceAt(spec, "metrics")[0].(map[string]any)
			mapAt(mapAt(metric, "resource"), "target")["averageUtilization"] = 1
		case "metric-type":
			sliceAt(spec, "metrics")[0].(map[string]any)["type"] = "Object"
		case "extra-metric":
			spec["metrics"] = append(sliceAt(spec, "metrics"), sliceAt(spec, "metrics")[0])
		default:
			return errors.New("unknown HPA mutation")
		}
		return nil
	}
	if name == "ingress" {
		ingress := find(docs, "Ingress", "live-gateway")
		spec := mapAt(ingress, "spec")
		tls := sliceAt(spec, "tls")[0].(map[string]any)
		rule := sliceAt(spec, "rules")[0].(map[string]any)
		paths := sliceAt(mapAt(rule, "http"), "paths")
		path := paths[0].(map[string]any)
		service := mapAt(mapAt(path, "backend"), "service")
		switch invariant {
		case "backend":
			service["name"] = "live-worker"
		case "backend-port":
			mapAt(service, "port")["name"] = "wrong"
		case "rule-host":
			rule["host"] = "wrong.invalid"
		case "tls-host":
			tls["hosts"] = []any{"wrong.invalid"}
		case "tls-secret":
			tls["secretName"] = "wrong"
		case "path":
			path["path"] = "/wrong"
		case "class":
			spec["ingressClassName"] = "other"
		case "annotation":
			mapAt(mapAt(ingress, "metadata"), "annotations")["nginx.ingress.kubernetes.io/proxy-buffering"] = "on"
		case "extra-path":
			mapAt(rule, "http")["paths"] = append(paths, path)
		default:
			return errors.New("unknown Ingress mutation")
		}
		return nil
	}
	if name == "live-config" {
		config := find(docs, "ConfigMap", "live-config")
		switch invariant {
		case "missing":
			delete(mapAt(config, "data"), "config.yaml")
		case "extra":
			mapAt(config, "data")["extra"] = "orphan"
		case "content":
			mapAt(config, "data")["config.yaml"] = strings.Replace(stringAt(mapAt(config, "data"), "config.yaml"), "service_name: gpt-live", "service_name: gpt-live-mutated", 1)
		default:
			return errors.New("unknown live-config mutation")
		}
		return nil
	}
	if name == "deploy-metadata" {
		metadata := find(docs, "ConfigMap", "live-deploy-metadata")
		switch invariant {
		case "trusted":
			mapAt(metadata, "data")["trusted-proxy-cidr"] = "10.99.0.0/16"
		case "extra":
			mapAt(metadata, "data")["orphan"] = "value"
		default:
			return errors.New("unknown metadata mutation")
		}
		return nil
	}
	if name == "default-deny" {
		policy := find(docs, "NetworkPolicy", name)
		if policy == nil || invariant != "selector" {
			return errors.New("unknown default-deny mutation")
		}
		mapAt(policy, "spec")["podSelector"] = document{"matchLabels": document{"unsafe": "true"}}
		return nil
	}
	if name == "migrate-egress" {
		policy := find(docs, "NetworkPolicy", name)
		if policy == nil {
			return errors.New("unknown migrate-egress mutation")
		}
		spec := mapAt(policy, "spec")
		switch invariant {
		case "extra":
			spec["egress"] = append(sliceAt(spec, "egress"), document{"to": []any{document{"ipBlock": document{"cidr": "0.0.0.0/0"}}}, "ports": []any{document{"protocol": "TCP", "port": 443}}})
		case "selector":
			mapAt(mapAt(spec, "podSelector"), "matchLabels")["app.kubernetes.io/name"] = "live-gateway"
		case "source":
			mapAt(mapAt(sliceAt(sliceAt(spec, "egress")[1].(map[string]any), "to")[0].(map[string]any), "podSelector"), "matchLabels")["app.kubernetes.io/name"] = "other"
		case "widen":
			sliceAt(sliceAt(spec, "egress")[1].(map[string]any), "to")[0].(map[string]any)["namespaceSelector"] = document{}
		case "cross-namespace":
			mapAt(mapAt(sliceAt(sliceAt(spec, "egress")[1].(map[string]any), "to")[0].(map[string]any), "namespaceSelector"), "matchLabels")["kubernetes.io/metadata.name"] = "other"
		case "port":
			sliceAt(sliceAt(spec, "egress")[1].(map[string]any), "ports")[0].(map[string]any)["port"] = 443
		default:
			return errors.New("unknown migrate-egress mutation")
		}
		return nil
	}
	if name == "gateway-ingress" {
		policy := find(docs, "NetworkPolicy", name)
		if policy == nil {
			return errors.New("unknown gateway-ingress mutation")
		}
		spec := mapAt(policy, "spec")
		rule := sliceAt(spec, "ingress")[0].(map[string]any)
		peer := sliceAt(rule, "from")[0].(map[string]any)
		switch invariant {
		case "selector":
			mapAt(mapAt(spec, "podSelector"), "matchLabels")["app.kubernetes.io/name"] = "live-worker"
		case "source-namespace":
			mapAt(mapAt(peer, "namespaceSelector"), "matchLabels")["kubernetes.io/metadata.name"] = "default"
		case "source-pod":
			for key := range mapAt(mapAt(peer, "podSelector"), "matchLabels") {
				mapAt(mapAt(peer, "podSelector"), "matchLabels")[key] = "other"
			}
		case "port":
			sliceAt(rule, "ports")[0].(map[string]any)["port"] = 9090
		case "extra":
			rule["from"] = append(sliceAt(rule, "from"), document{"ipBlock": document{"cidr": "0.0.0.0/0"}})
		default:
			return errors.New("unknown gateway-ingress mutation")
		}
		return nil
	}
	if name == "application-egress" {
		policy := find(docs, "NetworkPolicy", name)
		if policy == nil {
			return errors.New("unknown application-egress mutation")
		}
		spec := mapAt(policy, "spec")
		if invariant == "selector" {
			sliceAt(mapAt(spec, "podSelector"), "matchExpressions")[0].(map[string]any)["values"] = []any{"gateway"}
			return nil
		}
		if invariant == "extra" {
			spec["egress"] = append(sliceAt(spec, "egress"), document{"to": []any{document{"ipBlock": document{"cidr": "0.0.0.0/0"}}}, "ports": []any{document{"protocol": "TCP", "port": 443}}})
			return nil
		}
		if invariant == "oidc-port" {
			for _, raw := range sliceAt(spec, "egress") {
				rule := raw.(map[string]any)
				signature, _ := peerSignature(sliceAt(rule, "to")[0].(map[string]any))
				if signature == "nspod:identity:oidc" {
					sliceAt(rule, "ports")[0].(map[string]any)["port"] = 80
					return nil
				}
			}
		}
		if invariant == "widen" {
			for _, raw := range sliceAt(spec, "egress") {
				rule := raw.(map[string]any)
				peer := sliceAt(rule, "to")[0].(map[string]any)
				if signature, _ := peerSignature(peer); strings.Contains(signature, ":postgres") {
					peer["namespaceSelector"] = document{}
					return nil
				}
			}
		}
		if invariant == "cross-namespace" {
			for _, raw := range sliceAt(spec, "egress") {
				peer := sliceAt(raw.(map[string]any), "to")[0].(map[string]any)
				if signature, _ := peerSignature(peer); strings.Contains(signature, ":postgres") {
					mapAt(mapAt(peer, "namespaceSelector"), "matchLabels")["kubernetes.io/metadata.name"] = "other"
					return nil
				}
			}
		}
		if invariant != "proxy-port" {
			return errors.New("unknown application-egress mutation")
		}
		for _, raw := range sliceAt(mapAt(policy, "spec"), "egress") {
			rule := raw.(map[string]any)
			for _, peerRaw := range sliceAt(rule, "to") {
				if stringAt(mapAt(peerRaw.(map[string]any), "ipBlock"), "cidr") != "" {
					sliceAt(rule, "ports")[0].(map[string]any)["port"] = 443
					return nil
				}
			}
		}
		return errors.New("proxy CIDR rule is missing")
	}
	if name == "namespace" {
		namespace := find(docs, "Namespace", "gpt-live")
		if namespace == nil || invariant != "name" {
			return errors.New("unknown namespace mutation")
		}
		mapAt(namespace, "metadata")["name"] = "other"
		return nil
	}
	kind := "Deployment"
	if name == "live-migrate" {
		kind = "Job"
	}
	doc := find(docs, kind, name)
	if doc == nil {
		return errors.New("mutation workload is missing")
	}
	pod, podMeta := podSpec(doc)
	container := firstContainer(doc)
	switch invariant {
	case "root":
		mapAt(pod, "securityContext")["runAsUser"] = 0
	case "writable":
		mapAt(container, "securityContext")["readOnlyRootFilesystem"] = false
	case "privilege":
		mapAt(container, "securityContext")["allowPrivilegeEscalation"] = true
	case "capabilities":
		mapAt(mapAt(container, "securityContext"), "capabilities")["drop"] = []any{}
	case "seccomp":
		mapAt(mapAt(pod, "securityContext"), "seccompProfile")["type"] = "Unconfined"
	case "automount":
		pod["automountServiceAccountToken"] = true
	case "readiness", "liveness", "startup":
		delete(container, invariant+"Probe")
	case "grace":
		pod["terminationGracePeriodSeconds"] = 1
	case "prestop":
		delete(container, "lifecycle")
	case "readiness-path":
		command := sliceAt(mapAt(mapAt(container, "readinessProbe"), "exec"), "command")
		command[len(command)-1] = "http://127.0.0.1:9090/health/live"
	case "readiness-port":
		command := sliceAt(mapAt(mapAt(container, "readinessProbe"), "exec"), "command")
		command[len(command)-1] = "http://127.0.0.1:9091/health/ready"
	case "prestop-noop":
		mapAt(mapAt(mapAt(container, "lifecycle"), "preStop"), "exec")["command"] = []any{"/bin/true"}
	case "prestop-binary":
		sliceAt(mapAt(mapAt(mapAt(container, "lifecycle"), "preStop"), "exec"), "command")[0] = "/bin/true"
	case "prestop-port":
		sliceAt(mapAt(mapAt(mapAt(container, "lifecycle"), "preStop"), "exec"), "command")[2] = "8080"
	case "prestop-wait":
		sliceAt(mapAt(mapAt(mapAt(container, "lifecycle"), "preStop"), "exec"), "command")[4] = "0s"
	case "config-hash":
		mapAt(podMeta, "annotations")["live.openai.com/config-hash"] = "v1:" + strings.Repeat("0", 64)
	case "pod-label":
		mapAt(podMeta, "labels")["app.kubernetes.io/component"] = "other"
	case "metadata-label":
		mapAt(mapAt(doc, "metadata"), "labels")["app.kubernetes.io/name"] = "other"
	case "selector":
		mapAt(mapAt(mapAt(doc, "spec"), "selector"), "matchLabels")["app.kubernetes.io/name"] = "other"
	case "config-volume":
		mapAt(sliceAt(pod, "volumes")[0].(map[string]any), "configMap")["name"] = "other"
	case "config-subpath":
		sliceAt(container, "volumeMounts")[0].(map[string]any)["subPath"] = "other.yaml"
	case "config-mount":
		sliceAt(container, "volumeMounts")[0].(map[string]any)["mountPath"] = "/wrong/config.yaml"
	case "config-key":
		sliceAt(mapAt(sliceAt(pod, "volumes")[0].(map[string]any), "configMap"), "items")[0].(map[string]any)["key"] = "other.yaml"
	case "config-path":
		sliceAt(mapAt(sliceAt(pod, "volumes")[0].(map[string]any), "configMap"), "items")[0].(map[string]any)["path"] = "other.yaml"
	case "extra-volume":
		pod["volumes"] = append(sliceAt(pod, "volumes"), document{"name": "orphan", "emptyDir": document{}})
	case "args":
		container["args"] = []any{"-config", "/wrong/config.yaml"}
	case "container-port":
		sliceAt(container, "ports")[0].(map[string]any)["containerPort"] = 8081
	case "container-port-name":
		sliceAt(container, "ports")[0].(map[string]any)["name"] = "wrong"
	case "container-port-protocol":
		sliceAt(container, "ports")[0].(map[string]any)["protocol"] = "UDP"
	case "image":
		container["image"] = "registry.example.invalid/mutated@sha256:" + strings.Repeat("a", 64)
	case "image-scheme":
		container["image"] = "https://registry.acme.com/mutated@sha256:4d1b72371c42a69bd949048883897aa673059f2e7b1cc992a650584132aae8f3"
	case "image-tag":
		container["image"] = "registry.acme.com/mutated:v1@sha256:4d1b72371c42a69bd949048883897aa673059f2e7b1cc992a650584132aae8f3"
	case "image-space":
		container["image"] = " registry.acme.com/mutated@sha256:4d1b72371c42a69bd949048883897aa673059f2e7b1cc992a650584132aae8f3"
	case "image-255":
		container["image"] = imageWithRepositoryLength(255)
	case "image-256":
		container["image"] = imageWithRepositoryLength(256)
	case "resources":
		delete(container, "resources")
	case "init-writable":
		copy := document{"name": "unsafe-init", "image": container["image"], "securityContext": document{"readOnlyRootFilesystem": false, "allowPrivilegeEscalation": false, "capabilities": document{"drop": []any{"ALL"}}}, "resources": container["resources"]}
		pod["initContainers"] = []any{copy}
	case "static-secret":
		var kept []any
		for _, raw := range sliceAt(container, "env") {
			if stringAt(raw.(map[string]any), "name") != "LIVE_STATIC_BEARER_TOKEN" {
				kept = append(kept, raw)
			}
		}
		container["env"] = kept
	case "static-secret-prod":
		container["env"] = append(sliceAt(container, "env"), document{"name": "LIVE_STATIC_BEARER_TOKEN", "valueFrom": document{"secretKeyRef": document{"name": "live-secrets", "key": "LIVE_STATIC_BEARER_TOKEN"}}})
	case "static-value-prod":
		container["env"] = append(sliceAt(container, "env"), document{"name": "LIVE_STATIC_BEARER_TOKEN", "value": "inline-static-secret"})
	case "duplicate-inline-admin":
		container["env"] = append(sliceAt(container, "env"), document{"name": "LIVE_ADMIN_BEARER_TOKEN", "value": "inline-secret"})
	case "inline-admin":
		for _, raw := range sliceAt(container, "env") {
			env := raw.(map[string]any)
			if stringAt(env, "name") == "LIVE_ADMIN_BEARER_TOKEN" {
				delete(env, "valueFrom")
				env["value"] = "inline-secret"
			}
		}
	case "unexpected-secret":
		container["env"] = append(sliceAt(container, "env"), document{"name": "EXTRA_TOKEN", "valueFrom": document{"secretKeyRef": document{"name": "live-secrets", "key": "EXTRA_TOKEN"}}})
	case "prestop-auth":
		var kept []any
		for _, raw := range sliceAt(container, "env") {
			if stringAt(raw.(map[string]any), "name") != "LIVE_ADMIN_BEARER_TOKEN" {
				kept = append(kept, raw)
			}
		}
		container["env"] = kept
	default:
		return errors.New("unknown mutation invariant")
	}
	return nil
}

func find(docs []document, kind, name string) document {
	for _, doc := range docs {
		if stringAt(doc, "kind") == kind && stringAt(mapAt(doc, "metadata"), "name") == name {
			return doc
		}
	}
	return nil
}

func findKind(docs []document, kind string) document {
	for _, doc := range docs {
		if stringAt(doc, "kind") == kind {
			return doc
		}
	}
	return nil
}

func podSpec(doc document) (map[string]any, map[string]any) {
	template := mapAt(mapAt(doc, "spec"), "template")
	return mapAt(template, "spec"), mapAt(template, "metadata")
}

func firstContainer(doc document) map[string]any {
	pod, _ := podSpec(doc)
	values := sliceAt(pod, "containers")
	if len(values) == 0 {
		return document{}
	}
	value, _ := values[0].(map[string]any)
	return value
}

func mapAt(value map[string]any, key string) map[string]any {
	if value == nil {
		return document{}
	}
	result, _ := value[key].(map[string]any)
	if result == nil {
		return document{}
	}
	return result
}

func sliceAt(value map[string]any, key string) []any {
	if value == nil {
		return nil
	}
	result, _ := value[key].([]any)
	return result
}

func stringAt(value map[string]any, key string) string {
	if value == nil {
		return ""
	}
	result, _ := value[key].(string)
	return result
}

func intAt(value map[string]any, key string) int64 {
	if value == nil {
		return 0
	}
	switch result := value[key].(type) {
	case int:
		return int64(result)
	case int64:
		return result
	case uint64:
		return int64(result)
	default:
		return 0
	}
}

func boolAt(value map[string]any, key string, missing bool) bool {
	if value == nil {
		return missing
	}
	result, ok := value[key].(bool)
	if !ok {
		return missing
	}
	return result
}

func stringSlice(value map[string]any, key string) []string {
	var out []string
	for _, raw := range sliceAt(value, key) {
		if text, ok := raw.(string); ok {
			out = append(out, text)
		}
	}
	sort.Strings(out)
	return out
}

func orderedStringSlice(value map[string]any, key string) []string {
	var out []string
	for _, raw := range sliceAt(value, key) {
		text, ok := raw.(string)
		if !ok {
			return nil
		}
		out = append(out, text)
	}
	return out
}

func sameStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func envValue(container map[string]any, name string) string {
	for _, raw := range sliceAt(container, "env") {
		env := raw.(map[string]any)
		if stringAt(env, "name") == name {
			return stringAt(env, "value")
		}
	}
	return ""
}

func secretEnv(container map[string]any, name, secret, key string) bool {
	for _, raw := range sliceAt(container, "env") {
		env := raw.(map[string]any)
		if stringAt(env, "name") != name || stringAt(env, "value") != "" {
			continue
		}
		ref := mapAt(mapAt(env, "valueFrom"), "secretKeyRef")
		return stringAt(ref, "name") == secret && stringAt(ref, "key") == key
	}
	return false
}
