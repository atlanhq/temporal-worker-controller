// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2024 Datadog, Inc.

package k8s

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/distribution/reference"
	temporaliov1alpha1 "github.com/temporalio/temporal-worker-controller/api/v1alpha1"
	"github.com/temporalio/temporal-worker-controller/internal/controller/k8s.io/utils"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	DeployOwnerKey = ".metadata.controller"
	// BuildIDLabel is the label that identifies the build ID for a deployment
	BuildIDLabel                  = "temporal.io/build-id"
	twdNameLabel                  = "temporal.io/deployment-name"
	WorkerDeploymentNameSeparator = "/"
	ResourceNameSeparator         = "-"
	MaxBuildIDLen                 = 63
	MaxDeploymentNameLen          = 47
	ConnectionSpecHashAnnotation  = "temporal.io/connection-spec-hash"
	PodTemplateSpecHashAnnotation = "temporal.io/pod-template-spec-hash"
	// TemporalDeploymentNameEnvVar names the env var that records, on each worker pod, the
	// Temporal worker deployment name its version was registered under at creation time.
	TemporalDeploymentNameEnvVar = "TEMPORAL_DEPLOYMENT_NAME"

	// VariantLabel discriminates the pod-shape variant (spec.variants) a versioned
	// Deployment belongs to. Only present when the TWD declares variants: the base
	// Deployment carries VariantLabel=BaseVariantName and each variant Deployment
	// carries its variant name, keeping their selectors disjoint while both share
	// the version-independent twdNameLabel (so the scale subresource - and any VPA
	// targeting the TWD - spans all of them). TWDs without variants render exactly
	// as before, with no VariantLabel at all.
	VariantLabel = "temporal.io/variant"
	// BaseVariantName is the reserved VariantLabel value for the base Deployment.
	BaseVariantName = "base"
	// VariantSpecHashAnnotation stores a hash of the WorkerVariant delta applied to
	// a variant Deployment, enabling drift detection when the build ID is stable
	// but the variant's delta (affinity, resources, ...) changed.
	VariantSpecHashAnnotation = "temporal.io/variant-spec-hash"
)

// DeploymentState represents the Kubernetes state of all deployments for a temporal worker deployment
type DeploymentState struct {
	// Map of buildID to the BASE deployment. Variant deployments (VariantLabel set
	// to anything but BaseVariantName) live in VariantDeployments instead, so two
	// same-build Deployments never collide here and every legacy consumer of this
	// map keeps meaning "the base".
	Deployments map[string]*appsv1.Deployment
	// Sorted base deployments by creation time
	DeploymentsByTime []*appsv1.Deployment
	// Map of buildID to base deployment references
	DeploymentRefs map[string]*corev1.ObjectReference

	// VariantDeployments maps buildID -> variant name -> deployment for the
	// spec.variants child Deployments of each version.
	VariantDeployments map[string]map[string]*appsv1.Deployment
	// VariantDeploymentRefs mirrors VariantDeployments with object references.
	VariantDeploymentRefs map[string]map[string]*corev1.ObjectReference
}

// GetDeploymentState queries Kubernetes to get the state of all deployments
// associated with a TemporalWorkerDeployment
func GetDeploymentState(
	ctx context.Context,
	k8sClient client.Client,
	namespace string,
	ownerName string,
	workerDeploymentName string,
) (*DeploymentState, error) {
	state := &DeploymentState{
		Deployments:           make(map[string]*appsv1.Deployment),
		DeploymentsByTime:     []*appsv1.Deployment{},
		DeploymentRefs:        make(map[string]*corev1.ObjectReference),
		VariantDeployments:    make(map[string]map[string]*appsv1.Deployment),
		VariantDeploymentRefs: make(map[string]map[string]*corev1.ObjectReference),
	}

	// List k8s deployments that correspond to managed worker deployment versions
	var childDeploys appsv1.DeploymentList
	if err := k8sClient.List(
		ctx,
		&childDeploys,
		client.InNamespace(namespace),
		client.MatchingFields{DeployOwnerKey: ownerName},
	); err != nil {
		return nil, fmt.Errorf("unable to list child deployments: %w", err)
	}

	// Sort deployments by creation timestamp
	sort.SliceStable(childDeploys.Items, func(i, j int) bool {
		return childDeploys.Items[i].ObjectMeta.CreationTimestamp.Before(&childDeploys.Items[j].ObjectMeta.CreationTimestamp)
	})

	// Track each k8s deployment by build ID. Variant deployments (VariantLabel
	// set to anything but BaseVariantName) are kept in the variant maps so a
	// version's base and variants never collide on the shared buildID key.
	for i := range childDeploys.Items {
		deploy := &childDeploys.Items[i]
		buildID, ok := deploy.GetLabels()[BuildIDLabel]
		if !ok {
			continue // Any deployments without the build ID label are ignored
		}
		if variant := deploy.GetLabels()[VariantLabel]; variant != "" && variant != BaseVariantName {
			if state.VariantDeployments[buildID] == nil {
				state.VariantDeployments[buildID] = make(map[string]*appsv1.Deployment)
				state.VariantDeploymentRefs[buildID] = make(map[string]*corev1.ObjectReference)
			}
			state.VariantDeployments[buildID][variant] = deploy
			state.VariantDeploymentRefs[buildID][variant] = NewObjectRef(deploy)
			continue
		}
		state.Deployments[buildID] = deploy
		state.DeploymentsByTime = append(state.DeploymentsByTime, deploy)
		state.DeploymentRefs[buildID] = NewObjectRef(deploy)
	}

	return state, nil
}

// IsDeploymentHealthy checks if a deployment is in the "Available" state
func IsDeploymentHealthy(deployment *appsv1.Deployment) (bool, *metav1.Time) {
	// TODO(jlegrone): do we need to sort conditions by timestamp to check only latest?
	for _, c := range deployment.Status.Conditions {
		if c.Type == appsv1.DeploymentAvailable && c.Status == corev1.ConditionTrue {
			return true, &c.LastTransitionTime
		}
	}
	return false, nil
}

// NewObjectRef creates a reference to a Kubernetes object
func NewObjectRef(obj client.Object) *corev1.ObjectReference {
	return &corev1.ObjectReference{
		APIVersion: obj.GetObjectKind().GroupVersionKind().GroupVersion().String(),
		Kind:       obj.GetObjectKind().GroupVersionKind().Kind,
		Name:       obj.GetName(),
		Namespace:  obj.GetNamespace(),
		UID:        obj.GetUID(),
	}
}

func ComputeBuildID(w *temporaliov1alpha1.TemporalWorkerDeployment) string {
	// Check for user-provided build ID in spec.workerOptions.unsafeCustomBuildID
	if override := w.Spec.WorkerOptions.UnsafeCustomBuildID; override != "" {
		cleaned := cleanBuildID(override)
		if cleaned != "" {
			return TruncateString(cleaned, MaxBuildIDLen)
		}
		// Fall through to default hash-based generation if buildID is invalid after cleaning
	}

	if containers := w.Spec.Template.Spec.Containers; len(containers) > 0 {
		if img := containers[0].Image; img != "" {
			shortHashSuffix := ResourceNameSeparator + utils.ComputeHash(&w.Spec.Template, nil, true)
			maxImgLen := MaxBuildIDLen - len(shortHashSuffix)
			imagePrefix := computeImagePrefix(img, maxImgLen)
			return cleanBuildID(imagePrefix + shortHashSuffix)
		}
	}
	return utils.ComputeHash(&w.Spec.Template, nil, false)
}

// ComputeWorkerDeploymentName generates the base worker deployment name
func ComputeWorkerDeploymentName(w *temporaliov1alpha1.TemporalWorkerDeployment) string {
	// Atlan: an explicit WorkerDeploymentName lets multiple TWDs share one logical
	// Temporal Worker Deployment (see WorkerOptions.WorkerDeploymentName). When unset,
	// derive from namespace/name.
	if override := w.Spec.WorkerOptions.WorkerDeploymentName; override != "" {
		return override
	}
	// Use the name and namespace to form the worker deployment name
	return w.GetNamespace() + WorkerDeploymentNameSeparator + w.GetName()
}

// WorkerDeploymentNameFromDeployment returns the Temporal worker deployment name that the given
// Deployment's workers were registered under, read from the TemporalDeploymentNameEnvVar set at
// creation time. It returns "" if no container records the env var. This is the reliable signal
// for detecting a Deployment left behind by a spec.workerOptions.workerDeploymentName change: its
// recorded name differs from the currently-resolved one.
func WorkerDeploymentNameFromDeployment(d *appsv1.Deployment) string {
	if d == nil {
		return ""
	}
	for _, container := range d.Spec.Template.Spec.Containers {
		for _, env := range container.Env {
			if env.Name == TemporalDeploymentNameEnvVar {
				return env.Value
			}
		}
	}
	return ""
}

// ComputeVersionedDeploymentName generates a name for a versioned deployment
// Name will be <=47 characters and unique for that Worker Deployment Version within the namespace.
func ComputeVersionedDeploymentName(baseName, buildID string) string {
	fullName := baseName + ResourceNameSeparator + buildID
	if len(fullName) > MaxDeploymentNameLen {
		hashName := HashString(fullName)[:10]
		fullName = TruncateString(baseName, 10) + ResourceNameSeparator + TruncateString(buildID, 10) + ResourceNameSeparator + hashName
	}
	return CleanStringForDNS(fullName)
}

func HashString(s string) string {
	h := sha256.New()
	_, _ = h.Write([]byte(s))
	return hex.EncodeToString(h.Sum(nil))
}

func computeImagePrefix(s string, maxLen int) string {
	ref, err := reference.Parse(s)
	if err == nil {
		switch v := ref.(type) {
		case reference.Tagged: // (e.g., "docker.io/library/busybox:latest", "docker.io/library/busybox:latest@sha256:<digest>")
			s = v.Tag() // -> latest
		case reference.Digested: // (e.g., "docker.io@sha256:<digest>", "docker.io/library/busybo@sha256:<digest>")
			s = v.Digest().Hex() // -> <digest>
		case reference.Named: // (e.g., "docker.io/library/busybox")
			s = reference.Path(v) // -> library/busybox
		default:
		}
	}
	return TruncateString(s, maxLen)
}

// TruncateString truncates string to the first n characters.
// Pass n = -1 to skip truncation.
func TruncateString(s string, n int) string {
	if len(s) > n && n > 0 {
		s = s[:n]
	}
	return s
}

func CleanStringForDNS(s string) string {
	// Keep only letters, numbers, and dashes.
	re := regexp.MustCompile(`[^a-zA-Z0-9-]+`)
	// Lowercase to ensure RFC 1123 DNS label compliance for Kubernetes resource names.
	return strings.ToLower(re.ReplaceAllString(s, ResourceNameSeparator))
}

// Build ID is used as a label in k8s, and as the build ID for
// the worker in Temporal. That means it needs to conform to both
// system's requirements.
//
// https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#syntax-and-character-set
// Valid label value:
// - must be 63 characters or less (can be empty),
// - unless empty, must begin and end with an alphanumeric character ([a-z0-9A-Z]),
// - could contain dashes (-), underscores (_), dots (.), and alphanumerics between.
//
// Temporal build IDs only need to be ASCII.
func cleanBuildID(s string) string {
	// Keep only letters, numbers, dashes, underscores, and dots.
	re := regexp.MustCompile(`[^a-zA-Z0-9-._]+`)
	s = re.ReplaceAllString(s, ResourceNameSeparator)
	// Trim leading/trailing separators to comply with K8s label requirements
	// (must begin and end with alphanumeric character)
	return strings.Trim(s, "-._")
}

// ComputeSelectorLabels returns the selector labels used by a versioned Deployment.
// These are the same labels set on the Deployment.Spec.Selector.MatchLabels.
func ComputeSelectorLabels(twdName, buildID string) map[string]string {
	return map[string]string{
		twdNameLabel: TruncateString(CleanStringForDNS(twdName), 63),
		BuildIDLabel: TruncateString(buildID, 63),
	}
}

// ComputeSelectorLabelsWithVariant returns the selector labels for a versioned
// Deployment of a TWD that declares variants: the standard pair plus the
// VariantLabel discriminator, so base and variant Deployments of one version
// have disjoint selectors while sharing the version-independent twdNameLabel.
// The twdNameLabel value stays the TWD name for every variant - that is what
// keeps the scale-subresource selector (TWDNameSelector) spanning all of them.
func ComputeSelectorLabelsWithVariant(twdName, buildID, variant string) map[string]string {
	labels := ComputeSelectorLabels(twdName, buildID)
	labels[VariantLabel] = TruncateString(CleanStringForDNS(variant), 63)
	return labels
}

// TWDNameSelector returns the label-selector string matching every pod managed by the
// named TemporalWorkerDeployment across all versions (the version-independent twdNameLabel,
// without the per-version BuildIDLabel). The controller writes this to status.selector for
// the scale subresource so a VPA or HPA targeting the TWD can discover its pods.
func TWDNameSelector(twdName string) string {
	return fmt.Sprintf("%s=%s", twdNameLabel, TruncateString(CleanStringForDNS(twdName), 63))
}

// NewDeploymentWithOwnerRef creates a new BASE deployment resource, including owner
// references. When the spec declares variants, the base Deployment additionally
// carries the VariantLabel=BaseVariantName discriminator in its selector and pod
// labels so variant Deployments of the same build never overlap with it; without
// variants the output is unchanged from before variants existed.
func NewDeploymentWithOwnerRef(
	typeMeta *metav1.TypeMeta,
	objectMeta *metav1.ObjectMeta,
	spec *temporaliov1alpha1.TemporalWorkerDeploymentSpec,
	workerDeploymentName string,
	buildID string,
	connection temporaliov1alpha1.TemporalConnectionSpec,
) *appsv1.Deployment {
	return newVersionedDeployment(typeMeta, objectMeta, spec, workerDeploymentName, buildID, connection, nil)
}

// NewVariantDeploymentWithOwnerRef creates the child Deployment for one
// spec.variants entry of a version: the base template plus the variant's bounded
// delta, registering the SAME {workerDeploymentName, buildID} as the base but
// polling its own (suffixed) task queue.
func NewVariantDeploymentWithOwnerRef(
	typeMeta *metav1.TypeMeta,
	objectMeta *metav1.ObjectMeta,
	spec *temporaliov1alpha1.TemporalWorkerDeploymentSpec,
	workerDeploymentName string,
	buildID string,
	connection temporaliov1alpha1.TemporalConnectionSpec,
	variant *temporaliov1alpha1.WorkerVariant,
) *appsv1.Deployment {
	return newVersionedDeployment(typeMeta, objectMeta, spec, workerDeploymentName, buildID, connection, variant)
}

// newVersionedDeployment is the shared builder behind the base and variant
// constructors. variant == nil builds the base.
func newVersionedDeployment(
	typeMeta *metav1.TypeMeta,
	objectMeta *metav1.ObjectMeta,
	spec *temporaliov1alpha1.TemporalWorkerDeploymentSpec,
	workerDeploymentName string,
	buildID string,
	connection temporaliov1alpha1.TemporalConnectionSpec,
	variant *temporaliov1alpha1.WorkerVariant,
) *appsv1.Deployment {
	deploymentName := ComputeVersionedDeploymentName(objectMeta.Name, buildID)
	var selectorLabels map[string]string
	switch {
	case variant != nil:
		deploymentName = ComputeVersionedDeploymentName(objectMeta.Name+ResourceNameSeparator+variant.Name, buildID)
		selectorLabels = ComputeSelectorLabelsWithVariant(objectMeta.GetName(), buildID, variant.Name)
	case len(spec.Variants) > 0:
		// Base of a variant-declaring TWD: discriminate so variant selectors are disjoint.
		selectorLabels = ComputeSelectorLabelsWithVariant(objectMeta.GetName(), buildID, BaseVariantName)
	default:
		selectorLabels = ComputeSelectorLabels(objectMeta.GetName(), buildID)
	}

	// Set pod labels
	podLabels := make(map[string]string)
	for k, v := range spec.Template.Labels {
		podLabels[k] = v
	}
	for k, v := range selectorLabels {
		podLabels[k] = v
	}

	podSpec := spec.Template.Spec.DeepCopy()

	// Apply the variant's bounded delta BEFORE controller modifications so the
	// controller-managed env (TEMPORAL_*) can never be suffixed or overridden.
	if variant != nil {
		ApplyVariantDelta(podSpec, variant)
	}

	// Apply controller-managed environment variables and volume mounts
	ApplyControllerPodSpecModifications(podSpec, connection, spec.WorkerOptions.TemporalNamespace, workerDeploymentName, buildID)

	// Build pod annotations
	podAnnotations := make(map[string]string)
	for k, v := range spec.Template.Annotations {
		podAnnotations[k] = v
	}
	podAnnotations[ConnectionSpecHashAnnotation] = ComputeConnectionSpecHash(connection)
	// Store hash of user-provided pod template spec BEFORE controller modifications
	// This enables drift detection when build ID is stable
	podAnnotations[PodTemplateSpecHashAnnotation] = ComputePodTemplateSpecHash(spec.Template)
	if variant != nil {
		podAnnotations[VariantSpecHashAnnotation] = ComputeVariantSpecHash(variant)
	}
	blockOwnerDeletion := true

	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:                       deploymentName,
			Namespace:                  objectMeta.Namespace,
			DeletionGracePeriodSeconds: nil,
			Labels:                     selectorLabels,
			Annotations:                spec.Template.Annotations,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion:         typeMeta.APIVersion,
				Kind:               typeMeta.Kind,
				Name:               objectMeta.Name,
				UID:                objectMeta.UID,
				BlockOwnerDeletion: &blockOwnerDeletion,
				Controller:         nil,
			}},
			// TODO(jlegrone): Add finalizer managed by the controller in order to prevent
			//                 deleting deployments that are still reachable.
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: spec.Replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: selectorLabels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      podLabels,
					Annotations: podAnnotations,
				},
				Spec: *podSpec,
			},
			MinReadySeconds: spec.MinReadySeconds,
		},
	}
}

// ApplyVariantDelta applies a WorkerVariant's bounded delta to a pod spec:
// placement fields REPLACE their base counterparts when set, Resources replaces
// the first container's resources, and the env vars named in EnvValueSuffixes
// get TaskQueueSuffix appended to their literal Values (valueFrom entries are
// untouched - there is no value to suffix).
func ApplyVariantDelta(podSpec *corev1.PodSpec, variant *temporaliov1alpha1.WorkerVariant) {
	if variant.Affinity != nil {
		podSpec.Affinity = variant.Affinity.DeepCopy()
	}
	if variant.NodeSelector != nil {
		podSpec.NodeSelector = variant.NodeSelector
	}
	if variant.Tolerations != nil {
		podSpec.Tolerations = variant.Tolerations
	}
	if variant.Resources != nil && len(podSpec.Containers) > 0 {
		podSpec.Containers[0].Resources = *variant.Resources.DeepCopy()
	}
	if variant.TaskQueueSuffix == "" || len(variant.EnvValueSuffixes) == 0 {
		return
	}
	suffixed := make(map[string]struct{}, len(variant.EnvValueSuffixes))
	for _, name := range variant.EnvValueSuffixes {
		suffixed[name] = struct{}{}
	}
	for i := range podSpec.Containers {
		for j, env := range podSpec.Containers[i].Env {
			if _, ok := suffixed[env.Name]; ok && env.Value != "" {
				podSpec.Containers[i].Env[j].Value = env.Value + variant.TaskQueueSuffix
			}
		}
	}
}

// ComputeVariantSpecHash hashes a WorkerVariant delta for drift detection on
// variant Deployments, mirroring ComputePodTemplateSpecHash for the base.
func ComputeVariantSpecHash(variant *temporaliov1alpha1.WorkerVariant) string {
	hasher := sha256.New()
	data, _ := json.Marshal(variant) // never errors for the WorkerVariant shape
	_, _ = hasher.Write(data)
	return hex.EncodeToString(hasher.Sum(nil))
}

// TODO (Shivam): Change hash when secret name is updated as well.
func ComputeConnectionSpecHash(connection temporaliov1alpha1.TemporalConnectionSpec) string {
	// HostPort is required, but MutualTLSSecret can be empty for non-mTLS connections
	if connection.HostPort == "" {
		return ""
	}

	hasher := sha256.New()

	// Hash connection spec fields in deterministic order
	_, _ = hasher.Write([]byte(connection.HostPort))
	if connection.MutualTLSSecretRef != nil {
		_, _ = hasher.Write([]byte(connection.MutualTLSSecretRef.Name))
	} else if connection.APIKeySecretRef != nil {
		_, _ = hasher.Write([]byte(connection.APIKeySecretRef.Name))
	}

	return hex.EncodeToString(hasher.Sum(nil))
}

// ComputePodTemplateSpecHash computes a SHA256 hash of the user-provided pod template spec.
// This hash is used to detect drift when the build ID is stable but the pod spec has changed.
// JSON marshaling is used so that new zero-value fields added in future k8s API versions
// (which carry omitempty) are excluded, keeping hashes stable across k8s upgrades.
func ComputePodTemplateSpecHash(template corev1.PodTemplateSpec) string {
	hasher := sha256.New()
	data, _ := json.Marshal(template) // never errors for corev1.PodTemplateSpec
	_, _ = hasher.Write(data)
	return hex.EncodeToString(hasher.Sum(nil))
}

// ApplyControllerPodSpecModifications applies controller-managed environment variables and
// volume mounts to a pod spec. This is used both when creating new deployments and when
// updating existing deployments for drift detection.
func ApplyControllerPodSpecModifications(
	podSpec *corev1.PodSpec,
	connection temporaliov1alpha1.TemporalConnectionSpec,
	temporalNamespace string,
	workerDeploymentName string,
	buildID string,
) {
	// Add environment variables to containers
	for i, container := range podSpec.Containers {
		container.Env = append(container.Env,
			corev1.EnvVar{
				Name:  "TEMPORAL_ADDRESS",
				Value: connection.HostPort,
			},
			corev1.EnvVar{
				Name:  "TEMPORAL_NAMESPACE",
				Value: temporalNamespace,
			},
			corev1.EnvVar{
				Name:  TemporalDeploymentNameEnvVar,
				Value: workerDeploymentName,
			},
			corev1.EnvVar{
				Name:  "TEMPORAL_WORKER_BUILD_ID",
				Value: buildID,
			},
		)
		podSpec.Containers[i] = container
	}

	// Add TLS config if mTLS is enabled
	if connection.MutualTLSSecretRef != nil {
		for i, container := range podSpec.Containers {
			container.Env = append(container.Env,
				corev1.EnvVar{
					Name:  "TEMPORAL_TLS",
					Value: "true",
				},
				corev1.EnvVar{
					Name:  "TEMPORAL_TLS_CLIENT_KEY_PATH",
					Value: "/etc/temporal/tls/tls.key",
				},
				corev1.EnvVar{
					Name:  "TEMPORAL_TLS_CLIENT_CERT_PATH",
					Value: "/etc/temporal/tls/tls.crt",
				},
			)
			container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
				Name:      "temporal-tls",
				MountPath: "/etc/temporal/tls",
			})
			podSpec.Containers[i] = container
		}
		podSpec.Volumes = append(podSpec.Volumes, corev1.Volume{
			Name: "temporal-tls",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: connection.MutualTLSSecretRef.Name,
				},
			},
		})
	} else if connection.APIKeySecretRef != nil {
		for i, container := range podSpec.Containers {
			container.Env = append(container.Env,
				corev1.EnvVar{
					Name: "TEMPORAL_API_KEY",
					ValueFrom: &corev1.EnvVarSource{
						SecretKeyRef: connection.APIKeySecretRef,
					},
				},
			)
			podSpec.Containers[i] = container
		}
	}
}

func NewDeploymentWithControllerRef(
	w *temporaliov1alpha1.TemporalWorkerDeployment,
	buildID string,
	connection temporaliov1alpha1.TemporalConnectionSpec,
	reconcilerScheme *runtime.Scheme,
) (*appsv1.Deployment, error) {
	d := NewDeploymentWithOwnerRef(
		&w.TypeMeta,
		&w.ObjectMeta,
		&w.Spec,
		ComputeWorkerDeploymentName(w),
		buildID,
		connection,
	)
	if err := ctrl.SetControllerReference(w, d, reconcilerScheme); err != nil {
		return nil, err
	}
	return d, nil
}

// NewVariantDeploymentWithControllerRef builds a variant child Deployment (see
// NewVariantDeploymentWithOwnerRef) with the controller reference set.
func NewVariantDeploymentWithControllerRef(
	w *temporaliov1alpha1.TemporalWorkerDeployment,
	buildID string,
	connection temporaliov1alpha1.TemporalConnectionSpec,
	variant *temporaliov1alpha1.WorkerVariant,
	reconcilerScheme *runtime.Scheme,
) (*appsv1.Deployment, error) {
	d := NewVariantDeploymentWithOwnerRef(
		&w.TypeMeta,
		&w.ObjectMeta,
		&w.Spec,
		ComputeWorkerDeploymentName(w),
		buildID,
		connection,
		variant,
	)
	if err := ctrl.SetControllerReference(w, d, reconcilerScheme); err != nil {
		return nil, err
	}
	return d, nil
}
