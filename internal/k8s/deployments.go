// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2024 Datadog, Inc.

package k8s

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/davecgh/go-spew/spew"
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
	BuildIDLabel = "temporal.io/build-id"
	// PoolLabel identifies which pool a managed Deployment belongs to. For
	// single-pool (no spec.pools) TemporalWorkerDeployments this label is absent,
	// preserving byte-identical behavior with pre-pools deployments.
	PoolLabel                     = "temporal.io/pool"
	twdNameLabel                  = "temporal.io/deployment-name"
	WorkerDeploymentNameSeparator = "/"
	ResourceNameSeparator         = "-"
	MaxBuildIdLen                 = 63
	ConnectionSpecHashAnnotation  = "temporal.io/connection-spec-hash"
	PodTemplateSpecHashAnnotation = "temporal.io/pod-template-spec-hash"
)

// DeploymentState represents the Kubernetes state of all deployments for a temporal worker deployment
type DeploymentState struct {
	// Map of buildID to deployment.
	//
	// For single-pool deployments this is the (only) Deployment for the build.
	// For multi-pool deployments this points at the build's "primary" pool
	// Deployment (the first pool by sorted name) so that all the existing
	// single-pool code paths (status refs, scaling, deletion) keep working;
	// the full per-pool set lives in DeploymentsByVersionPool.
	Deployments map[string]*appsv1.Deployment
	// Sorted deployments by creation time
	DeploymentsByTime []*appsv1.Deployment
	// Map of buildID to deployment references (primary pool, see above)
	DeploymentRefs map[string]*corev1.ObjectReference
	// DeploymentsByVersionPool maps buildID -> poolName -> Deployment for every
	// managed Deployment, across all pools of a version. For single-pool
	// deployments there is exactly one entry per build keyed by the empty pool
	// name "".
	DeploymentsByVersionPool map[string]map[string]*appsv1.Deployment
}

// PoolDeployments returns all Deployments for the given buildID across pools,
// keyed by pool name (empty string == implicit single pool).
//
// It is safe to call when the build is unknown (returns nil). As a robustness
// fallback (e.g. for hand-constructed DeploymentState in tests that only set
// Deployments), if the per-pool map has no entry for the build but the flat
// Deployments map does, it synthesizes a single implicit-pool entry.
func (s *DeploymentState) PoolDeployments(buildID string) map[string]*appsv1.Deployment {
	if s == nil {
		return nil
	}
	if byPool, ok := s.DeploymentsByVersionPool[buildID]; ok && len(byPool) > 0 {
		return byPool
	}
	if d, ok := s.Deployments[buildID]; ok {
		return map[string]*appsv1.Deployment{ImplicitPoolNameKey: d}
	}
	return nil
}

// ImplicitPoolNameKey is the per-pool map key used for the single implicit pool.
const ImplicitPoolNameKey = ""

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
		Deployments:              make(map[string]*appsv1.Deployment),
		DeploymentsByTime:        []*appsv1.Deployment{},
		DeploymentRefs:           make(map[string]*corev1.ObjectReference),
		DeploymentsByVersionPool: make(map[string]map[string]*appsv1.Deployment),
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

	// Track each k8s deployment by build ID and pool.
	for i := range childDeploys.Items {
		deploy := &childDeploys.Items[i]
		buildID, ok := deploy.GetLabels()[BuildIDLabel]
		if !ok {
			// Any deployments without the build ID label are ignored
			continue
		}
		state.DeploymentsByTime = append(state.DeploymentsByTime, deploy)

		// Pool name is empty ("") for single-pool (pre-pools) deployments.
		poolName := deploy.GetLabels()[PoolLabel]
		if state.DeploymentsByVersionPool[buildID] == nil {
			state.DeploymentsByVersionPool[buildID] = make(map[string]*appsv1.Deployment)
		}
		state.DeploymentsByVersionPool[buildID][poolName] = deploy
	}

	// Choose a deterministic "primary" pool Deployment per build for the
	// single-pool-shaped maps consumed by status/scale/delete code. For
	// single-pool deployments this is simply the only Deployment.
	for buildID, byPool := range state.DeploymentsByVersionPool {
		primary := primaryPoolDeployment(byPool)
		if primary != nil {
			state.Deployments[buildID] = primary
			state.DeploymentRefs[buildID] = NewObjectRef(primary)
		}
	}

	return state, nil
}

// primaryPoolDeployment returns a deterministic Deployment from a build's
// per-pool map: the one whose pool name sorts first.
func primaryPoolDeployment(byPool map[string]*appsv1.Deployment) *appsv1.Deployment {
	if len(byPool) == 0 {
		return nil
	}
	poolNames := make([]string, 0, len(byPool))
	for name := range byPool {
		poolNames = append(poolNames, name)
	}
	sort.Strings(poolNames)
	return byPool[poolNames[0]]
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
			return TruncateString(cleaned, MaxBuildIdLen)
		}
		// Fall through to default hash-based generation if buildID is invalid after cleaning
	}

	if containers := w.Spec.Template.Spec.Containers; len(containers) > 0 {
		if img := containers[0].Image; img != "" {
			shortHashSuffix := ResourceNameSeparator + utils.ComputeHash(&w.Spec.Template, nil, true)
			maxImgLen := MaxBuildIdLen - len(shortHashSuffix)
			imagePrefix := computeImagePrefix(img, maxImgLen)
			return cleanBuildID(imagePrefix + shortHashSuffix)
		}
	}
	return utils.ComputeHash(&w.Spec.Template, nil, false)
}

// ComputeWorkerDeploymentName generates the base worker deployment name
func ComputeWorkerDeploymentName(w *temporaliov1alpha1.TemporalWorkerDeployment) string {
	// Use the name and namespace to form the worker deployment name
	return w.GetNamespace() + WorkerDeploymentNameSeparator + w.GetName()
}

// ComputeVersionedDeploymentName generates a name for a versioned deployment.
// This is the single-pool name and is unchanged from pre-pools behavior.
func ComputeVersionedDeploymentName(baseName, buildID string) string {
	return CleanStringForDNS(baseName + ResourceNameSeparator + buildID)
}

// ComputeVersionedPoolDeploymentName generates the k8s Deployment name for a
// (version, pool) pair. For the implicit pool (poolName == "") it returns
// exactly ComputeVersionedDeploymentName so single-pool deployment names are
// byte-identical to pre-pools behavior.
func ComputeVersionedPoolDeploymentName(baseName, buildID, poolName string) string {
	if poolName == temporaliov1alpha1.ImplicitPoolName {
		return ComputeVersionedDeploymentName(baseName, buildID)
	}
	return CleanStringForDNS(baseName + ResourceNameSeparator + buildID + ResourceNameSeparator + poolName)
}

// ResolvePoolByName returns the PoolSpec with the given name from the spec's
// effective pools. If poolName is not found (e.g. a pool removed from spec but
// still present in k8s), it returns a PoolSpec with just that name so callers
// still get deterministic behavior.
func ResolvePoolByName(spec *temporaliov1alpha1.TemporalWorkerDeploymentSpec, poolName string) temporaliov1alpha1.PoolSpec {
	for _, p := range spec.EffectivePools() {
		if p.Name == poolName {
			return p
		}
	}
	return temporaliov1alpha1.PoolSpec{Name: poolName}
}

// ApplyPoolPodSpecModifications applies a pool's placement/sizing overrides and
// then the controller-managed env/volumes (including the pool task queue) to a
// pod spec in place. It is the update-path equivalent of NewDeploymentForPool's
// pod-spec construction.
func ApplyPoolPodSpecModifications(
	podSpec *corev1.PodSpec,
	connection temporaliov1alpha1.TemporalConnectionSpec,
	temporalNamespace string,
	workerDeploymentName string,
	buildID string,
	pool temporaliov1alpha1.PoolSpec,
) {
	applyPoolPodPlacement(podSpec, pool)
	applyControllerPodSpecModifications(podSpec, connection, temporalNamespace, workerDeploymentName, buildID, pool.TaskQueue)
}

// applyPoolPodPlacement applies a pool's placement and sizing overrides to a
// pod spec in place. Nil/empty overrides leave the corresponding base-template
// field untouched.
func applyPoolPodPlacement(podSpec *corev1.PodSpec, pool temporaliov1alpha1.PoolSpec) {
	if pool.NodeSelector != nil {
		podSpec.NodeSelector = pool.NodeSelector
	}
	if pool.Affinity != nil {
		podSpec.Affinity = pool.Affinity
	}
	if pool.Tolerations != nil {
		podSpec.Tolerations = pool.Tolerations
	}
	if pool.Resources != nil {
		for i := range podSpec.Containers {
			podSpec.Containers[i].Resources = *pool.Resources
		}
	}
}

// poolReplicas returns the replica count to use for a pool: the pool override
// when set, otherwise the spec-level replicas.
func poolReplicas(spec *temporaliov1alpha1.TemporalWorkerDeploymentSpec, pool temporaliov1alpha1.PoolSpec) *int32 {
	if pool.Replicas != nil {
		return pool.Replicas
	}
	return spec.Replicas
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
	return re.ReplaceAllString(s, ResourceNameSeparator)
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

// NewDeploymentWithOwnerRef creates a new deployment resource for the implicit
// (single) pool, including owner references. Preserved for backward compatibility;
// it delegates to NewDeploymentForPool with the implicit pool.
func NewDeploymentWithOwnerRef(
	typeMeta *metav1.TypeMeta,
	objectMeta *metav1.ObjectMeta,
	spec *temporaliov1alpha1.TemporalWorkerDeploymentSpec,
	workerDeploymentName string,
	buildID string,
	connection temporaliov1alpha1.TemporalConnectionSpec,
) *appsv1.Deployment {
	return NewDeploymentForPool(
		typeMeta,
		objectMeta,
		spec,
		workerDeploymentName,
		buildID,
		connection,
		temporaliov1alpha1.PoolSpec{Name: temporaliov1alpha1.ImplicitPoolName},
	)
}

// NewDeploymentForPool creates a new k8s Deployment for a single (version, pool)
// pair, including owner references. For the implicit pool (pool.Name == "") the
// result is byte-identical to the pre-pools behavior: no pool label, no
// TEMPORAL_TASK_QUEUE env var, base template placement/resources, and the
// pre-pools Deployment name.
func NewDeploymentForPool(
	typeMeta *metav1.TypeMeta,
	objectMeta *metav1.ObjectMeta,
	spec *temporaliov1alpha1.TemporalWorkerDeploymentSpec,
	workerDeploymentName string,
	buildID string,
	connection temporaliov1alpha1.TemporalConnectionSpec,
	pool temporaliov1alpha1.PoolSpec,
) *appsv1.Deployment {
	selectorLabels := map[string]string{
		twdNameLabel: TruncateString(CleanStringForDNS(objectMeta.GetName()), 63),
		BuildIDLabel: TruncateString(buildID, 63),
	}
	// Only add the pool label for real (named) pools, so single-pool Deployments
	// keep their original selector and remain byte-identical to pre-pools.
	if pool.Name != temporaliov1alpha1.ImplicitPoolName {
		selectorLabels[PoolLabel] = TruncateString(CleanStringForDNS(pool.Name), 63)
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

	// Apply pool-specific placement/sizing BEFORE controller env injection so
	// that resource overrides land on the user containers.
	applyPoolPodPlacement(podSpec, pool)

	// Apply controller-managed environment variables and volume mounts (and the
	// pool's task queue, if any).
	applyControllerPodSpecModifications(podSpec, connection, spec.WorkerOptions.TemporalNamespace, workerDeploymentName, buildID, pool.TaskQueue)

	// Build pod annotations
	podAnnotations := make(map[string]string)
	for k, v := range spec.Template.Annotations {
		podAnnotations[k] = v
	}
	podAnnotations[ConnectionSpecHashAnnotation] = ComputeConnectionSpecHash(connection)
	// Store hash of user-provided pod template spec BEFORE controller modifications
	// This enables drift detection when build ID is stable
	podAnnotations[PodTemplateSpecHashAnnotation] = ComputePodTemplateSpecHash(spec.Template)
	blockOwnerDeletion := true

	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:                       ComputeVersionedPoolDeploymentName(objectMeta.Name, buildID, pool.Name),
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
			Replicas: poolReplicas(spec, pool),
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
// The hash captures ALL user-controllable fields in the pod template spec.
func ComputePodTemplateSpecHash(template corev1.PodTemplateSpec) string {
	hasher := sha256.New()

	// Use spew to get a deterministic string representation of the entire struct.
	// This captures ALL fields including env vars, commands, volumes, etc.
	// The config MUST NOT be changed because that could change the result of a hash operation.
	printer := &spew.ConfigState{
		Indent:                  " ",
		SortKeys:                true,
		DisableMethods:          true,
		SpewKeys:                true,
		DisablePointerAddresses: true,
		DisableCapacities:       true,
	}

	_, _ = hasher.Write([]byte(printer.Sprintf("%#v", template)))

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
	applyControllerPodSpecModifications(podSpec, connection, temporalNamespace, workerDeploymentName, buildID, "")
}

// applyControllerPodSpecModifications is the pool-aware implementation. When
// taskQueue is non-empty, it injects a TEMPORAL_TASK_QUEUE env var so the
// worker polls the pool's queue. When empty (single implicit pool) no such env
// var is added, preserving byte-identical behavior with pre-pools deployments.
func applyControllerPodSpecModifications(
	podSpec *corev1.PodSpec,
	connection temporaliov1alpha1.TemporalConnectionSpec,
	temporalNamespace string,
	workerDeploymentName string,
	buildID string,
	taskQueue string,
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
				Name:  "TEMPORAL_DEPLOYMENT_NAME",
				Value: workerDeploymentName,
			},
			corev1.EnvVar{
				Name:  "TEMPORAL_WORKER_BUILD_ID",
				Value: buildID,
			},
		)
		if taskQueue != "" {
			container.Env = append(container.Env, corev1.EnvVar{
				Name:  "TEMPORAL_TASK_QUEUE",
				Value: taskQueue,
			})
		}
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

// NewPoolDeploymentsWithControllerRef builds one k8s Deployment per effective
// pool for the given build, all sharing the worker deployment name and build ID.
// For a TWD without spec.pools this returns exactly one Deployment, identical to
// NewDeploymentWithControllerRef.
func NewPoolDeploymentsWithControllerRef(
	w *temporaliov1alpha1.TemporalWorkerDeployment,
	buildID string,
	connection temporaliov1alpha1.TemporalConnectionSpec,
	reconcilerScheme *runtime.Scheme,
) ([]*appsv1.Deployment, error) {
	var deployments []*appsv1.Deployment
	for _, pool := range w.Spec.EffectivePools() {
		d := NewDeploymentForPool(
			&w.TypeMeta,
			&w.ObjectMeta,
			&w.Spec,
			ComputeWorkerDeploymentName(w),
			buildID,
			connection,
			pool,
		)
		if err := ctrl.SetControllerReference(w, d, reconcilerScheme); err != nil {
			return nil, err
		}
		deployments = append(deployments, d)
	}
	return deployments, nil
}
