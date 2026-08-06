// Copyright 2025 The Atlan Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"
	temporaliov1alpha1 "github.com/temporalio/temporal-worker-controller/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

func TestScaledObjectName(t *testing.T) {
	cases := []struct {
		name    string
		twdName string
		buildID string
		// We can't always assert exact name when length cap kicks in, so we
		// check properties: bounded length, deterministic, includes prefix.
		expectExact  string
		expectPrefix string
		maxLen       int
	}{
		{
			name:        "short names — exact format",
			twdName:     "publish-worker-twd",
			buildID:     "main-4c9e7dc",
			expectExact: "publish-worker-twd-main-4c9e7dc-scale",
		},
		{
			name:        "minimal — exact",
			twdName:     "a",
			buildID:     "b",
			expectExact: "a-b-scale",
		},
		{
			name:         "long buildID — hashed, bounded length",
			twdName:      "publish-worker-twd",
			buildID:      "very-long-build-id-that-pushes-past-the-63-char-dns-label-limit-and-then-some",
			expectPrefix: "publish-worker-twd-",
			maxLen:       63,
		},
		{
			name:         "long twdName + buildID — both truncated, suffix preserved",
			twdName:      "exceeding-the-prefix-budget-significantly-with-a-very-long-name",
			buildID:      "another-very-long-build-id-string-causing-overflow",
			expectPrefix: "",
			maxLen:       63,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ScaledObjectName(tc.twdName, "", tc.buildID)
			assert.LessOrEqual(t, len(got), 63, "name must fit DNS label limit")
			if tc.expectExact != "" {
				assert.Equal(t, tc.expectExact, got)
			}
			if tc.expectPrefix != "" {
				assert.True(t, len(got) > len(tc.expectPrefix),
					"name should be longer than prefix")
			}
			// Deterministic: same inputs → same name.
			again := ScaledObjectName(tc.twdName, "", tc.buildID)
			assert.Equal(t, got, again, "name must be deterministic")
		})
	}
}

// A base and its variants share one buildID by design, so the buildID alone
// cannot distinguish their ScaledObject names. Once the TWD name alone fills
// the prefix budget the variant name is truncated away, and every variant of a
// version resolves to the same name as the base.
func TestScaledObjectNameVariantsDistinctWhenTruncated(t *testing.T) {
	const buildID = "master-a1b2c3d4e5f6-xk4p"

	for _, twdName := range []string{
		"app-worker-twd",
		"atlan-automation-engine-worker-default-normal-prio",
		"atlan-automation-engine-worker-default-normal-priority-queue",
	} {
		t.Run(twdName, func(t *testing.T) {
			base := ScaledObjectName(twdName, "", buildID)
			od := ScaledObjectName(twdName, "od", buildID)
			spot := ScaledObjectName(twdName, "spot", buildID)

			assert.NotEqual(t, base, od, "base and od variant must not share an SO name")
			assert.NotEqual(t, od, spot, "two variants must not share an SO name")
			assert.NotEqual(t, base, spot, "base and spot variant must not share an SO name")

			for _, got := range []string{base, od, spot} {
				assert.LessOrEqual(t, len(got), scaledObjectMaxNameLen)
			}
			assert.Contains(t, od, "-od-", "variant name stays visible in the SO name")
			assert.Contains(t, spot, "-spot-", "variant name stays visible in the SO name")
		})
	}
}

func TestDuplicateStrings(t *testing.T) {
	assert.Empty(t, duplicateStrings(nil))
	assert.Empty(t, duplicateStrings([]string{"a", "b", "c"}))
	assert.Equal(t, []string{"a"}, duplicateStrings([]string{"a", "b", "a"}))
	assert.Equal(t, []string{"a"}, duplicateStrings([]string{"a", "a", "a"}),
		"a value repeated three times is reported once")
	assert.Equal(t, []string{"b", "a"}, duplicateStrings([]string{"a", "b", "b", "a"}),
		"duplicates come back in first-seen order")
}

func TestScaledObjectNameAlwaysEndsWithSuffix(t *testing.T) {
	cases := []struct {
		twd, build string
	}{
		{"a", "b"},
		{"publish-worker-twd", "main-1234567"},
		{"very-long-twd-name", "very-long-build-id-overflowing-the-budget"},
	}
	for _, tc := range cases {
		got := ScaledObjectName(tc.twd, "", tc.build)
		assert.True(t, len(got) > len(scaledObjectSuffix),
			"name %q should include the -scale suffix", got)
		assert.Equal(t, scaledObjectSuffix, got[len(got)-len(scaledObjectSuffix):],
			"name %q must end with %q", got, scaledObjectSuffix)
	}
}

func TestActiveVersionsForScaling_ExcludesDrained(t *testing.T) {
	dep := &corev1.ObjectReference{Name: "d"}
	status := &temporaliov1alpha1.TemporalWorkerDeploymentStatus{
		CurrentVersion: &temporaliov1alpha1.CurrentWorkerDeploymentVersion{
			BaseWorkerDeploymentVersion: temporaliov1alpha1.BaseWorkerDeploymentVersion{
				BuildID:    "current",
				Status:     temporaliov1alpha1.VersionStatusCurrent,
				Deployment: dep,
			},
		},
		TargetVersion: temporaliov1alpha1.TargetWorkerDeploymentVersion{
			BaseWorkerDeploymentVersion: temporaliov1alpha1.BaseWorkerDeploymentVersion{
				BuildID:    "target",
				Status:     temporaliov1alpha1.VersionStatusRamping,
				Deployment: dep,
			},
		},
		DeprecatedVersions: []*temporaliov1alpha1.DeprecatedWorkerDeploymentVersion{
			{
				BaseWorkerDeploymentVersion: temporaliov1alpha1.BaseWorkerDeploymentVersion{
					BuildID:    "drained-one",
					Status:     temporaliov1alpha1.VersionStatusDrained,
					Deployment: dep,
				},
			},
			{
				BaseWorkerDeploymentVersion: temporaliov1alpha1.BaseWorkerDeploymentVersion{
					BuildID:    "inactive-one",
					Status:     temporaliov1alpha1.VersionStatusInactive,
					Deployment: dep,
				},
			},
		},
	}

	got := activeVersionsForScaling(status)
	buildIDs := make([]string, 0, len(got))
	for _, v := range got {
		buildIDs = append(buildIDs, v.BuildID)
	}
	assert.ElementsMatch(t, []string{"current", "target", "inactive-one"}, buildIDs,
		"drained version should be excluded; current/target/inactive included")

	byID := make(map[string]versionRef, len(got))
	for _, v := range got {
		byID[v.BuildID] = v
	}
	assert.True(t, byID["target"].IsTarget, "target ref must be marked IsTarget")
	assert.False(t, byID["current"].IsTarget, "current ref must not be marked IsTarget")
	assert.False(t, byID["inactive-one"].IsTarget, "deprecated ref must not be marked IsTarget")
}

func TestActiveVersionsForScaling_TargetSameAsCurrent_NotDuplicated(t *testing.T) {
	dep := &corev1.ObjectReference{Name: "d"}
	status := &temporaliov1alpha1.TemporalWorkerDeploymentStatus{
		CurrentVersion: &temporaliov1alpha1.CurrentWorkerDeploymentVersion{
			BaseWorkerDeploymentVersion: temporaliov1alpha1.BaseWorkerDeploymentVersion{
				BuildID: "v1", Status: temporaliov1alpha1.VersionStatusCurrent, Deployment: dep,
			},
		},
		TargetVersion: temporaliov1alpha1.TargetWorkerDeploymentVersion{
			BaseWorkerDeploymentVersion: temporaliov1alpha1.BaseWorkerDeploymentVersion{
				BuildID: "v1", Status: temporaliov1alpha1.VersionStatusCurrent, Deployment: dep,
			},
		},
	}
	got := activeVersionsForScaling(status)
	assert.Len(t, got, 1, "target == current should not double-count")
	assert.Equal(t, "v1", got[0].BuildID)
}

func TestResolveMinReplicas(t *testing.T) {
	int32Ptr := func(v int32) *int32 { return &v }

	twdNoConfig := &temporaliov1alpha1.TemporalWorkerDeployment{}
	twdMin0 := &temporaliov1alpha1.TemporalWorkerDeployment{
		Spec: temporaliov1alpha1.TemporalWorkerDeploymentSpec{
			WorkerScaling: &temporaliov1alpha1.WorkerScalingConfig{MinReplicaCount: int32Ptr(0)},
		},
	}
	twdMin3 := &temporaliov1alpha1.TemporalWorkerDeployment{
		Spec: temporaliov1alpha1.TemporalWorkerDeploymentSpec{
			WorkerScaling: &temporaliov1alpha1.WorkerScalingConfig{MinReplicaCount: int32Ptr(3)},
		},
	}

	cases := []struct {
		name     string
		twd      *temporaliov1alpha1.TemporalWorkerDeployment
		status   temporaliov1alpha1.VersionStatus
		isTarget bool
		wantVal  int64
		wantSet  bool
	}{
		// Nothing set → omitted entirely (KEDA default applies)
		{"current, no config - omit", twdNoConfig, temporaliov1alpha1.VersionStatusCurrent, false, 0, false},
		{"drained, no config - omit", twdNoConfig, temporaliov1alpha1.VersionStatusDrained, false, 0, false},

		// Ramping/Inactive: warm-start invariant → at least 1
		{"ramping, no config - warm-start bumps to 1", twdNoConfig, temporaliov1alpha1.VersionStatusRamping, false, 1, true},
		{"inactive, no config - warm-start bumps to 1", twdNoConfig, temporaliov1alpha1.VersionStatusInactive, false, 1, true},
		{"ramping, user min=0 - warm-start still bumps to 1", twdMin0, temporaliov1alpha1.VersionStatusRamping, false, 1, true},

		// NotRegistered target: floored to 1 so a worker can start, poll
		// Temporal and register the build ID. Without this, a KEDA-managed
		// target reads 0 backlog and is held at 0 forever (never registers,
		// never promoted).
		{"notregistered target, no config - warm-start bumps to 1", twdNoConfig, temporaliov1alpha1.VersionStatusNotRegistered, true, 1, true},
		{"notregistered target, user min=0 - warm-start still bumps to 1", twdMin0, temporaliov1alpha1.VersionStatusNotRegistered, true, 1, true},
		{"notregistered target, user min=3 - uses 3", twdMin3, temporaliov1alpha1.VersionStatusNotRegistered, true, 3, true},

		// NotRegistered but NOT the target (e.g. deleted server-side, pending
		// cleanup): must NOT be pinned, or stale versions accumulate pods.
		{"notregistered non-target - omit", twdNoConfig, temporaliov1alpha1.VersionStatusNotRegistered, false, 0, false},

		// User-configured min: honored when above the warm-start floor
		{"current, user min=3 - uses 3", twdMin3, temporaliov1alpha1.VersionStatusCurrent, false, 3, true},
		{"ramping, user min=3 - uses 3 (above the warm-start floor of 1)", twdMin3, temporaliov1alpha1.VersionStatusRamping, false, 3, true},

		// Explicit 0 with user config: honored for non-ramping
		{"current, user min=0 - uses 0", twdMin0, temporaliov1alpha1.VersionStatusCurrent, false, 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotVal, gotSet := resolveMinReplicas(versionRef{Status: tc.status, IsTarget: tc.isTarget}, tc.twd)
			assert.Equal(t, tc.wantSet, gotSet, "set flag")
			if tc.wantSet {
				assert.Equal(t, tc.wantVal, gotVal)
			}
		})
	}
}

func TestResolveMaxReplicas(t *testing.T) {
	int32Ptr := func(v int32) *int32 { return &v }

	t.Run("no config — omitted", func(t *testing.T) {
		val, ok := resolveMaxReplicas(&temporaliov1alpha1.TemporalWorkerDeployment{})
		assert.False(t, ok)
		assert.Equal(t, int64(0), val)
	})

	t.Run("user max=15 — honored", func(t *testing.T) {
		twd := &temporaliov1alpha1.TemporalWorkerDeployment{
			Spec: temporaliov1alpha1.TemporalWorkerDeploymentSpec{
				WorkerScaling: &temporaliov1alpha1.WorkerScalingConfig{MaxReplicaCount: int32Ptr(15)},
			},
		}
		val, ok := resolveMaxReplicas(twd)
		assert.True(t, ok)
		assert.Equal(t, int64(15), val)
	})
}

func TestResolveTargetQueueSize(t *testing.T) {
	int32Ptr := func(v int32) *int32 { return &v }

	t.Run("no config — omitted", func(t *testing.T) {
		val, ok := resolveTargetQueueSize(&temporaliov1alpha1.TemporalWorkerDeployment{})
		assert.False(t, ok)
		assert.Equal(t, "", val)
	})

	t.Run("user target=7 — emitted as string", func(t *testing.T) {
		twd := &temporaliov1alpha1.TemporalWorkerDeployment{
			Spec: temporaliov1alpha1.TemporalWorkerDeploymentSpec{
				WorkerScaling: &temporaliov1alpha1.WorkerScalingConfig{TargetQueueSize: int32Ptr(7)},
			},
		}
		val, ok := resolveTargetQueueSize(twd)
		assert.True(t, ok)
		assert.Equal(t, "7", val)
	})
}

func TestIsDeploymentKEDAManaged(t *testing.T) {
	cases := []struct {
		name string
		dep  *appsv1.Deployment
		want bool
	}{
		{"nil deployment", nil, false},
		{"no labels", &appsv1.Deployment{}, false},
		{"empty labels", &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{}},
		}, false},
		{"managed label true", &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{ManagedByLabel: "true"}},
		}, true},
		{"managed label TRUE (case-insensitive)", &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{ManagedByLabel: "TRUE"}},
		}, true},
		{"managed label false", &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{ManagedByLabel: "false"}},
		}, false},
		{"unrelated label", &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"foo": "bar"}},
		}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, IsDeploymentKEDAManaged(tc.dep))
		})
	}
}

func TestBuildScaledObject_ShapeAndFields(t *testing.T) {
	int32Ptr := func(v int32) *int32 { return &v }
	twd := &temporaliov1alpha1.TemporalWorkerDeployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "publish",
			Namespace: "publish-app",
			UID:       "abc-uid",
		},
		Spec: temporaliov1alpha1.TemporalWorkerDeploymentSpec{
			WorkerOptions: temporaliov1alpha1.WorkerOptions{
				TemporalNamespace: "default",
			},
			WorkerScaling: &temporaliov1alpha1.WorkerScalingConfig{
				MinReplicaCount: int32Ptr(0),
				MaxReplicaCount: int32Ptr(7),
				TargetQueueSize: int32Ptr(5),
				TaskQueue:       "atlan-publish-production",
			},
		},
	}
	twd.SetGroupVersionKind(temporaliov1alpha1.GroupVersion.WithKind("TemporalWorkerDeployment"))

	v := versionRef{
		BuildID: "main-f085195",
		Status:  temporaliov1alpha1.VersionStatusRamping,
		Deployment: &corev1.ObjectReference{
			Name:      "publish-worker-twd-main-f085195",
			Namespace: "publish-app",
		},
	}

	so := buildScaledObject(twd, v, "temporal:7233")

	assert.Equal(t, "publish-main-f085195-scale", so.GetName())
	assert.Equal(t, "publish-app", so.GetNamespace())
	assert.Equal(t, "ScaledObject", so.GetKind())
	assert.Equal(t, "keda.sh/v1alpha1", so.GetAPIVersion())

	// Labels
	assert.Equal(t, "publish", so.GetLabels()[OwnerTWDLabel])
	assert.Equal(t, "main-f085195", so.GetLabels()[BuildIDLabel])

	// Owner reference
	ownerRefs := so.GetOwnerReferences()
	assert.Len(t, ownerRefs, 1)
	assert.Equal(t, "publish", ownerRefs[0].Name)
	assert.Equal(t, "TemporalWorkerDeployment", ownerRefs[0].Kind)
	assert.NotNil(t, ownerRefs[0].Controller)
	assert.True(t, *ownerRefs[0].Controller)

	// Spec shape: scaleTargetRef
	spec := so.Object["spec"].(map[string]interface{})
	scaleTargetRef := spec["scaleTargetRef"].(map[string]interface{})
	assert.Equal(t, "Deployment", scaleTargetRef["kind"])
	assert.Equal(t, "publish-worker-twd-main-f085195", scaleTargetRef["name"])

	// MinReplicaCount: ramping warm-start invariant → 1 (user min was 0, but ramping bumps it)
	assert.EqualValues(t, 1, spec["minReplicaCount"])

	// MaxReplicaCount: from WorkerScaling
	assert.EqualValues(t, 7, spec["maxReplicaCount"])

	// Trigger
	triggers := spec["triggers"].([]interface{})
	assert.Len(t, triggers, 1)
	trigger := triggers[0].(map[string]interface{})
	assert.Equal(t, "temporal", trigger["type"])
	meta := trigger["metadata"].(map[string]interface{})
	assert.Equal(t, "temporal:7233", meta["endpoint"])
	assert.Equal(t, "default", meta["namespace"])
	assert.Equal(t, "main-f085195", meta["workerDeploymentBuildId"])
	assert.Equal(t, "5", meta["targetQueueSize"])
	assert.Equal(t, "atlan-publish-production", meta["taskQueue"])
	assert.Equal(t, "publish-app/publish", meta["workerDeploymentName"])
	// includeRunningWorkflowCount is omitted when unset — KEDA's own default (true) applies.
	_, hasIRWC := meta["includeRunningWorkflowCount"]
	assert.False(t, hasIRWC, "includeRunningWorkflowCount should be omitted when unset")
}

func TestBuildScaledObject_OmitsUnsetFields(t *testing.T) {
	// No WorkerScaling at all → SO omits every optional field so KEDA's own
	// defaults apply. Current-status version has no warm-start floor, so min
	// is also omitted.
	twd := &temporaliov1alpha1.TemporalWorkerDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: "ns"},
		Spec: temporaliov1alpha1.TemporalWorkerDeploymentSpec{
			WorkerOptions: temporaliov1alpha1.WorkerOptions{TemporalNamespace: "default"},
		},
	}
	twd.SetGroupVersionKind(temporaliov1alpha1.GroupVersion.WithKind("TemporalWorkerDeployment"))
	v := versionRef{
		BuildID:    "v1",
		Status:     temporaliov1alpha1.VersionStatusCurrent,
		Deployment: &corev1.ObjectReference{Name: "foo-worker-twd-v1", Namespace: "ns"},
	}
	so := buildScaledObject(twd, v, "temporal:7233")
	spec := so.Object["spec"].(map[string]interface{})

	// Every optional ScaledObject-level field is absent.
	for _, k := range []string{
		"minReplicaCount", "maxReplicaCount", "idleReplicaCount",
		"pollingInterval", "cooldownPeriod", "initialCooldownPeriod",
		"fallback", "advanced",
	} {
		_, has := spec[k]
		assert.False(t, has, "spec.%s should be omitted when unset", k)
	}

	// Every optional trigger metadata field is absent.
	trigger := spec["triggers"].([]interface{})[0].(map[string]interface{})
	meta := trigger["metadata"].(map[string]interface{})
	for _, k := range []string{
		"targetQueueSize", "activationTargetQueueSize", "activitySlotsPerWorker",
		"gateSlotsOnRunningWorkflow", "queueTypes", "includeRunningWorkflowCount",
		"workflowTaskQueueForCount", "workerMetricsPort", "minConnectTimeout",
	} {
		_, has := meta[k]
		assert.False(t, has, "trigger.metadata.%s should be omitted when unset", k)
	}
}

func TestBuildScaledObject_FullScalingConfig(t *testing.T) {
	// Every field on WorkerScalingConfig (and ScalingFallback) flows through
	// to the generated SO. Confirms Tianchu's review #3 expansion is wired up
	// end-to-end. RawExtension Advanced block tested separately below.
	int32Ptr := func(v int32) *int32 { return &v }
	boolPtr := func(v bool) *bool { return &v }
	twd := &temporaliov1alpha1.TemporalWorkerDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: "publish", Namespace: "publish-app"},
		Spec: temporaliov1alpha1.TemporalWorkerDeploymentSpec{
			WorkerOptions: temporaliov1alpha1.WorkerOptions{TemporalNamespace: "default"},
			WorkerScaling: &temporaliov1alpha1.WorkerScalingConfig{
				MinReplicaCount:             int32Ptr(2),
				MaxReplicaCount:             int32Ptr(10),
				IdleReplicaCount:            int32Ptr(1),
				PollingInterval:             int32Ptr(30),
				CooldownPeriod:              int32Ptr(300),
				InitialCooldownPeriod:       int32Ptr(60),
				TargetQueueSize:             int32Ptr(5),
				ActivationTargetQueueSize:   int32Ptr(1),
				ActivitySlotsPerWorker:      int32Ptr(6),
				GateSlotsOnRunningWorkflow:  boolPtr(false),
				QueueTypes:                  []string{"workflow", "activity"},
				IncludeRunningWorkflowCount: boolPtr(false),
				WorkflowTaskQueueForCount:   "custom-wf-tq",
				WorkerMetricsPort:           int32Ptr(9464),
				MinConnectTimeout:           int32Ptr(10),
				Fallback: &temporaliov1alpha1.ScalingFallback{
					FailureThreshold: int32Ptr(3),
					Replicas:         int32Ptr(2),
					Behavior:         "static",
				},
			},
		},
	}
	twd.SetGroupVersionKind(temporaliov1alpha1.GroupVersion.WithKind("TemporalWorkerDeployment"))
	v := versionRef{
		BuildID:    "v1",
		Status:     temporaliov1alpha1.VersionStatusCurrent,
		Deployment: &corev1.ObjectReference{Name: "publish-worker-twd-v1", Namespace: "publish-app"},
	}
	so := buildScaledObject(twd, v, "temporal:7233")
	spec := so.Object["spec"].(map[string]interface{})

	// ScaledObject-level
	assert.EqualValues(t, 2, spec["minReplicaCount"])
	assert.EqualValues(t, 10, spec["maxReplicaCount"])
	assert.EqualValues(t, 1, spec["idleReplicaCount"])
	assert.EqualValues(t, 30, spec["pollingInterval"])
	assert.EqualValues(t, 300, spec["cooldownPeriod"])
	assert.EqualValues(t, 60, spec["initialCooldownPeriod"])

	fb := spec["fallback"].(map[string]interface{})
	assert.EqualValues(t, 3, fb["failureThreshold"])
	assert.EqualValues(t, 2, fb["replicas"])
	assert.Equal(t, "static", fb["behavior"])

	// Trigger metadata — all values are strings per KEDA convention.
	meta := spec["triggers"].([]interface{})[0].(map[string]interface{})["metadata"].(map[string]interface{})
	assert.Equal(t, "5", meta["targetQueueSize"])
	assert.Equal(t, "1", meta["activationTargetQueueSize"])
	assert.Equal(t, "6", meta["activitySlotsPerWorker"])
	assert.Equal(t, "false", meta["gateSlotsOnRunningWorkflow"])
	assert.Equal(t, "workflow,activity", meta["queueTypes"])
	assert.Equal(t, "false", meta["includeRunningWorkflowCount"])
	assert.Equal(t, "custom-wf-tq", meta["workflowTaskQueueForCount"])
	assert.Equal(t, "9464", meta["workerMetricsPort"])
	assert.Equal(t, "10", meta["minConnectTimeout"])
}

func TestBuildScaledObject_AdvancedRawExtensionPassthrough(t *testing.T) {
	// The Advanced block is forwarded verbatim as parsed JSON. Used to set
	// HPA behavior tuning without bumping the CRD.
	advRaw := []byte(`{"horizontalPodAutoscalerConfig":{"behavior":{"scaleDown":{"stabilizationWindowSeconds":120}}}}`)
	twd := &temporaliov1alpha1.TemporalWorkerDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: "ns"},
		Spec: temporaliov1alpha1.TemporalWorkerDeploymentSpec{
			WorkerOptions: temporaliov1alpha1.WorkerOptions{TemporalNamespace: "default"},
			WorkerScaling: &temporaliov1alpha1.WorkerScalingConfig{
				Advanced: &runtime.RawExtension{Raw: advRaw},
			},
		},
	}
	twd.SetGroupVersionKind(temporaliov1alpha1.GroupVersion.WithKind("TemporalWorkerDeployment"))
	v := versionRef{
		BuildID:    "v1",
		Status:     temporaliov1alpha1.VersionStatusCurrent,
		Deployment: &corev1.ObjectReference{Name: "foo-worker-twd-v1", Namespace: "ns"},
	}
	so := buildScaledObject(twd, v, "temporal:7233")
	spec := so.Object["spec"].(map[string]interface{})

	adv, ok := spec["advanced"].(map[string]interface{})
	assert.True(t, ok, "advanced block should be set")
	hpa := adv["horizontalPodAutoscalerConfig"].(map[string]interface{})
	behavior := hpa["behavior"].(map[string]interface{})
	scaleDown := behavior["scaleDown"].(map[string]interface{})
	assert.EqualValues(t, 120, scaleDown["stabilizationWindowSeconds"])
}

func TestBuildScaledObject_FallbackPartialFields(t *testing.T) {
	// Setting only FailureThreshold + Replicas (no Behavior) yields a
	// fallback block without the behavior key.
	int32Ptr := func(v int32) *int32 { return &v }
	twd := &temporaliov1alpha1.TemporalWorkerDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: "ns"},
		Spec: temporaliov1alpha1.TemporalWorkerDeploymentSpec{
			WorkerOptions: temporaliov1alpha1.WorkerOptions{TemporalNamespace: "default"},
			WorkerScaling: &temporaliov1alpha1.WorkerScalingConfig{
				Fallback: &temporaliov1alpha1.ScalingFallback{
					FailureThreshold: int32Ptr(5),
					Replicas:         int32Ptr(1),
				},
			},
		},
	}
	twd.SetGroupVersionKind(temporaliov1alpha1.GroupVersion.WithKind("TemporalWorkerDeployment"))
	v := versionRef{
		BuildID:    "v1",
		Status:     temporaliov1alpha1.VersionStatusCurrent,
		Deployment: &corev1.ObjectReference{Name: "foo-worker-twd-v1", Namespace: "ns"},
	}
	so := buildScaledObject(twd, v, "temporal:7233")
	spec := so.Object["spec"].(map[string]interface{})
	fb := spec["fallback"].(map[string]interface{})
	assert.EqualValues(t, 5, fb["failureThreshold"])
	assert.EqualValues(t, 1, fb["replicas"])
	_, hasBehavior := fb["behavior"]
	assert.False(t, hasBehavior, "behavior should be omitted when unset")
}

func TestBuildScaledObject_RampingWithoutConfig_StillWarmStarts(t *testing.T) {
	// Even with no WorkerScaling, a Ramping version must be pinned to min=1
	// (correctness invariant, not a default).
	twd := &temporaliov1alpha1.TemporalWorkerDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: "ns"},
		Spec: temporaliov1alpha1.TemporalWorkerDeploymentSpec{
			WorkerOptions: temporaliov1alpha1.WorkerOptions{TemporalNamespace: "default"},
		},
	}
	twd.SetGroupVersionKind(temporaliov1alpha1.GroupVersion.WithKind("TemporalWorkerDeployment"))
	v := versionRef{
		BuildID:    "v1",
		Status:     temporaliov1alpha1.VersionStatusRamping,
		Deployment: &corev1.ObjectReference{Name: "foo-worker-twd-v1", Namespace: "ns"},
	}
	so := buildScaledObject(twd, v, "temporal:7233")
	spec := so.Object["spec"].(map[string]interface{})
	assert.EqualValues(t, 1, spec["minReplicaCount"],
		"ramping versions must be pinned to at least 1 to receive new workflows")
}

func TestBuildScaledObject_CurrentVersionCatchesUnassignedBacklog(t *testing.T) {
	// Current AND Ramping versions' SOs must set selectAllActive +
	// selectUnversioned so their DescribeTaskQueueEnhanced queries catch
	// workflows that arrived but haven't been assigned to any worker yet
	// (the from-zero scale-up case in Temporal's worker-deployment-versioning
	// model — newly-queued workflows have no version search attribute until
	// a worker picks them up, and matching spools them in the default
	// partition with a syncMatchQueue target that per-build queries can't
	// see). Ramping needs this because the matching-side routing decision
	// can deterministically target Ramping for a fraction of new workflows
	// (per the routing %), and without the flag those tasks would sit in
	// the default partition with no scaling signal back to Ramping.
	// Other version statuses (Inactive/Draining/NotRegistered) stay strictly
	// per-build-scoped — they should never reach into the unversioned bucket.
	mk := func(status temporaliov1alpha1.VersionStatus) *unstructured.Unstructured {
		twd := &temporaliov1alpha1.TemporalWorkerDeployment{
			ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: "ns"},
			Spec: temporaliov1alpha1.TemporalWorkerDeploymentSpec{
				WorkerOptions: temporaliov1alpha1.WorkerOptions{TemporalNamespace: "default"},
				WorkerScaling: &temporaliov1alpha1.WorkerScalingConfig{
					TaskQueue: "atlan-foo-production",
				},
			},
		}
		twd.SetGroupVersionKind(temporaliov1alpha1.GroupVersion.WithKind("TemporalWorkerDeployment"))
		v := versionRef{
			BuildID:    "v1",
			Status:     status,
			Deployment: &corev1.ObjectReference{Name: "foo-worker-twd-v1", Namespace: "ns"},
		}
		return buildScaledObject(twd, v, "temporal:7233")
	}

	cases := []struct {
		name        string
		status      temporaliov1alpha1.VersionStatus
		wantAllActv bool
		wantUnvrsnd bool
	}{
		{"Current — must catch unassigned", temporaliov1alpha1.VersionStatusCurrent, true, true},
		{"Ramping — must catch unassigned (matching may route here)", temporaliov1alpha1.VersionStatusRamping, true, true},
		{"Inactive — per-build scoped", temporaliov1alpha1.VersionStatusInactive, false, false},
		{"Draining — per-build scoped", temporaliov1alpha1.VersionStatusDraining, false, false},
		{"NotRegistered — per-build scoped", temporaliov1alpha1.VersionStatusNotRegistered, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			so := mk(tc.status)
			spec := so.Object["spec"].(map[string]interface{})
			meta := spec["triggers"].([]interface{})[0].(map[string]interface{})["metadata"].(map[string]interface{})
			gotAllActv := meta["selectAllActive"] == "true"
			gotUnvrsnd := meta["selectUnversioned"] == "true"
			assert.Equal(t, tc.wantAllActv, gotAllActv, "selectAllActive for status %s", tc.status)
			assert.Equal(t, tc.wantUnvrsnd, gotUnvrsnd, "selectUnversioned for status %s", tc.status)
		})
	}
}

// triggerMetadataOf extracts the temporal trigger's metadata map from a built SO.
func triggerMetadataOf(t *testing.T, so *unstructured.Unstructured) map[string]interface{} {
	t.Helper()
	triggers, found, err := unstructured.NestedSlice(so.Object, "spec", "triggers")
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Len(t, triggers, 1)
	trigger := triggers[0].(map[string]interface{})
	return trigger["metadata"].(map[string]interface{})
}

// A version preserved across a spec.workerOptions.workerDeploymentName change
// registered its workers (and therefore its backlog + running pinned
// workflows) under the OLD name recorded on its Deployment. Its ScaledObject
// must query Temporal under that recorded name: aiming it at the
// newly-resolved name targets a version Temporal has never seen, the metrics
// read zero forever, KEDA holds the preserved Deployment at min replicas, and
// the version's pinned workflows starve — the Deployment-preservation guard in
// the planner (805cd49) alone does not keep them running.
func TestBuildScaledObject_RecordedWDNAimsTriggerAtOldName(t *testing.T) {
	twd := &temporaliov1alpha1.TemporalWorkerDeployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "publish-worker-twd",
			Namespace: "publish-app",
			UID:       "abc-uid",
		},
		Spec: temporaliov1alpha1.TemporalWorkerDeploymentSpec{
			WorkerOptions: temporaliov1alpha1.WorkerOptions{
				TemporalNamespace: "default",
				// The rename that strands old versions:
				WorkerDeploymentName: "publish-app/shared",
			},
			WorkerScaling: &temporaliov1alpha1.WorkerScalingConfig{
				TaskQueue: "atlan-publish-production",
			},
		},
	}
	twd.SetGroupVersionKind(temporaliov1alpha1.GroupVersion.WithKind("TemporalWorkerDeployment"))

	cases := []struct {
		name        string
		recordedWDN string
		wantTrigger string
	}{
		{
			// The incident shape: version created before the rename.
			name:        "recorded name differs from resolved -> trigger uses recorded",
			recordedWDN: "publish-app/publish-worker-twd",
			wantTrigger: "publish-app/publish-worker-twd",
		},
		{
			name:        "recorded name equals resolved -> trigger uses resolved",
			recordedWDN: "publish-app/shared",
			wantTrigger: "publish-app/shared",
		},
		{
			// Deployment unreadable (or pre-env-var Deployment): fall back to
			// resolved rather than guessing.
			name:        "recorded name empty -> trigger uses resolved",
			recordedWDN: "",
			wantTrigger: "publish-app/shared",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := versionRef{
				BuildID: "feat-publish-app-enable-pinned-04b0570",
				Status:  temporaliov1alpha1.VersionStatusNotRegistered,
				Deployment: &corev1.ObjectReference{
					Name:      "publish-worker-twd-feat-publish-app-enable-pinned-04b0570",
					Namespace: "publish-app",
				},
				RecordedWDN: tc.recordedWDN,
			}
			so := buildScaledObject(twd, v, "temporal:7233")
			meta := triggerMetadataOf(t, so)
			assert.Equal(t, tc.wantTrigger, meta["workerDeploymentName"])
			assert.Equal(t, "feat-publish-app-enable-pinned-04b0570", meta["workerDeploymentBuildId"])
		})
	}
}

// Guards the SO-preservation half of the rename story: a NotRegistered
// deprecated version (the classification a rename-orphaned version gets, since
// the controller only describes the currently-resolved name) must stay in the
// active-for-scaling set so its ScaledObject is kept and step 5 does not sweep
// it. Excluding it the way Drained versions are excluded would delete its SO,
// strip the keda-managed label, and leave the preserved Deployment frozen at
// whatever replica count it happened to have (possibly zero) with no scaler.
func TestActiveVersionsForScaling_KeepsNotRegisteredDeprecated(t *testing.T) {
	status := &temporaliov1alpha1.TemporalWorkerDeploymentStatus{
		CurrentVersion: &temporaliov1alpha1.CurrentWorkerDeploymentVersion{
			BaseWorkerDeploymentVersion: temporaliov1alpha1.BaseWorkerDeploymentVersion{
				BuildID:    "new-build",
				Status:     temporaliov1alpha1.VersionStatusCurrent,
				Deployment: &corev1.ObjectReference{Name: "d-new", Namespace: "publish-app"},
			},
		},
		DeprecatedVersions: []*temporaliov1alpha1.DeprecatedWorkerDeploymentVersion{
			{
				BaseWorkerDeploymentVersion: temporaliov1alpha1.BaseWorkerDeploymentVersion{
					BuildID:    "old-build-under-old-wdn",
					Status:     temporaliov1alpha1.VersionStatusNotRegistered,
					Deployment: &corev1.ObjectReference{Name: "d-old", Namespace: "publish-app"},
				},
			},
		},
	}

	out := activeVersionsForScaling(status)

	builds := make([]string, 0, len(out))
	for _, v := range out {
		builds = append(builds, v.BuildID)
	}
	assert.Contains(t, builds, "new-build")
	assert.Contains(t, builds, "old-build-under-old-wdn",
		"rename-orphaned (NotRegistered) versions must keep their ScaledObject")
}

// TestBuildScaledObject_SpecHashStableAndSensitive verifies the spec-hash
// annotation that applyDesiredScaledObject uses to skip redundant applies:
// it must be stable for identical input (so a converged SO is not re-applied
// every requeue) and change whenever a managed field changes (so a real
// change is still applied).
func TestBuildScaledObject_SpecHashStableAndSensitive(t *testing.T) {
	int32Ptr := func(v int32) *int32 { return &v }
	twd := &temporaliov1alpha1.TemporalWorkerDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: "publish", Namespace: "publish-app", UID: "abc-uid"},
		Spec: temporaliov1alpha1.TemporalWorkerDeploymentSpec{
			WorkerOptions: temporaliov1alpha1.WorkerOptions{TemporalNamespace: "default"},
			WorkerScaling: &temporaliov1alpha1.WorkerScalingConfig{
				MinReplicaCount: int32Ptr(1),
				MaxReplicaCount: int32Ptr(7),
				TaskQueue:       "atlan-publish-production",
			},
		},
	}
	twd.SetGroupVersionKind(temporaliov1alpha1.GroupVersion.WithKind("TemporalWorkerDeployment"))
	v := versionRef{
		BuildID: "main-f085195",
		Status:  temporaliov1alpha1.VersionStatusCurrent,
		Deployment: &corev1.ObjectReference{
			Name:      "publish-worker-twd-main-f085195",
			Namespace: "publish-app",
		},
	}
	hashOf := func(so *unstructured.Unstructured) string {
		return so.GetAnnotations()[specHashAnnotation]
	}

	base := hashOf(buildScaledObject(twd, v, "temporal:7233"))
	assert.Len(t, base, 40, "sha1 hex digest is 40 chars")

	// Stable: identical input yields an identical hash across builds.
	assert.Equal(t, base, hashOf(buildScaledObject(twd, v, "temporal:7233")))

	// Sensitive: a managed trigger field (endpoint) changes the hash.
	assert.NotEqual(t, base, hashOf(buildScaledObject(twd, v, "temporal:9999")))

	// Sensitive: a managed spec field (maxReplicas) changes the hash.
	twd.Spec.WorkerScaling.MaxReplicaCount = int32Ptr(99)
	assert.NotEqual(t, base, hashOf(buildScaledObject(twd, v, "temporal:7233")))
}
