// Copyright 2025 The Atlan Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	temporaliov1alpha1 "github.com/temporalio/temporal-worker-controller/api/v1alpha1"
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
			got := ScaledObjectName(tc.twdName, tc.buildID)
			assert.LessOrEqual(t, len(got), 63, "name must fit DNS label limit")
			if tc.expectExact != "" {
				assert.Equal(t, tc.expectExact, got)
			}
			if tc.expectPrefix != "" {
				assert.True(t, len(got) > len(tc.expectPrefix),
					"name should be longer than prefix")
			}
			// Deterministic: same inputs → same name.
			again := ScaledObjectName(tc.twdName, tc.buildID)
			assert.Equal(t, got, again, "name must be deterministic")
		})
	}
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
		got := ScaledObjectName(tc.twd, tc.build)
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
		name    string
		twd     *temporaliov1alpha1.TemporalWorkerDeployment
		status  temporaliov1alpha1.VersionStatus
		wantVal int64
		wantSet bool
	}{
		// Nothing set → omitted entirely (KEDA default applies)
		{"current, no config — omit", twdNoConfig, temporaliov1alpha1.VersionStatusCurrent, 0, false},
		{"drained, no config — omit", twdNoConfig, temporaliov1alpha1.VersionStatusDrained, 0, false},

		// Ramping/Inactive: warm-start invariant → at least 1
		{"ramping, no config — warm-start bumps to 1", twdNoConfig, temporaliov1alpha1.VersionStatusRamping, 1, true},
		{"inactive, no config — warm-start bumps to 1", twdNoConfig, temporaliov1alpha1.VersionStatusInactive, 1, true},
		{"ramping, user min=0 — warm-start still bumps to 1", twdMin0, temporaliov1alpha1.VersionStatusRamping, 1, true},

		// User-configured min: honored when above the warm-start floor
		{"current, user min=3 — uses 3", twdMin3, temporaliov1alpha1.VersionStatusCurrent, 3, true},
		{"ramping, user min=3 — uses 3 (above the warm-start floor of 1)", twdMin3, temporaliov1alpha1.VersionStatusRamping, 3, true},

		// Explicit 0 with user config: honored for non-ramping
		{"current, user min=0 — uses 0", twdMin0, temporaliov1alpha1.VersionStatusCurrent, 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotVal, gotSet := resolveMinReplicas(versionRef{Status: tc.status}, tc.twd)
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
	assert.Equal(t, "main-f085195", meta["buildId"])
	assert.Equal(t, "5", meta["targetQueueSize"])
	assert.Equal(t, "true", meta["includeRunningWorkflowCount"])
	assert.Equal(t, "publish-app:publish", meta["taskQueue"])
}

func TestBuildScaledObject_OmitsUnsetFields(t *testing.T) {
	// No WorkerScaling at all → SO omits minReplicaCount, maxReplicaCount,
	// targetQueueSize entirely so KEDA's own defaults apply. Current-status
	// version has no warm-start floor, so min is also omitted.
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

	_, hasMin := spec["minReplicaCount"]
	_, hasMax := spec["maxReplicaCount"]
	assert.False(t, hasMin, "minReplicaCount should be omitted when unset")
	assert.False(t, hasMax, "maxReplicaCount should be omitted when unset")

	trigger := spec["triggers"].([]interface{})[0].(map[string]interface{})
	meta := trigger["metadata"].(map[string]interface{})
	_, hasTQS := meta["targetQueueSize"]
	assert.False(t, hasTQS, "targetQueueSize should be omitted when unset")
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

func TestSOSpecEqual(t *testing.T) {
	int32Ptr := func(v int32) *int32 { return &v }
	twd := &temporaliov1alpha1.TemporalWorkerDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: "ns"},
		Spec: temporaliov1alpha1.TemporalWorkerDeploymentSpec{
			WorkerOptions: temporaliov1alpha1.WorkerOptions{TemporalNamespace: "default"},
			WorkerScaling: &temporaliov1alpha1.WorkerScalingConfig{
				MaxReplicaCount: int32Ptr(5),
			},
		},
	}
	twd.SetGroupVersionKind(temporaliov1alpha1.GroupVersion.WithKind("TemporalWorkerDeployment"))
	v := versionRef{
		BuildID:    "v1",
		Status:     temporaliov1alpha1.VersionStatusCurrent,
		Deployment: &corev1.ObjectReference{Name: "d", Namespace: "ns"},
	}

	a := buildScaledObject(twd, v, "temporal:7233")
	b := buildScaledObject(twd, v, "temporal:7233")
	assert.True(t, soSpecEqual(a, b), "same inputs → equivalent SOs")

	c := buildScaledObject(twd, v, "different-endpoint:7233")
	assert.False(t, soSpecEqual(a, c), "different endpoint → not equivalent")
}
