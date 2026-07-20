// Copyright 2025 The Atlan Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	temporaliov1alpha1 "github.com/temporalio/temporal-worker-controller/api/v1alpha1"
	"github.com/temporalio/temporal-worker-controller/internal/k8s"
	"github.com/temporalio/temporal-worker-controller/internal/temporal"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func int32Ptr(i int32) *int32 { return &i }

func variantTWD() *temporaliov1alpha1.TemporalWorkerDeployment {
	return &temporaliov1alpha1.TemporalWorkerDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: "app-worker-twd", Namespace: "app-ns"},
		Spec: temporaliov1alpha1.TemporalWorkerDeploymentSpec{
			WorkerOptions: temporaliov1alpha1.WorkerOptions{TemporalNamespace: "default"},
			WorkerScaling: &temporaliov1alpha1.WorkerScalingConfig{
				TaskQueue:                 "atlan-app-production",
				MinReplicaCount:           int32Ptr(0),
				MaxReplicaCount:           int32Ptr(10),
				WorkflowTaskQueueForCount: "atlan-app-production",
			},
			Variants: []temporaliov1alpha1.WorkerVariant{{
				Name:            "od",
				TaskQueueSuffix: "-od",
				Scaling: &temporaliov1alpha1.VariantScaling{
					MaxReplicaCount: int32Ptr(3),
				},
			}},
		},
	}
}

func soTrigger(t *testing.T, so *unstructured.Unstructured) map[string]interface{} {
	t.Helper()
	triggers, found, err := unstructured.NestedSlice(so.Object, "spec", "triggers")
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, triggers, 1)
	md, ok := triggers[0].(map[string]interface{})["metadata"].(map[string]interface{})
	require.True(t, ok)
	return md
}

func TestBuildScaledObjectVariant(t *testing.T) {
	twd := variantTWD()
	ref := &corev1.ObjectReference{Name: "app-worker-twd-od-bid1", Namespace: "app-ns"}
	v := versionRef{
		BuildID:    "bid1",
		Status:     temporaliov1alpha1.VersionStatusCurrent,
		Deployment: ref,
		Variant:    &twd.Spec.Variants[0],
	}

	so := buildScaledObject(twd, v, "temporal:7233")

	assert.Equal(t, ScaledObjectName("app-worker-twd-od", "bid1"), so.GetName())
	assert.Equal(t, "od", so.GetLabels()[VariantSOLabel])

	md := soTrigger(t, so)
	assert.Equal(t, "atlan-app-production-od", md["taskQueue"], "variant SO watches the suffixed queue")
	assert.Equal(t, "atlan-app-production-od", md["workflowTaskQueueForCount"], "wtqfc follows the suffix")
	assert.Equal(t, "app-ns/app-worker-twd", md["workerDeploymentName"], "same version identity as base")
	assert.Equal(t, "bid1", md["workerDeploymentBuildId"])

	target, _, _ := unstructured.NestedString(so.Object, "spec", "scaleTargetRef", "name")
	assert.Equal(t, "app-worker-twd-od-bid1", target)

	maxR, _, _ := unstructured.NestedInt64(so.Object, "spec", "maxReplicaCount")
	assert.Equal(t, int64(3), maxR, "variant max override wins over workerScaling")

	// Current version: min floored per user config (0) - Current releases the floor.
	minR, found, _ := unstructured.NestedInt64(so.Object, "spec", "minReplicaCount")
	require.True(t, found)
	assert.Equal(t, int64(0), minR)

	// Base SO for the same version stays unsuffixed and unlabeled.
	base := buildScaledObject(twd, versionRef{BuildID: "bid1", Status: temporaliov1alpha1.VersionStatusCurrent, Deployment: ref}, "temporal:7233")
	baseMd := soTrigger(t, base)
	assert.Equal(t, "atlan-app-production", baseMd["taskQueue"])
	assert.Equal(t, "atlan-app-production", baseMd["workflowTaskQueueForCount"])
	_, hasLabel := base.GetLabels()[VariantSOLabel]
	assert.False(t, hasLabel)
}

func TestResolveMinReplicasVariantFloor(t *testing.T) {
	twd := variantTWD()
	v := versionRef{
		BuildID: "bid1",
		Status:  temporaliov1alpha1.VersionStatusRamping,
		Variant: &twd.Spec.Variants[0],
	}
	// Ramping keeps the >=1 floor even for a variant with min 0: its suffixed
	// queue must register before SetCurrentVersion.
	minR, ok := resolveMinReplicas(v, twd)
	require.True(t, ok)
	assert.Equal(t, int64(1), minR)

	// Once Current, the variant's own override applies.
	v.Status = temporaliov1alpha1.VersionStatusCurrent
	twd.Spec.Variants[0].Scaling.MinReplicaCount = int32Ptr(0)
	minR, ok = resolveMinReplicas(v, twd)
	require.True(t, ok)
	assert.Equal(t, int64(0), minR)
}

func TestVariantVersionsForScaling(t *testing.T) {
	twd := variantTWD()
	odRef := &corev1.ObjectReference{Name: "app-worker-twd-od-bid1"}
	status := &temporaliov1alpha1.TemporalWorkerDeploymentStatus{
		TargetVersion: temporaliov1alpha1.TargetWorkerDeploymentVersion{
			BaseWorkerDeploymentVersion: temporaliov1alpha1.BaseWorkerDeploymentVersion{
				BuildID:    "bid1",
				Status:     temporaliov1alpha1.VersionStatusRamping,
				Deployment: &corev1.ObjectReference{Name: "app-worker-twd-bid1"},
				Variants: []temporaliov1alpha1.VariantStatus{
					{Name: "od", Deployment: odRef},
					{Name: "removed", Deployment: &corev1.ObjectReference{Name: "app-worker-twd-removed-bid1"}},
				},
			},
		},
	}
	base := activeVersionsForScaling(status)
	require.Len(t, base, 1)

	got := variantVersionsForScaling(base, status, twd)
	require.Len(t, got, 1, "status variants not declared in spec are skipped (their SOs go stale)")
	assert.Equal(t, "od", got[0].Variant.Name)
	assert.Equal(t, odRef, got[0].Deployment)
	assert.True(t, got[0].IsTarget)
	assert.Equal(t, temporaliov1alpha1.VersionStatusRamping, got[0].Status)
}

func TestStateMapperVariants(t *testing.T) {
	base := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "w-bid1", Labels: map[string]string{k8s.BuildIDLabel: "bid1", k8s.VariantLabel: k8s.BaseVariantName}}}
	od := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "w-od-bid1", Labels: map[string]string{k8s.BuildIDLabel: "bid1", k8s.VariantLabel: "od"}},
		Status: appsv1.DeploymentStatus{
			Conditions: []appsv1.DeploymentCondition{{Type: appsv1.DeploymentAvailable, Status: corev1.ConditionTrue}},
		},
	}
	k8sState := &k8s.DeploymentState{
		Deployments:           map[string]*appsv1.Deployment{"bid1": base},
		DeploymentRefs:        map[string]*corev1.ObjectReference{"bid1": k8s.NewObjectRef(base)},
		VariantDeployments:    map[string]map[string]*appsv1.Deployment{"bid1": {"od": od}},
		VariantDeploymentRefs: map[string]map[string]*corev1.ObjectReference{"bid1": {"od": k8s.NewObjectRef(od)}},
	}
	m := newStateMapper(k8sState, &temporal.TemporalWorkerState{
		Versions: map[string]*temporal.VersionInfo{},
	}, "ns/w")

	target := m.mapTargetWorkerDeploymentVersionByBuildID("bid1")
	require.Len(t, target.Variants, 1)
	assert.Equal(t, "od", target.Variants[0].Name)
	assert.Equal(t, "w-od-bid1", target.Variants[0].Deployment.Name)
	assert.NotNil(t, target.Variants[0].HealthySince, "healthy variant records HealthySince")
	// Base health gating is untouched: target.HealthySince comes from the base only.
	assert.Nil(t, target.HealthySince)
}
