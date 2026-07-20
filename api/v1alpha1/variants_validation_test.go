// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2024 Datadog, Inc.

package v1alpha1

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidateVariants(t *testing.T) {
	withScaling := func(variants ...WorkerVariant) *TemporalWorkerDeploymentSpec {
		return &TemporalWorkerDeploymentSpec{
			WorkerScaling: &WorkerScalingConfig{TaskQueue: "q"},
			Variants:      variants,
		}
	}

	t.Run("valid od variant", func(t *testing.T) {
		errs := validateVariants(withScaling(WorkerVariant{Name: "od", TaskQueueSuffix: "-od"}))
		assert.Empty(t, errs)
	})

	t.Run("reserved name base", func(t *testing.T) {
		errs := validateVariants(withScaling(WorkerVariant{Name: "base"}))
		assert.Len(t, errs, 1)
	})

	t.Run("duplicate names", func(t *testing.T) {
		errs := validateVariants(withScaling(
			WorkerVariant{Name: "od", TaskQueueSuffix: "-od"},
			WorkerVariant{Name: "od", TaskQueueSuffix: "-od2"},
		))
		assert.Len(t, errs, 1)
	})

	t.Run("suffix without workerScaling.taskQueue", func(t *testing.T) {
		spec := &TemporalWorkerDeploymentSpec{
			Variants: []WorkerVariant{{Name: "od", TaskQueueSuffix: "-od"}},
		}
		errs := validateVariants(spec)
		assert.Len(t, errs, 1)
	})

	t.Run("empty suffix needs no taskQueue (same-queue standby)", func(t *testing.T) {
		spec := &TemporalWorkerDeploymentSpec{
			Variants: []WorkerVariant{{Name: "standby"}},
		}
		assert.Empty(t, validateVariants(spec))
	})

	t.Run("empty envValueSuffixes entry", func(t *testing.T) {
		errs := validateVariants(withScaling(WorkerVariant{Name: "od", TaskQueueSuffix: "-od", EnvValueSuffixes: []string{""}}))
		assert.Len(t, errs, 1)
	})
}
