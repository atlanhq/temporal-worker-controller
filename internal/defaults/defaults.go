// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2024 Datadog, Inc.
package defaults

import "time"

// Default values for TemporalWorkerDeploymentSpec fields
const (
	ScaledownDelay                   = 1 * time.Hour
	DeleteDelay                      = 24 * time.Hour
	ServerMaxVersions                = 100
	MaxVersionsIneligibleForDeletion = int32(ServerMaxVersions * 0.75)

	// TeardownDrainageTimeout bounds how long TWD deletion waits for open pinned
	// executions before terminating them. Set to the intended workflow execution
	// ceiling, so a workflow that is still running is given its whole legal lifetime
	// rather than being cut short by an unrelated chart change. Note the ceiling is
	// not guaranteed: workflow_max_timeout_hours is unset by default in the app SDK,
	// which leaves the namespace default, so for those apps this budget is the only
	// thing bounding the wait - which is why it expires into a terminate rather than
	// into an unbounded hold.
	TeardownDrainageTimeout = 72 * time.Hour

	// ToBeDeprecatedDefaultControllerIdentity will stop being used in the next release.
	ToBeDeprecatedDefaultControllerIdentity = "temporal-worker-controller"
)
