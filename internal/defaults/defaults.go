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
	// executions before terminating them. Sized to outlast a collateral uninstall:
	// on 2026-08-31 Flux reinstalled 20 minutes after remediating its own timeout, and
	// terminating inside that window would end work the returning workers could have
	// finished. The cost of the longer wait is only that the object holds its name.
	TeardownDrainageTimeout = 30 * time.Minute

	// ToBeDeprecatedDefaultControllerIdentity will stop being used in the next release.
	ToBeDeprecatedDefaultControllerIdentity = "temporal-worker-controller"
)
