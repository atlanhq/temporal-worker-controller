// Copyright 2025 The Atlan Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/util/validation"
)

// Real ScaledObject names observed on markeznp29 (2026-09-07). Apps released
// through the semver flow carry buildIDs like "1.1.1", so 99 of the 178
// TWC-owned ScaledObjects on that one tenant have a dotted name — and they work
// today, because an object name is a DNS subdomain and a dot is a legal
// separator. Normalisation must leave every one of them byte-identical.
//
// An earlier version of this change folded dots to "-", which would have
// renamed 56% of the fleet's ScaledObjects: a delete-and-recreate per object,
// each losing its HPA for a reconcile and resetting its cooldown, for no
// functional gain. This test exists so that cannot be reintroduced.
func TestNormalizeLeavesRealFleetNamesUnchanged(t *testing.T) {
	for _, name := range []string{
		"alloydb-postgres-worker-twd-od-1.1.1-scale",
		"amazon-dynamodb-assets-worker-twd-0.2.1-scale",
		"amazon-dynamodb-assets-worker-twd-od-0.2.1-scale",
		"azure-data-factory-worker-twd-od-1.0.1-scale",
		"azure-event-hub-worker-twd-od-1.2.0-scale",
		"cassandra-dse-worker-twd-od-0.3.0-scale",
		"cloudsql-postgres-worker-twd-od-0.1.2-scale",
		"connection-delete-worker-twd-od-1.0.0-scale",
		"microstrategy-worker-twd-od-0.3.1-scale",
		"mongodbatlas-worker-twd-od-1.0.2-scale",
		"monte-carlo-worker-twd-od-0.2.2-scale",
		"thoughtspot-worker-twd-od-1.1.0-scale",
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, name, normalizeScaledObjectName(name),
				"a live ScaledObject name must never be rewritten")
			assert.Empty(t, validation.IsDNS1123Subdomain(name),
				"premise: this live name is already a valid object name")
		})
	}
}

// Dots are legal but only between well-formed labels, so normalisation still
// has to repair the shapes a truncation or a character swap can produce.
func TestNormalizeRepairsMalformedDottedNames(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"app-worker-twd-1.1.1-scale", "app-worker-twd-1.1.1-scale"}, // untouched
		{"app-worker-twd-v1..2-scale", "app-worker-twd-v1.2-scale"},  // empty label dropped
		{"app-worker-twd-1.-2-scale", "app-worker-twd-1.2-scale"},    // dash-led label trimmed
		{"app-worker-twd-1-.2-scale", "app-worker-twd-1.2-scale"},    // dash-tailed label trimmed
		{"app-worker-twd-1.2.-scale", "app-worker-twd-1.2.scale"},    // interior repair
		{"app-worker-twd-release_2.0-scale", "app-worker-twd-release-2.0-scale"},
		{"App-Worker-TWD-1.1.1-SCALE", "app-worker-twd-1.1.1-scale"},
	} {
		t.Run(tc.in, func(t *testing.T) {
			got := normalizeScaledObjectName(tc.in)
			assert.Equal(t, tc.want, got)
			assert.Empty(t, validation.IsDNS1123Subdomain(got),
				"%q must be a valid object name", got)
			assert.Equal(t, got, normalizeScaledObjectName(got), "must be idempotent")
		})
	}
}
