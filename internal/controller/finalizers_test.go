// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.

package controller

import (
	"testing"
	"time"

	temporaliov1alpha1 "github.com/temporalio/temporal-worker-controller/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func twdWithFinalizers(finalizers []string, deleting bool) *temporaliov1alpha1.TemporalWorkerDeployment {
	twd := &temporaliov1alpha1.TemporalWorkerDeployment{}
	twd.SetFinalizers(finalizers)
	if deleting {
		t := metav1.NewTime(time.Unix(1000, 0))
		twd.SetDeletionTimestamp(&t)
	}
	return twd
}

// TestMigrateFinalizer covers the add-before-strip invariant that keeps the 600-cluster
// rollout from either orphaning the legacy fork finalizer or stripping it before
// delete-protection is in place.
func TestMigrateFinalizer(t *testing.T) {
	const (
		legacy = legacyTWDFinalizer
		prot   = finalizerName
		other  = "example.com/keep-me"
	)

	cases := []struct {
		name         string
		start        []string
		deleting     bool
		wantFinal    []string
		wantChanged  bool
		wantStripped bool
	}{
		{
			name:  "live, only legacy: adds delete-protection and strips legacy",
			start: []string{legacy}, deleting: false,
			wantFinal: []string{prot}, wantChanged: true, wantStripped: true,
		},
		{
			name:  "live, both present: strips legacy, keeps delete-protection",
			start: []string{legacy, prot}, deleting: false,
			wantFinal: []string{prot}, wantChanged: true, wantStripped: true,
		},
		{
			name:  "live, only delete-protection: idempotent no-op",
			start: []string{prot}, deleting: false,
			wantFinal: []string{prot}, wantChanged: false, wantStripped: false,
		},
		{
			name:  "live, none: adds delete-protection only",
			start: nil, deleting: false,
			wantFinal: []string{prot}, wantChanged: true, wantStripped: false,
		},
		{
			name:  "live: preserves unrelated finalizers",
			start: []string{legacy, other}, deleting: false,
			wantFinal: []string{other, prot}, wantChanged: true, wantStripped: true,
		},
		{
			name:  "deleting, both present: strips legacy, does not re-add delete-protection",
			start: []string{prot, legacy}, deleting: true,
			wantFinal: []string{prot}, wantChanged: true, wantStripped: true,
		},
		{
			// A deleting object with only the legacy finalizer is released by the
			// reconciler's deletion branch, not by this helper (the helper must not add
			// delete-protection to a deleting object). Here it is a deliberate no-op.
			name:  "deleting, only legacy: helper no-ops (deletion path releases it)",
			start: []string{legacy}, deleting: true,
			wantFinal: []string{legacy}, wantChanged: false, wantStripped: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			obj := twdWithFinalizers(tc.start, tc.deleting)

			changed, stripped := migrateFinalizer(obj, legacy)

			if changed != tc.wantChanged {
				t.Errorf("changed = %v, want %v", changed, tc.wantChanged)
			}
			if stripped != tc.wantStripped {
				t.Errorf("strippedLegacy = %v, want %v", stripped, tc.wantStripped)
			}
			if got := obj.GetFinalizers(); !equalStringSet(got, tc.wantFinal) {
				t.Errorf("finalizers = %v, want %v", got, tc.wantFinal)
			}
			// Invariant: a non-deleting object must never end up carrying the legacy
			// finalizer without delete-protection also present.
			if !tc.deleting &&
				controllerutil.ContainsFinalizer(obj, legacy) &&
				!controllerutil.ContainsFinalizer(obj, prot) {
				t.Errorf("invariant violated: legacy present without delete-protection: %v", obj.GetFinalizers())
			}
		})
	}
}

func equalStringSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	counts := map[string]int{}
	for _, s := range a {
		counts[s]++
	}
	for _, s := range b {
		counts[s]--
	}
	for _, c := range counts {
		if c != 0 {
			return false
		}
	}
	return true
}
