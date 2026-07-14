// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2024 Datadog, Inc.

package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"

	temporaliov1alpha1 "github.com/temporalio/temporal-worker-controller/api/v1alpha1"
	"github.com/temporalio/temporal-worker-controller/internal/controller"
	"github.com/temporalio/temporal-worker-controller/internal/controller/clientpool"
	"go.temporal.io/sdk/log"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	utilruntime.Must(temporaliov1alpha1.AddToScheme(scheme))
	//+kubebuilder:scaffold:scheme
}

func main() {
	var metricsAddr string
	var probeAddr string
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metric endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	// Leader election has been removed: the controller runs as a single replica,
	// so there is no second manager to coordinate with, and removing it drops the
	// lease-renewal calls to the apiserver (whose timeouts were crash-looping the
	// controller under load). The --leader-elect flag is retained as an accepted
	// no-op so manifests that still pass it do not fail with "flag provided but
	// not defined".
	_ = flag.Bool("leader-elect", false, "Deprecated: no-op. Leader election has been removed (single-replica controller).")
	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	//ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	ctrl.SetLogger(zap.New(zap.JSONEncoder()))

	ctrlOpts := ctrl.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress: metricsAddr,
		},
		HealthProbeBindAddress: probeAddr,
	}
	// WATCH_NAMESPACE, when set, scopes the controller's cache (and thus the resources it
	// reconciles) to a single namespace. Empty (the default) watches all namespaces, so
	// production behavior is unchanged. Used to isolate the controller to a test namespace
	// for on-cluster validation.
	if watchNs := os.Getenv("WATCH_NAMESPACE"); watchNs != "" {
		ctrlOpts.Cache.DefaultNamespaces = map[string]cache.Config{watchNs: {}}
	}
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrlOpts)
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	if err = (&controller.TemporalWorkerDeploymentReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
		TemporalClientPool: clientpool.New(
			log.NewStructuredLogger(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
				AddSource:   false,
				Level:       nil,
				ReplaceAttr: nil,
			}))),
			mgr.GetClient(),
		),
		Recorder: mgr.GetEventRecorderFor("temporal-worker-controller"),
		MaxDeploymentVersionsIneligibleForDeletion: controller.GetControllerMaxDeploymentVersionsIneligibleForDeletion(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "TemporalWorkerDeployment")
		os.Exit(1)
	}
	if err = temporaliov1alpha1.NewWorkerResourceTemplateValidator(mgr).SetupWebhookWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create webhook", "webhook", "WorkerResourceTemplate")
		os.Exit(1)
	}
	//+kubebuilder:scaffold:builder

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	podNamespace := os.Getenv("POD_NAMESPACE")
	if podNamespace == "" {
		setupLog.Error(nil, "POD_NAMESPACE environment variable must be set")
		os.Exit(1)
	}
	var ns corev1.Namespace
	if err := mgr.GetAPIReader().Get(context.Background(), types.NamespacedName{Name: podNamespace}, &ns); err != nil {
		setupLog.Error(err, "unable to fetch namespace UID for controller identity suffix")
		os.Exit(1)
	}
	if err := os.Setenv(controller.IdentitySuffixEnvKey, string(ns.UID)); err != nil {
		setupLog.Error(err, fmt.Sprintf("unable to set %s", controller.IdentitySuffixEnvKey))
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}
