// Command wol-provider serves Cluster Autoscaler's externalgrpc CloudProvider API for node groups
// made of existing machines, powering them on and off through pluggable methods.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/oldsj/cluster-autoscaler-wol-provider/internal/config"
	"github.com/oldsj/cluster-autoscaler-wol-provider/internal/kube"
	"github.com/oldsj/cluster-autoscaler-wol-provider/internal/provider"
	pb "github.com/oldsj/cluster-autoscaler-wol-provider/proto/externalgrpc"
)

// version is set at build time (-ldflags -X main.version=...).
var version = "dev"

func main() {
	var (
		configPath = flag.String("config", "/etc/wol-provider/config.yaml", "provider configuration file")
		listen     = flag.String("listen", ":8086", "gRPC listen address")
		healthAddr = flag.String("health-listen", ":8087", "plaintext gRPC health-only listen address, for kubelet probes (which can't present a client certificate); empty disables it")
		dryRun     = flag.Bool("dry-run", false, "log every decision, power nothing on or off, and report each change as failed")
		kubeconfig = flag.String("kubeconfig", "", "kubeconfig file; empty means in-cluster")
		tf         tlsFlags
		debug      = flag.Bool("debug", false, "debug logging")
	)
	flag.StringVar(&tf.certFile, "tls-cert-file", "", "server certificate (required unless --insecure)")
	flag.StringVar(&tf.keyFile, "tls-key-file", "", "server key (required unless --insecure)")
	flag.StringVar(&tf.clientCA, "tls-client-ca-file", "", "CA that signs client certificates; clients must present one (required unless --insecure)")
	flag.BoolVar(&tf.insecure, "insecure", false, "serve plaintext gRPC with no client authentication; anyone who can reach the port can power machines off. Testing only")
	flag.Parse()

	level := slog.LevelInfo
	if *debug {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	if err := run(log, *configPath, *listen, *healthAddr, *dryRun, *kubeconfig, tf); err != nil {
		log.Error("exiting", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger, configPath, listen, healthAddr string, dryRun bool, kubeconfig string, tf tlsFlags) error {
	creds, err := tf.credentials()
	if err != nil {
		return err
	}
	cfg, err := config.Load(configPath)
	if err != nil {
		return err
	}
	plugins, err := buildPlugins(cfg, registry)
	if err != nil {
		return err
	}
	approver, err := buildApprover(cfg)
	if err != nil {
		return err
	}
	restConfig, err := restConfig(kubeconfig)
	if err != nil {
		return fmt.Errorf("kubernetes client: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return fmt.Errorf("kubernetes client: %w", err)
	}
	p, err := provider.New(provider.Options{Config: cfg, Plugins: plugins, Approver: approver,
		Nodes: kube.Client{Clientset: clientset}, DryRun: dryRun, Log: log})
	if err != nil {
		return err
	}

	var opts []grpc.ServerOption
	if creds != nil {
		opts = append(opts, grpc.Creds(creds))
	} else {
		log.Warn("serving plaintext gRPC with no client authentication (--insecure)")
	}
	server := grpc.NewServer(opts...)
	pb.RegisterCloudProviderServer(server, p)
	hs := health.NewServer()
	hs.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	healthpb.RegisterHealthServer(server, hs)

	lis, err := net.Listen("tcp", listen)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if healthAddr != "" {
		hlis, err := net.Listen("tcp", healthAddr)
		if err != nil {
			return err
		}
		hserver := grpc.NewServer()
		healthpb.RegisterHealthServer(hserver, hs)
		go func() { _ = hserver.Serve(hlis) }()
		defer hserver.Stop()
	}
	go p.RunProbes(ctx)
	go watchHealth(ctx, log, p, hs)
	go func() {
		<-ctx.Done()
		done := make(chan struct{})
		go func() { server.GracefulStop(); close(done) }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			server.Stop()
		}
	}()
	groups := make([]string, 0, len(cfg.NodeGroups))
	for _, g := range cfg.NodeGroups {
		groups = append(groups, g.ID)
	}
	log.Info("serving", "version", version, "listen", listen, "dryRun", dryRun, "groups", groups, "mtls", creds != nil,
		"approval", approver != nil)
	return server.Serve(lis)
}

// watchHealth reports SERVING while the provider can read nodes, NOT_SERVING otherwise.
func watchHealth(ctx context.Context, log *slog.Logger, p *provider.Provider, hs *health.Server) {
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	serving := false
	for {
		rctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		_, err := p.Refresh(rctx, &pb.RefreshRequest{})
		cancel()
		switch {
		case err == nil && !serving:
			hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
			serving = true
		case err != nil && serving:
			log.Error("can't read nodes; reporting NOT_SERVING", "err", err)
			hs.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
			serving = false
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func restConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig == "" {
		return rest.InClusterConfig()
	}
	return clientcmd.BuildConfigFromFlags("", kubeconfig)
}
