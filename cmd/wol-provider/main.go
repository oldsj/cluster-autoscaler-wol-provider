// Command wol-provider serves Cluster Autoscaler's externalgrpc CloudProvider API for node groups
// made of existing machines, waking them with Wake-on-LAN and powering them off through their OS.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/oldsj/cluster-autoscaler-wol-provider/internal/config"
	"github.com/oldsj/cluster-autoscaler-wol-provider/internal/kube"
	"github.com/oldsj/cluster-autoscaler-wol-provider/internal/power"
	"github.com/oldsj/cluster-autoscaler-wol-provider/internal/provider"
	pb "github.com/oldsj/cluster-autoscaler-wol-provider/proto/externalgrpc"
)

func main() {
	var (
		configPath = flag.String("config", "/etc/wol-provider/config.yaml", "provider configuration file")
		listen     = flag.String("listen", ":8086", "gRPC listen address")
		dryRun     = flag.Bool("dry-run", false, "log every decision, power nothing on or off, and report each change as failed")
		kubeconfig = flag.String("kubeconfig", "", "kubeconfig file; empty means in-cluster")
		certFile   = flag.String("tls-cert-file", "", "server certificate; with -tls-key-file, serve TLS")
		keyFile    = flag.String("tls-key-file", "", "server key")
		clientCA   = flag.String("tls-client-ca-file", "", "require client certificates signed by this CA")
		debug      = flag.Bool("debug", false, "debug logging")
	)
	flag.Parse()

	level := slog.LevelInfo
	if *debug {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	if err := run(log, *configPath, *listen, *dryRun, *kubeconfig, *certFile, *keyFile, *clientCA); err != nil {
		log.Error("exiting", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger, configPath, listen string, dryRun bool, kubeconfig, certFile, keyFile, clientCA string) error {
	cfg, err := config.Load(configPath)
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
	methods := power.Methods{
		On:  map[string]power.On{"wol": power.WOL{}},
		Off: map[string]power.Off{"ssh": power.SSH{}},
	}
	if cfg.Talos != nil {
		methods.Off["talos"] = power.Talos{Credentials: *cfg.Talos}
	}
	p := provider.New(provider.Options{Config: cfg, Power: methods, Nodes: kube.Client{Clientset: clientset}, DryRun: dryRun, Log: log})

	var opts []grpc.ServerOption
	if certFile != "" || keyFile != "" {
		creds, err := serverTLS(certFile, keyFile, clientCA)
		if err != nil {
			return err
		}
		opts = append(opts, grpc.Creds(creds))
	}
	server := grpc.NewServer(opts...)
	pb.RegisterCloudProviderServer(server, p)
	healthpb.RegisterHealthServer(server, health.NewServer())

	lis, err := net.Listen("tcp", listen)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		server.GracefulStop()
	}()
	groups := make([]string, 0, len(cfg.NodeGroups))
	for _, g := range cfg.NodeGroups {
		groups = append(groups, g.ID)
	}
	log.Info("serving", "listen", listen, "dryRun", dryRun, "groups", groups, "tls", len(opts) > 0)
	return server.Serve(lis)
}

func restConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig == "" {
		return rest.InClusterConfig()
	}
	return clientcmd.BuildConfigFromFlags("", kubeconfig)
}

func serverTLS(certFile, keyFile, clientCA string) (credentials.TransportCredentials, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("server certificate: %w", err)
	}
	c := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	if clientCA != "" {
		pem, err := os.ReadFile(clientCA)
		if err != nil {
			return nil, fmt.Errorf("client CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("client CA: no PEM certificates in %s", clientCA)
		}
		c.ClientCAs = pool
		c.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return credentials.NewTLS(c), nil
}
