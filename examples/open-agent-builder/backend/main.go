package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/infiniflow/ragflow/harness/examples/open-agent-builder/engine"
	"github.com/infiniflow/ragflow/harness/examples/open-agent-builder/handler"
	"github.com/infiniflow/ragflow/harness/examples/open-agent-builder/store"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	// --- Load config.yaml ---
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		configPath = filepath.Join("config.yaml")
	}
	appCfg, err := engine.LoadConfigFile(configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}
	log.Printf("Loaded config from %s (%d model(s))", configPath, len(appCfg.Models))
	if appCfg.MySQL != nil {
		log.Printf("MySQL: %s@%s:%d/%s", appCfg.MySQL.User, appCfg.MySQL.Host, appCfg.MySQL.Port, appCfg.MySQL.Database)
	}

	// --- Connect MySQL (if configured) ---
	var st *store.Store
	if appCfg.MySQL != nil {
		dsn := appCfg.MySQL.DSN()
		st, err = store.New(dsn)
		if err != nil {
			log.Printf("Warning: MySQL not available: %v (config persistence disabled)", err)
			st = nil
		}
	} else {
		log.Println("No mysql section in config — config persistence disabled")
	}

	// --- Merge persisted API keys from MySQL ---
	if st != nil {
		saved, err := st.LoadModels()
		if err == nil && saved != nil {
			keyMap := make(map[string]string)
			for _, m := range saved {
				if m.APIKey != "" {
					keyMap[m.Name] = m.APIKey
				}
			}
			for i := range appCfg.Models {
				if k, ok := keyMap[appCfg.Models[i].Name]; ok {
					appCfg.Models[i].APIKey = k
				}
			}
			log.Printf("Merged %d API key(s) from MySQL", len(keyMap))
		}
	}

	registry := engine.NewRegistryFromConfig(appCfg)
	eng := engine.New(registry)
	h := handler.New(eng, st, registry)

	mux := http.NewServeMux()
	h.RegisterAPI(mux)

	corsHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		mux.ServeHTTP(w, r)
	})

	server := &http.Server{
		Addr:    fmt.Sprintf("0.0.0.0:%s", port),
		Handler: corsHandler,
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-quit
		log.Println("Shutting down server...")
		server.Close()
		if st != nil {
			st.Close()
		}
	}()

	log.Printf("Open Agent Builder backend listening on :%s", port)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Server error: %v", err)
	}
}
