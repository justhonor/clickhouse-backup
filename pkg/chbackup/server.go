package chbackup

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	"github.com/gorilla/mux"
	"golang.org/x/sync/semaphore"
	yaml "gopkg.in/yaml.v2"
)

type APIServer struct {
	config  Config
	lock    *semaphore.Weighted
	server  *http.Server
	restart chan bool
}

type APIResult struct {
	Type    string
	Message string
}

type APIGenericResult struct {
	Type   string
	Result interface{}
}

type APIListResult struct {
	Type string
	Backup
}

type APITablesResult struct {
	Type string
	Table
}

var (
	ErrAPILocked = errors.New("Another operation is currently running")
)

// Server - expose CLI commands as REST API
func Server(config Config) error {
	api := APIServer{config: config, lock: semaphore.NewWeighted(1), restart: make(chan bool)}
	for {
		api.server = api.setupAPIServer(api.config)
		go func() {
			log.Printf("Starting API server on %s", api.config.API.ListenAddr)
			if err := api.server.ListenAndServe(); err != http.ErrServerClosed {
				log.Printf("Error starting API server: %v", err)
				os.Exit(1)
			}
		}()
		_ = <-api.restart
		api.server.Close()
		log.Printf("Reloading config and restarting API server.")
	}
	return nil
}
func (api *APIServer) setupAPIServer(config Config) *http.Server {
	r := mux.NewRouter()
	r.HandleFunc("/", httpRootHandler).Methods("GET")

	r.HandleFunc("/backup/tables", func(w http.ResponseWriter, r *http.Request) {
		httpTablesHandler(w, r, config)
	}).Methods("GET")
	r.HandleFunc("/backup/list", func(w http.ResponseWriter, r *http.Request) {
		httpListHandler(w, r, config)
	}).Methods("GET")
	r.HandleFunc("/backup/create", func(w http.ResponseWriter, r *http.Request) {
		api.httpCreateHandler(w, r, config)
	}).Methods("POST", "GET")
	r.HandleFunc("/backup/clean", func(w http.ResponseWriter, r *http.Request) {
		api.httpCleanHandler(w, r, config)
	}).Methods("POST", "GET")
	r.HandleFunc("/backup/freeze", func(w http.ResponseWriter, r *http.Request) {
		api.httpFreezeHandler(w, r, config)
	}).Methods("POST", "GET")
	r.HandleFunc("/backup/upload/{name}", func(w http.ResponseWriter, r *http.Request) {
		api.httpUploadHandler(w, r, config)
	}).Methods("POST", "GET")
	r.HandleFunc("/backup/download/{name}", func(w http.ResponseWriter, r *http.Request) {
		api.httpDownloadHandler(w, r, config)
	}).Methods("POST", "GET")
	r.HandleFunc("/backup/restore/{name}", func(w http.ResponseWriter, r *http.Request) {
		api.httpRestoreHandler(w, r, config)
	}).Methods("POST", "GET")
	r.HandleFunc("/backup/delete/{where}/{name}", func(w http.ResponseWriter, r *http.Request) {
		api.httpDeleteHandler(w, r, config)
	}).Methods("POST", "GET")
	r.HandleFunc("/backup/config/default", func(w http.ResponseWriter, r *http.Request) {
		httpConfigDefaultHandler(w, r, config)
	}).Methods("GET")
	r.HandleFunc("/backup/config", func(w http.ResponseWriter, r *http.Request) {
		httpConfigHandler(w, r, config)
	}).Methods("GET")
	r.HandleFunc("/backup/config", func(w http.ResponseWriter, r *http.Request) {
		api.httpConfigUpdateHandler(w, r, config)
	}).Methods("POST", "GET")

	srv := &http.Server{
		Addr:    config.API.ListenAddr,
		Handler: r,
	}
	return srv
}

// show API index
func httpRootHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, rootHtml)
}

func httpConfigDefaultHandler(w http.ResponseWriter, r *http.Request, c Config) {
	defaultConfig := DefaultConfig()
	d, _ := yaml.Marshal(&defaultConfig)
	out, err := json.Marshal(APIGenericResult{Type: "success", Result: string(d)})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		out, _ := json.Marshal(APIResult{Type: "error", Message: err.Error()})
		fmt.Fprintf(w, string(out))
		return
	}
	fmt.Fprintln(w, string(out))
}

func httpConfigHandler(w http.ResponseWriter, r *http.Request, c Config) {
	cfg, _ := yaml.Marshal(&c)
	out, err := json.Marshal(APIGenericResult{Type: "success", Result: string(cfg)})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		out, _ := json.Marshal(APIResult{Type: "error", Message: err.Error()})
		fmt.Fprintf(w, string(out))
		return
	}
	fmt.Fprintln(w, string(out))
}

func (api *APIServer) httpConfigUpdateHandler(w http.ResponseWriter, r *http.Request, c Config) {
	if locked := api.lock.TryAcquire(1); !locked {
		log.Println(ErrAPILocked)
		w.WriteHeader(http.StatusServiceUnavailable)
		out, _ := json.Marshal(APIResult{Type: "error", Message: ErrAPILocked.Error()})
		fmt.Fprintf(w, string(out))
		return
	}
	defer api.lock.Release(1)

	defer r.Body.Close()
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		out, _ := json.Marshal(APIResult{Type: "error", Message: fmt.Sprintf("Error parsing POST form: %v", err.Error())})
		fmt.Fprintf(w, string(out))
		return
	}

	newConfig := DefaultConfig()
	if err := yaml.Unmarshal(body, &newConfig); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		out, _ := json.Marshal(APIResult{Type: "error", Message: fmt.Sprintf("Error parsing new config: %v", err.Error())})
		fmt.Fprintf(w, string(out))
		return
	}

	if err := validateConfig(newConfig); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		out, _ := json.Marshal(APIResult{Type: "error", Message: fmt.Sprintf("Error validating new config: %v", err.Error())})
		fmt.Fprintf(w, string(out))
		return
	}
	log.Printf("Applying new valid config.")
	api.config = *newConfig
	api.restart <- true
	return
}

// list of tables
func httpTablesHandler(w http.ResponseWriter, r *http.Request, c Config) {
	tables, err := getTables(c)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		out, _ := json.Marshal(APIResult{Type: "error", Message: err.Error()})
		fmt.Fprintf(w, string(out))
		return
	}
	for _, table := range tables {
		out, err := json.Marshal(APITablesResult{"table", table})
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			out, _ := json.Marshal(APIResult{Type: "error", Message: err.Error()})
			fmt.Fprintf(w, string(out))
			return
		}
		fmt.Fprintln(w, string(out))
	}
}

func httpListHandler(w http.ResponseWriter, r *http.Request, c Config) {
	localBackups, err := ListLocalBackups(c)
	if err != nil && !os.IsNotExist(err) {
		w.WriteHeader(http.StatusInternalServerError)
		out, _ := json.Marshal(APIResult{Type: "error", Message: err.Error()})
		fmt.Fprintf(w, string(out))
		return
	}
	backups := []APIListResult{}
	for _, backup := range localBackups {
		backups = append(backups, APIListResult{"local", backup})
	}
	if c.General.RemoteStorage != "none" {
		remoteBackups, err := getRemoteBackups(c)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			out, _ := json.Marshal(APIResult{Type: "error", Message: err.Error()})
			fmt.Fprintf(w, string(out))
			return
		}
		for _, backup := range remoteBackups {
			backups = append(backups, APIListResult{"remote", backup})
		}
	}

	for _, backup := range backups {
		out, err := json.Marshal(backup)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			out, _ := json.Marshal(APIResult{Type: "error", Message: err.Error()})
			fmt.Fprintf(w, string(out))
			return
		}
		fmt.Fprintln(w, string(out))
	}
}

func (api *APIServer) httpCreateHandler(w http.ResponseWriter, r *http.Request, c Config) {
	if locked := api.lock.TryAcquire(1); !locked {
		log.Println(ErrAPILocked)
		w.WriteHeader(http.StatusServiceUnavailable)
		out, _ := json.Marshal(APIResult{Type: "error", Message: ErrAPILocked.Error()})
		fmt.Fprintf(w, string(out))
		return
	}
	defer api.lock.Release(1)

	tablePattern := ""
	desiredName := ""

	query := r.URL.Query()
	if tp, exist := query["table"]; exist {
		tablePattern = tp[0]
	}
	if dn, exist := query["name"]; exist {
		desiredName = dn[0]
	}

	backup_name, err := CreateBackup(c, desiredName, tablePattern)
	if err != nil {
		log.Printf("CreateBackup error: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		out, _ := json.Marshal(APIResult{Type: "error", Message: err.Error()})
		fmt.Fprintf(w, string(out))
		return
	}
	out, err := json.Marshal(APIResult{Type: "success", Message: backup_name})
	if err != nil {
		e := fmt.Sprintf("marshal error: %v", err)
		log.Println(e)
		w.WriteHeader(http.StatusInternalServerError)
		out, _ := json.Marshal(APIResult{Type: "error", Message: e})
		fmt.Fprintf(w, string(out))
		return
	}
	fmt.Fprintf(w, string(out))
	return
}

func (api *APIServer) httpFreezeHandler(w http.ResponseWriter, r *http.Request, c Config) {
	if locked := api.lock.TryAcquire(1); !locked {
		log.Println(ErrAPILocked)
		w.WriteHeader(http.StatusServiceUnavailable)
		out, _ := json.Marshal(APIResult{Type: "error", Message: ErrAPILocked.Error()})
		fmt.Fprintf(w, string(out))
		return
	}
	defer api.lock.Release(1)

	tablePattern := ""
	if err := Freeze(c, tablePattern); err != nil {
		log.Printf("Freeze error: = %+v\n", err)
		w.WriteHeader(http.StatusInternalServerError)
		out, _ := json.Marshal(APIResult{Type: "error", Message: err.Error()})
		fmt.Fprintf(w, string(out))
		return
	}
	out, err := json.Marshal(APIResult{Type: "success"})
	if err != nil {
		e := fmt.Sprintf("marshal error: %v", err)
		log.Println(e)
		w.WriteHeader(http.StatusInternalServerError)
		out, _ := json.Marshal(APIResult{Type: "error", Message: e})
		fmt.Fprintf(w, string(out))
		return
	}
	fmt.Fprintf(w, string(out))
	return
}
func (api *APIServer) httpCleanHandler(w http.ResponseWriter, r *http.Request, c Config) {
	if locked := api.lock.TryAcquire(1); !locked {
		log.Println(ErrAPILocked)
		w.WriteHeader(http.StatusServiceUnavailable)
		out, _ := json.Marshal(APIResult{Type: "error", Message: ErrAPILocked.Error()})
		fmt.Fprintf(w, string(out))
		return
	}
	defer api.lock.Release(1)

	if err := Clean(c); err != nil {
		log.Printf("Clean error: = %+v\n", err)
		w.WriteHeader(http.StatusInternalServerError)
		out, _ := json.Marshal(APIResult{Type: "error", Message: err.Error()})
		fmt.Fprintf(w, string(out))
		return
	}
	out, err := json.Marshal(APIResult{Type: "success"})
	if err != nil {
		e := fmt.Sprintf("marshal error: %v", err)
		log.Println(e)
		w.WriteHeader(http.StatusInternalServerError)
		out, _ := json.Marshal(APIResult{Type: "error", Message: e})
		fmt.Fprintf(w, string(out))
		return
	}
	fmt.Fprintf(w, string(out))
	return
}

func (api *APIServer) httpUploadHandler(w http.ResponseWriter, r *http.Request, c Config) {
	vars := mux.Vars(r)
	diffFrom := ""
	query := r.URL.Query()
	if df, exist := query["diff-from"]; exist {
		diffFrom = df[0]
	}
	if err := Upload(c, vars["name"], diffFrom); err != nil {
		log.Printf("Upload error: %+v\n", err)
		w.WriteHeader(http.StatusInternalServerError)
		out, _ := json.Marshal(APIResult{Type: "error", Message: err.Error()})
		fmt.Fprintf(w, string(out))
		return
	}
	out, err := json.Marshal(APIResult{Type: "success"})
	if err != nil {
		e := fmt.Sprintf("marshal error: %v", err)
		log.Println(e)
		w.WriteHeader(http.StatusInternalServerError)
		out, _ := json.Marshal(APIResult{Type: "error", Message: e})
		fmt.Fprintf(w, string(out))
		return
	}
	fmt.Fprintf(w, string(out))
	return
}
func (api *APIServer) httpRestoreHandler(w http.ResponseWriter, r *http.Request, c Config) {
	if locked := api.lock.TryAcquire(1); !locked {
		log.Println(ErrAPILocked)
		w.WriteHeader(http.StatusServiceUnavailable)
		out, _ := json.Marshal(APIResult{Type: "error", Message: ErrAPILocked.Error()})
		fmt.Fprintf(w, string(out))
		return
	}
	defer api.lock.Release(1)

	vars := mux.Vars(r)
	tablePattern := ""
	schemaOnly := false
	dataOnly := false

	query := r.URL.Query()
	if tp, exist := query["table"]; exist {
		tablePattern = tp[0]
	}
	if _, exist := query["schema"]; exist {
		schemaOnly = true
	}
	if _, exist := query["data"]; exist {
		dataOnly = true
	}
	if err := Restore(c, vars["name"], tablePattern, schemaOnly, dataOnly); err != nil {
		log.Printf("Download error: %+v\n", err)
		w.WriteHeader(http.StatusInternalServerError)
		out, _ := json.Marshal(APIResult{Type: "error", Message: err.Error()})
		fmt.Fprintf(w, string(out))
		return
	}
	out, err := json.Marshal(APIResult{Type: "success"})
	if err != nil {
		e := fmt.Sprintf("marshal error: %v", err)
		log.Println(e)
		w.WriteHeader(http.StatusInternalServerError)
		out, _ := json.Marshal(APIResult{Type: "error", Message: e})
		fmt.Fprintf(w, string(out))
		return
	}
	fmt.Fprintf(w, string(out))
	return
}
func (api *APIServer) httpDownloadHandler(w http.ResponseWriter, r *http.Request, c Config) {
	vars := mux.Vars(r)
	if err := Download(c, vars["name"]); err != nil {
		log.Printf("Download error: %+v\n", err)
		w.WriteHeader(http.StatusInternalServerError)
		out, _ := json.Marshal(APIResult{Type: "error", Message: err.Error()})
		fmt.Fprintf(w, string(out))
		return
	}
	out, err := json.Marshal(APIResult{Type: "success"})
	if err != nil {
		e := fmt.Sprintf("marshal error: %v", err)
		log.Println(e)
		w.WriteHeader(http.StatusInternalServerError)
		out, _ := json.Marshal(APIResult{Type: "error", Message: e})
		fmt.Fprintf(w, string(out))
		return
	}
	fmt.Fprintf(w, string(out))
	return
}

func (api *APIServer) httpDeleteHandler(w http.ResponseWriter, r *http.Request, c Config) {
	if locked := api.lock.TryAcquire(1); !locked {
		log.Println(ErrAPILocked)
		w.WriteHeader(http.StatusServiceUnavailable)
		out, _ := json.Marshal(APIResult{Type: "error", Message: ErrAPILocked.Error()})
		fmt.Fprintf(w, string(out))
		return
	}
	defer api.lock.Release(1)

	vars := mux.Vars(r)
	switch vars["where"] {
	case "local":
		if err := RemoveBackupLocal(c, vars["name"]); err != nil {
			log.Printf("RemoveBackupLocal error: %+v\n", err)
			w.WriteHeader(http.StatusInternalServerError)
			out, _ := json.Marshal(APIResult{Type: "error", Message: err.Error()})
			fmt.Fprintf(w, string(out))
			return
		}
	case "remote":
		if err := RemoveBackupRemote(c, vars["name"]); err != nil {
			log.Printf("RemoveBackupRemote error: %+v\n", err)
			w.WriteHeader(http.StatusInternalServerError)
			out, _ := json.Marshal(APIResult{Type: "error", Message: err.Error()})
			fmt.Fprintf(w, string(out))
			return
		}
	default:
		w.WriteHeader(http.StatusInternalServerError)
		out, _ := json.Marshal(APIResult{Type: "error", Message: "Backup location must be 'local' or 'remote'."})
		fmt.Fprintf(w, string(out))
		return
	}
	out, err := json.Marshal(APIResult{Type: "success"})
	if err != nil {
		e := fmt.Sprintf("marshal error: %v", err)
		log.Println(e)
		w.WriteHeader(http.StatusInternalServerError)
		out, _ := json.Marshal(APIResult{Type: "error", Message: e})
		fmt.Fprintf(w, string(out))
		return
	}
	fmt.Fprintf(w, string(out))
	return
}

const rootHtml = `<html><body>
<h1>clickhouse-backup API</h1>
See: <a href="https://github.com/Altinity/clickhouse-backup#api-configuration">https://github.com/Altinity/clickhouse-backup#api-configuration</a>
</body></html>`
