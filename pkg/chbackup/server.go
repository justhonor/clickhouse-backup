package chbackup

// TODO: locking!

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/gorilla/mux"
)

// Server - expose CLI commands as REST API
func Server(config Config) error {
	r := mux.NewRouter()
	r.HandleFunc("/", httpRootHandler).Methods("GET")

	r.HandleFunc("/backup/tables", func(w http.ResponseWriter, r *http.Request) {
		httpTablesHandler(w, r, config)
	}).Methods("GET")
	r.HandleFunc("/backup/list", func(w http.ResponseWriter, r *http.Request) {
		httpListHandler(w, r, config)
	}).Methods("GET")
	r.HandleFunc("/backup/create", func(w http.ResponseWriter, r *http.Request) {
		httpCreateHandler(w, r, config)
	}).Methods("POST")
	r.HandleFunc("/backup/clean", func(w http.ResponseWriter, r *http.Request) {
		httpCleanHandler(w, r, config)
	}).Methods("POST")
	r.HandleFunc("/backup/freeze", func(w http.ResponseWriter, r *http.Request) {
		httpFreezeHandler(w, r, config)
	}).Methods("POST")
	r.HandleFunc("/backup/upload/{name}", func(w http.ResponseWriter, r *http.Request) {
		httpUploadHandler(w, r, config)
	}).Methods("POST")
	r.HandleFunc("/backup/download/{name}", func(w http.ResponseWriter, r *http.Request) {
		httpDownloadHandler(w, r, config)
	}).Methods("POST")
	r.HandleFunc("/backup/restore/{name}", func(w http.ResponseWriter, r *http.Request) {
		httpRestoreHandler(w, r, config)
	}).Methods("POST")

	r.HandleFunc("/backup/delete/{where}/{name}", func(w http.ResponseWriter, r *http.Request) {
		httpDeleteHandler(w, r, config)
	}).Methods("POST")

	// TODO: registerMetricsHandlers(r)
	srv := &http.Server{
		Addr:    config.API.ListenAddr,
		Handler: r,
	}
	log.Printf("Running API server on %s", config.API.ListenAddr)
	return srv.ListenAndServe()
}

// show API index
func httpRootHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, rootHtml)
}

// list of tables
func httpTablesHandler(w http.ResponseWriter, r *http.Request, c Config) {
	tables, err := getTables(c)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintln(w, err)
		log.Printf("Print tables error: = %+v\n", err)
		return
	}
	out, err := json.Marshal(tables)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintln(w, err)
		log.Printf("tables marshal error: = %+v\n", err)
		return
	}
	fmt.Fprintln(w, string(out))
}

type APIBackupsList struct {
	Local  []Backup
	Remote []Backup
}

func httpListHandler(w http.ResponseWriter, r *http.Request, c Config) {
	localBackups, err := ListLocalBackups(c)
	if err != nil && !os.IsNotExist(err) {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintln(w, err)
		log.Printf("ListLocalBackups error: %v", err)
		return
	}
	fullList := APIBackupsList{Local: localBackups}
	if c.General.RemoteStorage != "none" {
		remoteBackups, err := getRemoteBackups(c)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintln(w, err)
			log.Printf("ListRemoteBackups error: %v", err)
			return
		}
		fullList.Remote = remoteBackups
	}

	out, err := json.Marshal(fullList)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintln(w, err)
		log.Printf("backupList marshal error: = %+v\n", err)
		return
	}
	fmt.Fprintln(w, string(out))
}

type APIResult struct {
	Success bool
	Result  interface{}
}

func httpCreateHandler(w http.ResponseWriter, r *http.Request, c Config) {
	tablePattern := ""
	freezeOneByOne := false
	desiredName := ""

	query := r.URL.Query()
	if tp, exist := query["table"]; exist {
		tablePattern = tp[0]
	}
	if _, exist := query["freeze_one_by_one"]; exist {
		freezeOneByOne = true
	}
	if dn, exist := query["name"]; exist {
		desiredName = dn[0]
	}

	backup_name, err := CreateBackup(c, desiredName, tablePattern, freezeOneByOne)
	if err != nil {
		log.Printf("CreateBackup error: = %+v\n", err)
		w.WriteHeader(http.StatusInternalServerError)
		out, _ := json.Marshal(APIResult{Success: false, Result: err.Error()})
		fmt.Fprintf(w, string(out))
		return
	}
	out, err := json.Marshal(APIResult{Success: true, Result: backup_name})
	if err != nil {
		e := fmt.Sprintf("marshal error: %v", err)
		log.Println(e)
		w.WriteHeader(http.StatusInternalServerError)
		out, _ := json.Marshal(APIResult{Success: false, Result: e})
		fmt.Fprintf(w, string(out))
		return
	}
	fmt.Fprintf(w, string(out))
	return
}

func httpFreezeHandler(w http.ResponseWriter, r *http.Request, c Config) {
	tablePattern := ""
	useOldWay := false
	if err := Freeze(c, tablePattern, useOldWay); err != nil {
		log.Printf("Freeze error: = %+v\n", err)
		w.WriteHeader(http.StatusInternalServerError)
		out, _ := json.Marshal(APIResult{Success: false, Result: err.Error()})
		fmt.Fprintf(w, string(out))
		return
	}
	out, err := json.Marshal(APIResult{Success: true})
	if err != nil {
		e := fmt.Sprintf("marshal error: %v", err)
		log.Println(e)
		w.WriteHeader(http.StatusInternalServerError)
		out, _ := json.Marshal(APIResult{Success: false, Result: e})
		fmt.Fprintf(w, string(out))
		return
	}
	fmt.Fprintf(w, string(out))
	return
}
func httpCleanHandler(w http.ResponseWriter, r *http.Request, c Config) {
	if err := Clean(c); err != nil {
		log.Printf("Clean error: = %+v\n", err)
		w.WriteHeader(http.StatusInternalServerError)
		out, _ := json.Marshal(APIResult{Success: false, Result: err.Error()})
		fmt.Fprintf(w, string(out))
		return
	}
	out, err := json.Marshal(APIResult{Success: true})
	if err != nil {
		e := fmt.Sprintf("marshal error: %v", err)
		log.Println(e)
		w.WriteHeader(http.StatusInternalServerError)
		out, _ := json.Marshal(APIResult{Success: false, Result: e})
		fmt.Fprintf(w, string(out))
		return
	}
	fmt.Fprintf(w, string(out))
	return
}

func httpUploadHandler(w http.ResponseWriter, r *http.Request, c Config) {
	vars := mux.Vars(r)
	diffFrom := "" // TODO!
	if err := Upload(c, vars["name"], diffFrom); err != nil {
		log.Printf("Upload error: %+v\n", err)
		w.WriteHeader(http.StatusInternalServerError)
		out, _ := json.Marshal(APIResult{Success: false, Result: err.Error()})
		fmt.Fprintf(w, string(out))
		return
	}
	out, err := json.Marshal(APIResult{Success: true})
	if err != nil {
		e := fmt.Sprintf("marshal error: %v", err)
		log.Println(e)
		w.WriteHeader(http.StatusInternalServerError)
		out, _ := json.Marshal(APIResult{Success: false, Result: e})
		fmt.Fprintf(w, string(out))
		return
	}
	fmt.Fprintf(w, string(out))
	return
}
func httpRestoreHandler(w http.ResponseWriter, r *http.Request, c Config) {
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
		out, _ := json.Marshal(APIResult{Success: false, Result: err.Error()})
		fmt.Fprintf(w, string(out))
		return
	}
	out, err := json.Marshal(APIResult{Success: true})
	if err != nil {
		e := fmt.Sprintf("marshal error: %v", err)
		log.Println(e)
		w.WriteHeader(http.StatusInternalServerError)
		out, _ := json.Marshal(APIResult{Success: false, Result: e})
		fmt.Fprintf(w, string(out))
		return
	}
	fmt.Fprintf(w, string(out))
	return
}
func httpDownloadHandler(w http.ResponseWriter, r *http.Request, c Config) {
	vars := mux.Vars(r)
	if err := Download(c, vars["name"]); err != nil {
		log.Printf("Download error: %+v\n", err)
		w.WriteHeader(http.StatusInternalServerError)
		out, _ := json.Marshal(APIResult{Success: false, Result: err.Error()})
		fmt.Fprintf(w, string(out))
		return
	}
	out, err := json.Marshal(APIResult{Success: true})
	if err != nil {
		e := fmt.Sprintf("marshal error: %v", err)
		log.Println(e)
		w.WriteHeader(http.StatusInternalServerError)
		out, _ := json.Marshal(APIResult{Success: false, Result: e})
		fmt.Fprintf(w, string(out))
		return
	}
	fmt.Fprintf(w, string(out))
	return
}

func httpDeleteHandler(w http.ResponseWriter, r *http.Request, c Config) {
	vars := mux.Vars(r)
	switch vars["where"] {
	case "local":
		if err := RemoveBackupLocal(c, vars["name"]); err != nil {
			log.Printf("RemoveBackupLocal error: %+v\n", err)
			w.WriteHeader(http.StatusInternalServerError)
			out, _ := json.Marshal(APIResult{Success: false, Result: err.Error()})
			fmt.Fprintf(w, string(out))
			return
		}
	case "remote":
		if err := RemoveBackupRemote(c, vars["name"]); err != nil {
			log.Printf("RemoveBackupRemote error: %+v\n", err)
			w.WriteHeader(http.StatusInternalServerError)
			out, _ := json.Marshal(APIResult{Success: false, Result: err.Error()})
			fmt.Fprintf(w, string(out))
			return
		}
	default:
		w.WriteHeader(http.StatusInternalServerError)
		out, _ := json.Marshal(APIResult{Success: false, Result: "Backup location must be 'local' or 'remote'."})
		fmt.Fprintf(w, string(out))
		return
	}
	out, err := json.Marshal(APIResult{Success: true})
	if err != nil {
		e := fmt.Sprintf("marshal error: %v", err)
		log.Println(e)
		w.WriteHeader(http.StatusInternalServerError)
		out, _ := json.Marshal(APIResult{Success: false, Result: e})
		fmt.Fprintf(w, string(out))
		return
	}
	fmt.Fprintf(w, string(out))
	return
}

const rootHtml = `<html><body>
<h1>clickhouse-backup API</h1>
<ul>
<li><b>/</b> This index page
<li><b>/backup/tables</b> Print list of tables
<li><b>/backup/list</b> Print list of backups
<li><b>/backup/create</b> Create new backup
<li><b>/backup/clean</b> Remove data in 'shadow' folder
<li><b>/backup/delete</b> Delete specific backup
<li><b>/backup/freeze</b> Freeze tables
<li><b>/backup/upload</b> Upload backup to remote storage
<li><b>/backup/download</b> Download backup from remote storage
<li><b>/backup/restore</b> Create schema and restore data from backup
</ul>
</body></html>`
