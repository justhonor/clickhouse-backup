package chbackup

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

var (
	// ErrUnknownClickhouseDataPath -
	ErrTODO = errors.New("clickhouse data path is unknown, you can set data_path in config file")
)

// Server - expose CLI commands as REST API
func Server(config Config) error {
	r := mux.NewRouter()
	r.HandleFunc("/", httpRootHandler).Methods("GET")

	r.HandleFunc("/backup/tables", func(w http.ResponseWriter, r *http.Request) {
		httpTablesHandler(w, r, config)
	}).Methods("GET")
	r.HandleFunc("/backup/list", httpListHandler).Methods("GET")

	// TODO: registerMetricsHandlers(r)
	srv := &http.Server{
		Addr:    config.API.ListenAddr,
		Handler: r,
	}
	log.Printf("Running API server on %s", config.API.ListenAddr)
	return srv.ListenAndServe()
}

func httpRootHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, rootHtml)
}

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

func httpListHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, rootHtml)
}

const rootHtml = `<html><body>
<h1>clickhouse-backup API</h1>
<ul>
<li><b>/</b> This index page
<li><b>/backup/tables</b> Print list of tables
<li><b>/backup/list</b> Print list of backups
</ul>
</body></html>`

/*
   create          Create new backup
   upload          Upload backup to remote storage
   list            Print list of backups
   download        Download backup from remote storage
   restore         Create schema and restore data from backup
   delete          Delete specific backup
   default-config  Print default config
   freeze          Freeze tables
   clean           Remove data in 'shadow' folder
   server          Run API server
   help, h         Shows a list of commands or help for one command
*/
