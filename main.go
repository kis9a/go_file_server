package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"container/list"
	"context"
	"encoding/csv"
	"encoding/gob"
	"encoding/json"
	"flag"
	"fmt"
	"html/template"
	"io"
	"io/fs"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type Config struct {
	HOME                             string
	FS_ACCESS_LOG_EVICTION_INTERVAL  int
	FS_ACCESS_LOG_DIRECTORY          string
	FS_ADMIN_IP                      string
	FS_AUTH_PASSWORD                 string
	FS_AUTH_PATH                     string
	FS_AUTH_USER                     string
	FS_CHANGE_DIRECTORY              bool
	FS_CORS_ORIGINS                  string
	FS_DEBUG_MODE                    bool
	FS_FRECENCY_FILE_PATH            string
	FS_OPEN_CMD                      string
	FS_OPEN_MODE                     bool
	FS_RATE_LIMIT_COUNT              int
	FS_RATE_LIMIT_EVICTION_FILE_PATH string
	FS_RATE_LIMIT_INTERVAL           int
	FS_SERVER_HOST                   string
	FS_SERVER_PORT                   string
	FS_SIMPLE_MODE                   bool
	FS_TIME_FORMAT                   string
}

type CommandOption struct {
	serverPort string
	serverHost string
	simple     bool
	cd         bool
	debug      bool
	open       bool
}

type Server struct {
	address      string
	directory    string
	path         string
	urlPath      string
	query        url.Values
	files        []File
	config       *Config
	frecency     *Frecency
	accessLogger *AccessLogger
	rateLimiter  *RateLimiter
}

type File struct {
	Name          string    `json:"name"`
	IsDir         bool      `json:"isDir"`
	ModTime       time.Time `json:"modTime"`
	ModTimeString string    `json:"modTimeString"`
	Size          int64     `json:"size"`
	SizeString    string    `json:"SizeString"`
}

type RateLimiter struct {
	mutex            sync.RWMutex
	limitCount       int
	interval         int
	evictionFilePath string
	entry            map[string]int
	ctx              context.Context
	once             sync.Once
	done             chan struct{}
	lastCleanedTime  time.Time
}

type RateLimiterEvictionHeader struct {
	LastCleanedTime time.Time
	StopTime        time.Time
	Interval        int
	LimitCount      int
}

type RateLimiterEviction struct {
	Header RateLimiterEvictionHeader
	Entry  map[string]int
}

type AccessLogger struct {
	mutex                sync.RWMutex
	ctx                  context.Context
	done                 chan struct{}
	entry                *list.List
	evictionFileFormat   string
	evictionFile         *os.File
	maxEvicationFileSize int64
	evictionInterval     int
	logDirPath           string
	once                 sync.Once
}

type AccessLog struct {
	RmoteAddr  string `json:"remoteAddr"`
	Method     string `json:"method"`
	URL        string `json:"url"`
	Referer    string `json:"referer"`
	UserAgenet string `json:"userAgenet"`
}

type Frecency struct {
	mutex             sync.RWMutex
	frecs             []Frec
	frecencyFileItems []FrecencyFileItem
	filePath          string
	comma             rune
}

type Frec struct {
	score int
	path  string
}

type FrecencyFileItem struct {
	Path      string `csv:"path"`
	Rank      int    `csv:"rank"`
	TimeStamp int    `csv:"time"`
}

type SortKey int

const (
	SORT_KEY_NAME SortKey = iota
	SORT_KEY_SIZE
	SORT_KEY_TIME
	SORT_KEY_UNKNOWN
)

type SortMethod int

const (
	SORT_ASCENDING SortMethod = iota
	SORT_DESCENDING
	SORT_METHOD_UNKNOWN
)

var (
	flagSet  *flag.FlagSet
	localIps []net.IP

	// set at go build -ldflags '-X main.version=xxx'
	version = "0.0.0"
)

// server
func newServer(dir string, config *Config) (*Server, error) {
	// set server dir
	server := &Server{
		directory: dir,
		config:    config,
	}

	// check tcp addr
	addr := net.JoinHostPort(config.FS_SERVER_HOST, config.FS_SERVER_PORT)
	_, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return server, fmt.Errorf("Listen addr could not resolve: %s\n", addr)
	} else {
		server.address = addr
	}

	// initialize acess logger
	server.accessLogger = newAccessLogger(config.FS_ACCESS_LOG_EVICTION_INTERVAL, config.FS_ACCESS_LOG_DIRECTORY)

	// initialize rateLimiter
	server.rateLimiter = newRateLimiter(config.FS_RATE_LIMIT_COUNT, config.FS_RATE_LIMIT_INTERVAL, config.FS_RATE_LIMIT_EVICTION_FILE_PATH)

	// initialize frecency
	if !isExistPath(config.FS_FRECENCY_FILE_PATH) {
		f, err := os.Create(config.FS_FRECENCY_FILE_PATH)
		if err != nil {
			return server, fmt.Errorf("Failed initialize FrecencyFileDB path: %s, err: %v\n", config.FS_FRECENCY_FILE_PATH, err)
		}
		defer f.Close()
	}
	server.frecency = newFrecency(config.FS_FRECENCY_FILE_PATH)
	if err := server.frecency.clean(); err != nil {
		return server, fmt.Errorf("Failed frecency clean err: %v\n", err)
	}
	return server, err
}

func (s *Server) serve() error {
	var err error
	mux := s.routeHandler()
	log.Printf("Serving %s at %s", s.directory, s.address)

	if s.config.FS_OPEN_MODE {
		err := s.browserOpen("http://" + s.address)
		if err != nil {
			os.Stderr.WriteString("Failed browser open: " + err.Error() + "\n")
		}
	}

	// run access log eviction worker
	s.accessLogger.runEvictioner()
	defer s.accessLogger.stopEvictionWorker()

	// run rate limit clean worker
	s.rateLimiter.runLimitCleaner()
	defer s.rateLimiter.stoplimitCleanWorker()

	// server with graceful shutdown
	httpServer := &http.Server{
		Addr:              s.address,
		ReadHeaderTimeout: 15 * time.Second,
		Handler:           mux,
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, os.Interrupt)

	go func() {
		if err = httpServer.ListenAndServe(); err != nil {
			if err != http.ErrServerClosed {
				os.Stderr.WriteString("Failed on Listen And Server, error: " + err.Error() + "\n")
			}
		}
	}()

	<-stop

	serverCtx, serverCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer serverCancel()

	if err = httpServer.Shutdown(serverCtx); err != nil {
		return fmt.Errorf("Failed gracefully shutdown err: %v\n", err)
	}
	return err
}

func (s *Server) routeHandler() *http.ServeMux {
	mux := http.NewServeMux()

	// __fs
	fs := http.NewServeMux()
	fs.HandleFunc("/version", s.handleVersion)
	fs.HandleFunc("/frecs", s.handleFrecs)
	fs.HandleFunc("/files", s.handleFiles)
	fs.Handle("/cd", s.middlewareIpAuthorizer(http.HandlerFunc(s.handleCd)))

	// handle fs
	mux.Handle("/__fs/", http.StripPrefix("/__fs", s.commonMiddlewares(fs)))

	// handle favicon.ico
	mux.Handle("/favicon.ico", s.commonMiddlewares(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {},
	)))

	// ssr
	ssr := http.NewServeMux()
	ssr.HandleFunc("/", s.handleServer)

	// handle ssr
	mux.Handle("/", s.commonMiddlewares(s.middlewareFrecency(ssr)))
	return mux
}

func (s *Server) commonMiddlewares(h http.Handler) http.Handler {
	return s.middlewareCors(
		s.middlewareUrlParser(
			s.middlewareAccessLogger(
				s.middlewareRateLimiter(
					s.middlewareBasicAuthenticator(h),
				),
			),
		),
	)
}

func (s *Server) middlewareUrlParser(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error
		s.urlPath, err = url.PathUnescape(r.URL.Path)
		if err != nil {
			os.Stderr.WriteString(fmt.Sprintf("Failed to path unescape request path %v\n", err))
			http.Error(w, fmt.Sprintf("Failed to path unescape request path %v", err), http.StatusInternalServerError)
			return
		}
		pathBefore := s.path
		s.path = path.Join(s.directory, s.urlPath)
		s.query = r.URL.Query()
		h.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), "pathBefore", pathBefore)))
	})
}

func (s *Server) middlewareAccessLogger(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Printf("%s: %s %s", r.RemoteAddr, r.Method, r.Host+r.RequestURI)
		s.accessLogger.pushBack(AccessLog{
			RmoteAddr:  r.RemoteAddr,
			Method:     r.Method,
			URL:        r.URL.RequestURI(),
			Referer:    r.Referer(),
			UserAgenet: r.UserAgent(),
		})
		h.ServeHTTP(w, r)
	})
}

func (s *Server) middlewareCors(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", s.config.FS_CORS_ORIGINS)
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
		w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		h.ServeHTTP(w, r)
	})
}

func (s *Server) middlewareRateLimiter(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip, err := getReqIpAddress(r)
		if err != nil {
			os.Stderr.WriteString(fmt.Sprintf("Failed get remote IP: %v\n", err))
		}
		if s.rateLimiter.isRateLimited(ip.String()) {
			os.Stderr.WriteString(fmt.Sprintf("Rate limited, ip: %s\n", ip))
			http.Error(w, fmt.Sprintf("Rate Lmited, ip: %s\n", ip), http.StatusTooManyRequests)
			return
		} else {
			s.rateLimiter.incrementCount(ip.String())
		}
		h.ServeHTTP(w, r)
	})
}

func (s *Server) middlewareBasicAuthenticator(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.config.FS_AUTH_PATH != "" {
			authPath := s.config.FS_AUTH_PATH
			authPath = strings.TrimSuffix(authPath, "/")

			if strings.HasPrefix(s.path, authPath) {
				if !s.isBasicAuthorized(r) {
					w.Header().Add("WWW-Authenticate", `Basic realm="Auth path"`)
					w.WriteHeader(http.StatusUnauthorized)
					os.Stderr.WriteString(fmt.Sprintf("Authentication failed\n"))
					http.Error(w,
						"Authentication failed, Administrators should check the FS_AUTH_PATH, FS_AUTH_USER, and FS_AUTH_PASSWORD env vars",
						http.StatusInternalServerError,
					)
					return
				}
			}
		}
		h.ServeHTTP(w, r)
	})
}

func (s *Server) middlewareIpAuthorizer(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authed, err := s.isIpAuthorized(r)
		if err != nil {
			os.Stderr.WriteString(fmt.Sprintf("Failed check ip authorization body %v\n", err))
			http.Error(w, fmt.Sprintf("Failed check ip authorization %v\n", err), http.StatusInternalServerError)
			return
		}
		if !authed {
			ip, err := getReqIpAddress(r)
			if err != nil {
				os.Stderr.WriteString(fmt.Sprintf("Failed get remote IP: %v\n", err))
			}
			os.Stderr.WriteString(fmt.Sprintf("Not an allowed IP: %s\n", ip.String()))
			http.Error(w, "Not an allowed IP", http.StatusUnauthorized)
			return
		}
		h.ServeHTTP(w, r)
	})
}

func (s *Server) middlewareFrecency(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pathBefore := r.Context().Value("pathBefore")
		if pathBefore == nil {
			os.Stderr.WriteString(fmt.Sprintf("No value in context %v\n", "pathBefore"))
		} else {
			if r.Method == http.MethodGet {
				if pathBefore != s.path && isExistDirectory(s.path) {
					if err := s.frecency.add(s.path); err != nil {
						os.Stderr.WriteString(fmt.Sprintf("Failed frecency add path: %s, err: %v\n", s.path, err))
					}
				}
				s.handleServer(w, r)
				return
			}
		}
		h.ServeHTTP(w, r)
	})
}

func (s *Server) handleCd(w http.ResponseWriter, r *http.Request) {
	type Body struct {
		Path string `json:"path"`
	}
	var body Body
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		os.Stderr.WriteString(fmt.Sprintf("Failed decode request body %v\n", err))
		http.Error(w, fmt.Sprintf("Failed decode request body %v\n", err), http.StatusBadRequest)
		return
	}

	if filepath.IsAbs(body.Path) {
		s.directory = body.Path
	} else {
		dir, err := filepath.Abs(filepath.Join(s.directory, body.Path))
		if err != nil {
			os.Stderr.WriteString(fmt.Sprintf("Failed make absolute path %v\n", err))
			http.Error(w, fmt.Sprintf("Failed make absolute path %v", err), http.StatusInternalServerError)
			return
		} else {
			s.directory = dir
		}
	}

	if !isExistDirectory(s.directory) {
		os.Stderr.WriteString(fmt.Sprintf("No such directory %s", s.directory))
		http.Error(w, fmt.Sprintf("No such directory"), http.StatusBadRequest)
		return
	}

	if err := s.frecency.add(s.directory); err != nil {
		os.Stderr.WriteString(fmt.Sprintf("Failed frecency add %v", err))
		http.Error(w, fmt.Sprintf("Failed frecency add %v", err), http.StatusInternalServerError)
		return
	}
}

func (s *Server) handleVersion(w http.ResponseWriter, r *http.Request) {
	type VersionRes struct {
		Version string `json:"version"`
	}
	j, err := json.Marshal(VersionRes{
		Version: version,
	})
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("Failed json marshal %v\n", err))
		http.Error(w, fmt.Sprintf("Failed json marshal %v", err), http.StatusInternalServerError)
	}
	w.Write(j)
}

func (s *Server) handleFrecs(w http.ResponseWriter, r *http.Request) {
	if err := s.frecency.read(); err != nil {
		os.Stderr.WriteString(fmt.Sprintf("Failed read fracs err: %v\n", err))
		http.Error(w, fmt.Sprintf("Failed read fracs err: %v\n", err), http.StatusInternalServerError)
	}

	j, err := json.Marshal(s.frecency.getFrecs(30))
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("Failed json marshal %v\n", err))
		http.Error(w, fmt.Sprintf("Failed json marshal %v", err), http.StatusInternalServerError)
	}
	w.Write(j)
}

func (s *Server) handleFiles(w http.ResponseWriter, r *http.Request) {
	p := strings.TrimPrefix(s.urlPath, "/__fs/files")
	p = filepath.Join(s.directory, p)

	// check path exsits
	f, err := os.Open(p)
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("Failed to open path, err: %v\n", err))
		http.Error(w, fmt.Sprintf("Failed to open path, err: %v", err), http.StatusInternalServerError)
		return
	}
	defer f.Close()

	fs, err := f.Stat()
	if fs.IsDir() {
		s.files, err = s.readDirectoryFiles(f)
		if err != nil {
			os.Stderr.WriteString(fmt.Sprintf("Failed read directory files %v\n", err))
			http.Error(w, fmt.Sprintf("Failed read directory files %v", err), http.StatusInternalServerError)
			return
		}

		j, err := json.Marshal(s.files)
		if err != nil {
			os.Stderr.WriteString(fmt.Sprintf("Failed json marshal %v\n", err))
			http.Error(w, fmt.Sprintf("Failed json marshal %v", err), http.StatusInternalServerError)
		}
		w.Write(j)
	} else {
		j, err := json.Marshal(File{
			Name:          fs.Name(),
			IsDir:         fs.IsDir(),
			Size:          fs.Size(),
			ModTime:       fs.ModTime(),
			ModTimeString: fs.ModTime().Format(s.config.FS_TIME_FORMAT),
			SizeString:    fileSize(fs.Size()),
		})
		if err != nil {
			os.Stderr.WriteString(fmt.Sprintf("Failed json marshal %v\n", err))
			http.Error(w, fmt.Sprintf("Failed json marshal %v", err), http.StatusInternalServerError)
		}
		w.Write(j)
	}
}

func (s *Server) handleServer(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")

	if !isExistPath(s.path) {
		os.Stderr.WriteString(fmt.Sprintf("No such file or directory %s\n", s.path))
		http.Error(w, "file not found", http.StatusNotFound)
		return
	}

	f, err := os.Open(s.path)
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("Failed to open path %v\n", err))
		http.Error(w, fmt.Sprintf("Failed to open path %v", err), http.StatusInternalServerError)
		return
	}
	defer f.Close()

	if isExistDirectory(s.path) {
		s.files, err = s.readDirectoryFiles(f)
		if err != nil {
			os.Stderr.WriteString(fmt.Sprintf("Failed read directory files %v\n", err))
			http.Error(w, fmt.Sprintf("Failed read directory files %v", err), http.StatusInternalServerError)
			return
		}

		sortKey := s.getSortKey(s.query.Get("sortKey"))
		sortMethod := s.getSortMethod(s.query.Get("sortMethod"))

		if sortKey != SORT_KEY_UNKNOWN && sortMethod != SORT_METHOD_UNKNOWN {
			switch sortKey {
			case SORT_KEY_NAME:
				s.sortByFileName(sortMethod)
			case SORT_KEY_SIZE:
				s.sortByFileSize(sortMethod)
			case SORT_KEY_TIME:
				s.sortByFileTime(sortMethod)
			}
		}

		if err = s.renderDirectory(f, w); err != nil {
			os.Stderr.WriteString(fmt.Sprintf("Failed render directory %v\n", err))
			http.Error(w, fmt.Sprintf("Failed render directory %v", err), http.StatusInternalServerError)
			return
		}
	} else {
		if err = s.renderFile(f, w, r); err != nil {
			os.Stderr.WriteString(fmt.Sprintf("Failed render file %v\n", err))
			http.Error(w, fmt.Sprintf("Failed render file %v", err), http.StatusInternalServerError)
			return
		}
	}
}

func (s *Server) adaptLayout(w http.ResponseWriter, body string) (string, error) {
	const baseTmpl = `
<!DOCTYPE html>
<html>
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=dev ice-width, initial-scale=1.0" />
    <title>fs {{ .Path }}</title>
    <link rel="icon" href="data:," />
    <style>
      * {
        font-family: monospace;
      }
      body {
        display: block;
        margin: 0.8rem;
      }
      ul {
        display: table;
        list-style-type: none;
        border-collapse: separate;
        border-spacing: 4px 4px;
        padding: 0;
        width: 100%;
      }
      li {
        display: table-row;
      }
      li > * {
        display: table-cell;
        margin-right: 0.5rem;
      }
      li > *:last-child {
        margin-right: 0;
      }
      .up, .frec {
        color: #0000EE;
        text-decoration: underline;
      }
      .up:hover, .frec:hover {
        cursor: pointer;
      }
      .nav {
        margin-bottom: 1rem;
      }
      .colh span {
        font-weight: bold;
      }
      .colh span:hover {
        text-decoration: underline;
        cursor: pointer;
      }
      .path, .frecs {
        font-weight: bold;
        margin-bottom: 4px;
      }
      .path span, .frecs span {
        font-weight: bold;
      }
      .path span:hover, .frecs span:hover {
        text-decoration: underline;
        cursor: pointer;
      }
    </style>
  </head>
  <body>
    {{ .Body -}}
    <script>
      function files() {
        const q = new URLSearchParams(window.location.search);
        const path = window.location.pathname.split("?")[0];
        if (q.toString()) {
          window.location.href = "/__fs/files/" + [path, q.toString()].join("?")
        } else {
          window.location.href = "/__fs/files/" + path
        }
      }
      function frecs() {
        const q = new URLSearchParams(window.location.search);
        const path = window.location.pathname.split("?")[0];
        if (q.toString()) {
          window.location.href = "/__fs/frecs/" + [path, q.toString()].join("?")
        } else {
          window.location.href = "/__fs/frecs/" + path
        }
      }
      function s(key) {
        const q = new URLSearchParams(window.location.search);
        const path = window.location.href.split("?")[0];
        if (q.has("sortMethod")) {
          if (q.get("sortKey") == key) {
            if (q.get("sortMethod") == "ascending") {
              q.set("sortMethod", "descending");
            } else {
              q.set("sortMethod", "ascending");
            }
          }
        } else {
          q.set("sortMethod", "ascending");
        }
        q.set("sortKey", key);
        window.location.href = path + "?" + q.toString();
      }
      function cd(path) {
        fetch("/__fs/cd", {
          method: "POST",
          headers: {
            Accept: "application/json",
            "Content-Type": "application/json",
          },
          body: JSON.stringify({ path: path }),
        }).then((r) => {
          if (r.status === 200) {
            window.location.href = "/"
          } else {
            console.log(r);
          }
        });
      }
    </script>
  </body>
</html>
`

	type Template struct {
		Path string
		Body template.HTML
	}

	tmpl, err := template.New("").Parse(baseTmpl)
	if err != nil {
		return "", err
	}

	var b bytes.Buffer
	tmpl.Execute(&b, Template{
		Path: s.path,
		Body: template.HTML(body),
	})

	return b.String(), err
}

func (s *Server) renderDirectory(f *os.File, w http.ResponseWriter) error {
	html, err := os.Open(path.Join(s.path, "index.html"))
	if err == nil {
		io.Copy(w, html)
		os.Stderr.WriteString(fmt.Sprintf("Failed write index.html %v\n", err))
		html.Close()
		return err
	}
	defer html.Close()
	html.Close()

	absDir, err := filepath.Abs(s.directory)
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("Failed make absolute path %v\n", err))
		http.Error(w, fmt.Sprintf("Failed make absolute path %v", err), http.StatusInternalServerError)
		return err
	}

	absPath, err := filepath.Abs(s.path)
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("Could not get directory absolute path %v\n", err))
		http.Error(w, fmt.Sprintf("Could not get directory absolute path %v", err), http.StatusInternalServerError)
		return err
	}

	relPath, err := filepath.Rel(absDir, absPath)
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("Could not get directory rel path %v\n", err))
		http.Error(w, fmt.Sprintf("Could not get directory rel path %v", err), http.StatusInternalServerError)
		return err
	}

	absDirName := absDir
	if strings.HasPrefix(absDir, s.config.HOME) {
		absDirName = strings.ReplaceAll(absDir, s.config.HOME, "~")
	}

	queryString := s.query.Encode()
	if queryString != "" {
		queryString = "?" + queryString
	}

	var wb bytes.Buffer
	wb.WriteString("<div class=\"path\"><span onclick=\"files()\">PATH</span></div>\n")
	wb.WriteString(fmt.Sprintf("<div class=\"nav\">\n<span><a href=\"/%s\">%s</a></span>\n", queryString, absDirName))

	relPathPwd := "."
	splitedPaths := splitPath(relPath)
	var ps []string
	for k, p := range splitedPaths {
		ps = append(ps, p)
		if p != relPathPwd {
			if s.directory == "/" && k == 0 {
				wb.WriteString(fmt.Sprintf("<span> </span><span><a href=\"/%s/%s\">%s</a></span>\n", strings.Join(ps, "/"), queryString, p))
			} else {
				wb.WriteString(fmt.Sprintf("<span>/</span><span><a href=\"/%s/%s\">%s</a></span>\n", strings.Join(ps, "/"), queryString, p))
			}
		}
	}

	wb.WriteString("</div>\n<ul>\n")
	if s.config.FS_SIMPLE_MODE {
		wb.WriteString("<li class=\"colh\"><div><span onclick=\"s('name')\">FILES</span></div></li>\n")
	} else {
		wb.WriteString("<li class=\"colh\"><div><span onclick=\"s('name')\">FILES</span></div>\n<div><span onclick=\"s('size')\">SIZE</span></div>\n<div><span onclick=\"s('time')\">TIME</span></div></li>\n")
	}

	if s.directory != "/" && s.urlPath == "/" && s.config.FS_CHANGE_DIRECTORY {
		wb.WriteString(fmt.Sprintf("<li><span class=\"up\" onclick=\"cd('..')\">%s/</span></li>\n", ".."))
	}

	for _, fi := range s.parentFiles() {
		fiPath := filepath.Join(url.PathEscape(fi.Name))
		wb.WriteString(fmt.Sprintf("<li><span><a href=\"%s/%s\">%s/</a></span></li>\n", fiPath, queryString, fi.Name))
	}

	for _, fi := range s.files {
		fiPath := filepath.Join(url.PathEscape(fi.Name))

		if fi.IsDir {
			if s.config.FS_SIMPLE_MODE {
				wb.WriteString(fmt.Sprintf("<li><span><a href=\"%s/%s\">%s/</a></span></li>\n", fiPath, queryString, fi.Name))
			} else {
				wb.WriteString(fmt.Sprintf("<li><span><a href=\"%s/%s\">%s/</a></span><span class=\"size\">%s</span><span class=\"modTime\">%s</span></li>\n", fiPath, queryString, fi.Name, fi.SizeString, fi.ModTimeString))
			}
		} else {
			if s.config.FS_SIMPLE_MODE {
				wb.WriteString(fmt.Sprintf("<li><span><a href=\"%s/%s\">%s</a></span></li>\n", fiPath, queryString, fi.Name))
			} else {
				wb.WriteString(fmt.Sprintf("<li><span><a href=\"%s/%s\">%s</a></span><span class=\"size\">%s</span><span class=\"modTime\">%s</span></li>\n", fiPath, queryString, fi.Name, fi.SizeString, fi.ModTimeString))
			}
		}
	}
	wb.WriteString("</ul>\n")

	if s.config.FS_CHANGE_DIRECTORY {
		wb.WriteString("<ul>\n")
		wb.WriteString("<li class=\"frecs colh\"><div><span onclick=\"frecs()\">FRECS</span></div></li>\n")
		for _, fi := range s.frecency.getFrecs(30) {
			fiPath := strings.ReplaceAll(fi.path, s.config.HOME, "~")
			wb.WriteString(fmt.Sprintf("<li><span class=\"frec\" score=\"%d\" onclick=\"cd('%s')\">%s/</span></li>\n", fi.score, fi.path, fiPath))
		}
		wb.WriteString("</ul>\n")
	}

	htmlString, err := s.adaptLayout(w, wb.String())
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("Failed render base %v\n", err))
		http.Error(w, fmt.Sprintf("Failed render base %v", err), http.StatusInternalServerError)
		return err
	}
	_, err = io.WriteString(w, htmlString)
	return err
}

func (s *Server) renderFile(f *os.File, w http.ResponseWriter, r *http.Request) error {
	http.ServeContent(w, r, s.path, time.Time{}, f)
	return nil
}

func (s *Server) getSortKey(key string) SortKey {
	switch key {
	case "name":
		return SORT_KEY_NAME
	case "size":
		return SORT_KEY_SIZE
	case "time":
		return SORT_KEY_TIME
	}
	return SORT_KEY_UNKNOWN
}

func (s *Server) getSortMethod(method string) SortMethod {
	switch method {
	case "ascending":
		return SORT_ASCENDING
	case "descending":
		return SORT_DESCENDING
	}
	return SORT_METHOD_UNKNOWN
}

func (s *Server) sortByFileName(method SortMethod) {
	switch method {
	case SORT_ASCENDING:
		sort.Slice(s.files, func(i, j int) bool {
			return s.files[i].Name < s.files[j].Name
		})
		return
	case SORT_DESCENDING:
		sort.Slice(s.files, func(i, j int) bool {
			return s.files[i].Name > s.files[j].Name
		})
		return
	}
}

func (s *Server) sortByFileSize(method SortMethod) {
	switch method {
	case SORT_ASCENDING:
		sort.Slice(s.files, func(i, j int) bool {
			return s.files[i].Size < s.files[j].Size
		})
		return
	case SORT_DESCENDING:
		sort.Slice(s.files, func(i, j int) bool {
			return s.files[i].Size > s.files[j].Size
		})
		return
	}
}

func (s *Server) sortByFileTime(method SortMethod) {
	switch method {
	case SORT_ASCENDING:
		sort.Slice(s.files, func(i, j int) bool {
			return s.files[i].ModTime.Before(s.files[j].ModTime)
		})
		return
	case SORT_DESCENDING:
		sort.Slice(s.files, func(i, j int) bool {
			return s.files[i].ModTime.After(s.files[j].ModTime)
		})
		return
	}
}

func (s *Server) parentFiles() []File {
	if isExistDirectory(path.Join(s.directory, "..")) && s.urlPath != "/" {
		var files []File
		return append(files, File{
			Name:  "..",
			IsDir: true,
		})
	} else {
		return []File{}
	}
}

func (s *Server) readDirectoryFiles(f *os.File) ([]File, error) {
	fs, err := f.Readdir(-1)
	files := []File{}
	for _, f := range fs {
		files = append(files, File{
			Name:          f.Name(),
			IsDir:         f.IsDir(),
			Size:          f.Size(),
			ModTime:       f.ModTime(),
			ModTimeString: f.ModTime().Format(s.config.FS_TIME_FORMAT),
			SizeString:    fileSize(f.Size()),
		})
	}
	return files, err
}

func (s *Server) browserOpen(url string) error {
	var err error
	openCmd := s.config.FS_OPEN_CMD
	if openCmd != "" {
		exec.Command(openCmd, url).Output()
	} else {
		switch runtime.GOOS {
		case "linux":
			_, err = exec.Command("xdg-open", url).Output()
			return err
		case "windows":
			_, err = exec.Command("rundll32", "url.dll,FileProtocolHandler", url).Output()
			return err
		case "darwin":
			_, err := exec.Command("open", url).Output()
			return err
		default:
			return fmt.Errorf("Can't suggest platform\nPlease nset os environment $OPEN_CMD\n")
		}
	}
	return nil
}

func (s *Server) isBasicAuthorized(r *http.Request) bool {
	user, pw, ok := r.BasicAuth()
	return ok && user == s.config.FS_AUTH_USER && pw == s.config.FS_AUTH_PASSWORD
}

func (s *Server) isIpAuthorized(r *http.Request) (bool, error) {
	authed := false
	remoteIp, err := getReqIpAddress(r)
	if err != nil {
		return authed, err
	}
	if s.config.FS_ADMIN_IP == "" {
		localIps, err := getLocalIps()
		if err != nil {
			return authed, err
		}
		for _, l := range localIps {
			if remoteIp.Equal(l) {
				authed = true
				return authed, err
			}
		}
	} else {
		adminIp := net.ParseIP(s.config.FS_ADMIN_IP)
		authed = remoteIp.Equal(adminIp)
	}
	return authed, err
}

// frecency
func newFrecency(path string) *Frecency {
	return &Frecency{
		comma:    '|',
		filePath: path,
	}
}

func (f *Frecency) read() error {
	var items []FrecencyFileItem
	r, err := os.ReadFile(f.filePath)
	if err != nil {
		return fmt.Errorf("Counld not read file path: %s, err %v\n", f.filePath, err)
	}

	cr := csv.NewReader(bytes.NewReader(r))
	f.mutex.RLock()
	cr.Comma = f.comma
	csvs, err := cr.ReadAll()
	f.mutex.RUnlock()
	if err != nil {
		return fmt.Errorf("Counld not read csv: %s, err %v\n", f.filePath, err)
	}

	for _, c := range csvs {
		if len(c) == 3 {
			rank, err := strconv.Atoi(c[1])
			if err != nil {
				os.Stdout.WriteString("Ignore invalid type column `rank`: %s" + c[1] + "\n")
			}
			timeStamp, err := strconv.Atoi(c[2])
			if err != nil {
				os.Stdout.WriteString("Ignore invalid type column `timeStamp`: %s" + c[1] + "\n")
			}
			items = append(items, FrecencyFileItem{
				Path:      c[0],
				Rank:      rank,
				TimeStamp: timeStamp,
			})
		} else {
			os.Stdout.WriteString("Ignore invalid length row: %s" + strings.Join(c, "|"))
		}
	}

	f.frecencyFileItems = items
	return err
}

func (f *Frecency) backoff(retries int) time.Duration {
	maxDelay := 200 * time.Millisecond
	delay := time.Duration(1<<uint(retries))*time.Millisecond + time.Duration(rand.Int63n(int64(time.Millisecond)))
	if delay > maxDelay {
		delay = maxDelay
	}
	time.Sleep(delay)
	return delay
}

func (f *Frecency) save() error {
	var csvs [][]string
	for _, v := range f.frecencyFileItems {
		csvs = append(csvs, []string{v.Path, fmt.Sprintf("%d", v.Rank), fmt.Sprintf("%d", v.TimeStamp)})
	}
	f.mutex.RLock()
	fs, err := os.Create(f.filePath)
	if err != nil {
		return fmt.Errorf("Counld not create file path: %s, err %v\n", f.filePath, err)
	}
	f.mutex.RUnlock()
	defer fs.Close()
	retries := 5
	for i := 0; i < retries; i++ {
		w := csv.NewWriter(fs)
		f.mutex.Lock()
		w.Comma = f.comma
		err = w.WriteAll(csvs)
		f.mutex.Unlock()
		if err == nil {
			return nil
		}
		f.backoff(i)
	}
	return fmt.Errorf("Counld not write file path: %s, err %v\n", f.filePath, err)
}

func (f *Frecency) add(path string) error {
	var err error
	var newItems []FrecencyFileItem

	if !filepath.IsAbs(path) {
		path, err = filepath.Abs(path)
		if err != nil {
			return fmt.Errorf("Failed make absolute path: %s, err: %v\n", path, err)
		}
	}

	if err := f.read(); err != nil {
		return fmt.Errorf("Failed read fracs path: %s, err: %v\n", path, err)
	}

	isExist := false
	for _, v := range f.frecencyFileItems {
		if v.Path == path {
			v.Rank = v.Rank + 1
			v.TimeStamp = int(time.Now().Unix())
			isExist = true
		}

		if v.Rank >= 1 && isExistDirectory(v.Path) {
			newItems = append(newItems, v)
		}
	}
	f.frecencyFileItems = newItems

	if !isExist {
		f.frecencyFileItems = append(newItems, FrecencyFileItem{
			Path:      path,
			Rank:      1,
			TimeStamp: int(time.Now().Unix()),
		})
	}

	return f.save()
}

func (f *Frecency) frecent(rank int, time int, now int) int {
	dxr := float64(now-time) / 86.4
	return int(10000 * float64(rank) * (3.75 / ((0.0001*dxr + 1) + 0.25)))
}

func (f *Frecency) clean() error {
	var newItems []FrecencyFileItem

	for _, v := range f.frecencyFileItems {
		now := int(time.Now().Unix())
		score := f.frecent(v.Rank, v.TimeStamp, now)

		if score > 9000 {
			newItems = append(newItems, v)
		}
	}
	f.frecencyFileItems = newItems

	if err := f.save(); err != nil {
		return fmt.Errorf("Failed save frecency file items: %v\n", err)
	}
	return nil
}

func (f *Frecency) getFrecs(limit int) []Frec {
	var frecs []Frec

	for i, v := range f.frecencyFileItems {
		now := int(time.Now().Unix())
		score := f.frecent(v.Rank, v.TimeStamp, now)
		frecs = append(frecs, Frec{
			score: score,
			path:  v.Path,
		})

		if i == limit-1 {
			break
		}
	}

	sort.Slice(frecs, func(i, j int) bool {
		return frecs[i].score > frecs[j].score
	})
	return frecs
}

// AccessLogger
func newAccessLogger(interval int, logDirPath string) *AccessLogger {
	return &AccessLogger{
		ctx:                  context.Background(),
		done:                 make(chan struct{}),
		entry:                list.New(),
		evictionFileFormat:   "2006-01-02",
		evictionInterval:     interval,
		maxEvicationFileSize: 20000000, // 20MB
		logDirPath:           logDirPath,
		once:                 sync.Once{},
	}
}

func (a *AccessLogger) flushallEntry() {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	a.entry = list.New()
}

func (a *AccessLogger) checkRequiredVariables() bool {
	if a.logDirPath == "" || a.evictionInterval < 1 {
		return false
	} else {
		if isExistDirectory(a.logDirPath) {
			return true
		} else {
			os.Stderr.WriteString(fmt.Sprintf("No such directory, path: %s\n", a.logDirPath))
			return false
		}
	}
}

func (a *AccessLogger) evictionAccessLogs() error {
	if !a.checkRequiredVariables() {
		return nil
	}
	for e := a.entry.Front(); e != nil; e = e.Next() {
		j, err := json.Marshal(e.Value.(AccessLog))
		if err != nil {
			return fmt.Errorf("Failed json marshal, err: %v\n", err)
		}
		_, err = a.evictionFile.Write(append(j, []byte("\n")...))
		if err != nil {
			return fmt.Errorf("Failed write to evication file, err: %v\n", err)
		}
	}
	over, err := a.isEvicationFileSizeOverMax()
	if err != nil {
		return fmt.Errorf("Failed check is evication file size over max, err: %v", err)
	}
	if over {
		index, err := a.getEvicationFileIndex()
		if err != nil {
			return fmt.Errorf("Failed get evication file index, err: %v", err)
		}
		newIndex := index + 1
		err = a.compressAndSaveEvictionFile(a.evictionFile.Name())
		if err != nil {
			return fmt.Errorf("Failed compress and save evication file, err: %v", err)
		}
		now := time.Now()
		logFileName := now.Format(a.evictionFileFormat)
		logFilePath := filepath.Join(a.logDirPath, fmt.Sprintf("%s-%d", logFileName, newIndex))
		if f, err := os.Create(logFilePath); err != nil {
			defer f.Close()
			return fmt.Errorf("Failed create log file, path: %v, err: %v\n", logFilePath, err)
		} else {
			a.evictionFile = f
			os.Stdout.WriteString(fmt.Sprintf("Created log file, path: %v\n", logFilePath))
		}
	}
	return nil
}

func (a *AccessLogger) isEvicationFileSizeOverMax() (bool, error) {
	fi, err := a.evictionFile.Stat()
	if err != nil {
		return false, err
	}
	if a.maxEvicationFileSize < fi.Size() {
		return true, err
	} else {
		return false, err
	}
}

func (a *AccessLogger) compressAndSaveEvictionFile(filePath string) error {
	source, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("Failed open evcation log, path: %v, err: %v\n", filePath, err)
	}
	defer source.Close()
	rotatedPath := filepath.Join(filepath.Dir(filePath), "rotated")
	if !isExistDirectory(rotatedPath) {
		if err := os.MkdirAll(rotatedPath, os.ModePerm); err != nil {
			return fmt.Errorf("Failed make log rotated directory, path: %v, err: %v\n", rotatedPath, err)
		} else {
			os.Stdout.WriteString(fmt.Sprintf("Make log rotated directory path: %v\n", rotatedPath))
		}
	}
	path := filepath.Join(rotatedPath, filepath.Base(filePath)+".gz")
	dest, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("Failed create rotated log file path: %v, err: %v\n", path, err)
	}
	defer dest.Close()
	gzipWriter := gzip.NewWriter(dest)
	_, err = io.Copy(gzipWriter, source)
	if err != nil {
		return fmt.Errorf("Failed copy gzipWriter, source path: %v, path: %v, err: %v\n", filePath, path, err)
	}
	defer gzipWriter.Close()
	err = os.Remove(filePath)
	if err != nil {
		return fmt.Errorf("Failed to remove uncompressed log file, path: %s, err: %v\n", filePath, err)
	}
	return err
}

func (a *AccessLogger) rotateEvictionFiles() error {
	var err error
	if a.evictionFile != nil {
		return err
	}
	logFiles, err := a.getEvictionFiles()
	if err != nil {
		return fmt.Errorf("Failed get eviction files, err: %v\n", err)
	}
	for _, file := range logFiles {
		if file.Name() != a.evictionFile.Name() {
			a.compressAndSaveEvictionFile(file.Name())
		}
	}
	return err
}

func (a *AccessLogger) setEvictionFile() error {
	now := time.Now()
	logFileName := now.Format(a.evictionFileFormat)
	index, err := a.evictionFileLastIndex()
	if err != nil {
		return fmt.Errorf("Failed get log file index, err: %v\n", err)
	}
	logFilePath := filepath.Join(a.logDirPath, fmt.Sprintf("%s-%d", logFileName, index))
	if !isExistPath(logFilePath) {
		if f, err := os.Create(logFilePath); err != nil {
			defer f.Close()
			return fmt.Errorf("Failed create log file, path: %v, err: %v\n", logFilePath, err)
		} else {
			os.Stdout.WriteString(fmt.Sprintf("Created log file, path: %v\n", logFilePath))
		}
	}
	file, err := os.OpenFile(logFilePath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, os.ModeAppend)
	if err != nil {
		return fmt.Errorf("Failed open log file, err: %v\n", err)
	}
	a.evictionFile = file
	return nil
}

func (a *AccessLogger) evictionFileClose() {
	if a.evictionFile != nil {
		a.evictionFile.Close()
	}
}

func (a *AccessLogger) getEvictionFiles() ([]fs.FileInfo, error) {
	files, err := ioutil.ReadDir(a.logDirPath)
	if err != nil {
		return files, fmt.Errorf("Failed read log directory, err: %v\n", err)
	}
	now := time.Now()
	prefix := now.Format(a.evictionFileFormat)
	var logFiles []fs.FileInfo
	for _, file := range files {
		if !file.IsDir() && strings.HasPrefix(file.Name(), prefix) {
			logFiles = append(logFiles, file)
		}
	}
	return logFiles, err
}

func (a *AccessLogger) getEvicationFileIndex() (int, error) {
	str := a.evictionFile.Name()
	if len(str) < 1 {
		return 0, fmt.Errorf("Invalid eviction file name, name: %s", a.evictionFile.Name())
	}
	lastChar := string(str[len(str)-1])
	index, err := strconv.Atoi(lastChar)
	if err != nil {
		return 0, fmt.Errorf("Failed parser to int, string: %v", lastChar)
	}
	return index, nil
}

func (a *AccessLogger) evictionFileLastIndex() (int, error) {
	logFiles, err := a.getEvictionFiles()
	if err != nil {
		return 0, fmt.Errorf("Failed get eviction files, err: %v\n", err)
	}
	var indexes []int
	now := time.Now()
	prefix := now.Format(a.evictionFileFormat) + "-"
	for _, file := range logFiles {
		indexStr := strings.TrimPrefix(file.Name(), prefix)
		index, err := strconv.Atoi(indexStr)
		if err == nil {
			indexes = append(indexes, index)
		}
	}
	if len(indexes) == 0 {
		return 0, nil
	}
	sort.Ints(indexes)
	lastIndex := indexes[len(indexes)-1]
	return lastIndex, err
}

func (a *AccessLogger) runEvictioner() error {
	if !a.checkRequiredVariables() {
		return nil
	} else {
		if err := a.setEvictionFile(); err != nil {
			return fmt.Errorf("Failed set eviction file, err: %v\n", err)
		}
		if err := a.rotateEvictionFiles(); err != nil {
			return fmt.Errorf("Failed rotate eviction files, err: %v\n", err)
		}
		a.runEvictionWorker()
	}
	return nil
}

func (a *AccessLogger) runEvictionWorker() {
	go func() {
		ticker := time.NewTicker(time.Duration(a.evictionInterval) * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := a.evictionAccessLogs(); err != nil {
					os.Stderr.WriteString(fmt.Sprintf("Failed evication access logs, err: %v\n", err))
				}
				a.flushallEntry()
			case <-a.done:
				if err := a.evictionAccessLogs(); err != nil {
					os.Stderr.WriteString(fmt.Sprintf("Failed evication access logs, err: %v\n", err))
				}
				a.flushallEntry()
				return
			case <-a.ctx.Done():
				a.stopEvictionWorker()
			}
		}
	}()
}

func (a *AccessLogger) stopEvictionWorker() {
	a.once.Do(func() {
		if err := a.evictionAccessLogs(); err != nil {
			os.Stderr.WriteString(fmt.Sprintf("Failed evication access logs, err: %v\n", err))
		}
		a.evictionFileClose()
		close(a.done)
	})
}

func (a *AccessLogger) pushBack(log AccessLog) {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	a.entry.PushBack(log)
}

// RateLimiter
func newRateLimiter(limitCount int, interval int, evictionFilePath string) *RateLimiter {
	return &RateLimiter{
		entry:            make(map[string]int),
		limitCount:       limitCount,
		interval:         interval,
		evictionFilePath: evictionFilePath,
		lastCleanedTime:  time.Now(),
		once:             sync.Once{},
		ctx:              context.Background(),
		done:             make(chan struct{}),
	}
}

func (r *RateLimiter) setCount(key string, value int) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.entry[key] = value
}

func (r *RateLimiter) getCount(key string) (int, bool) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	value, ok := r.entry[key]
	return value, ok
}

func (r *RateLimiter) deleteCount(key string) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	delete(r.entry, key)
}

func (r *RateLimiter) flushallEntry() {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.entry = make(map[string]int)
}

func (r *RateLimiter) applyEvictionEntry(ev RateLimiterEviction) bool {
	if ev.Header.StopTime.IsZero() {
		return false
	}
	elapsedTime := ev.Header.LastCleanedTime.Sub(ev.Header.StopTime)
	if int(elapsedTime.Seconds()) >= ev.Header.Interval {
		return false
	} else {
		r.entry = ev.Entry
		return true
	}
}

func (r *RateLimiter) checkRequiredVariables() bool {
	if r.evictionFilePath == "" || r.interval < 1 || r.limitCount < 1 {
		return false
	} else {
		return true
	}
}

func (r *RateLimiter) readEvictionEntry() (RateLimiterEviction, error) {
	var err error
	var evictions RateLimiterEviction
	if !r.checkRequiredVariables() {
		return evictions, err
	}
	if !isExistPath(r.evictionFilePath) {
		if f, err := os.Create(r.evictionFilePath); err != nil {
			defer f.Close()
			os.Stderr.WriteString(fmt.Sprintf("Failed create eviction file, path: %v, err: %v\n", r.evictionFilePath, err))
		} else {
			os.Stdout.WriteString(fmt.Sprintf("Created eviction file, path: %v\n", r.evictionFilePath))
			return evictions, err
		}
	}
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	f, err := os.Open(r.evictionFilePath)
	if err != nil {
		return evictions, fmt.Errorf("Failed open eviction file, path: %v, err: %v\n", r.evictionFilePath, err)
	}
	defer f.Close()
	if fi, err := f.Stat(); err != nil {
		return evictions, fmt.Errorf("Failed stat eviction file, path: %v, err: %v\n", r.evictionFilePath, err)
	} else {
		if fi.Size() == 0 {
			return evictions, nil
		}
	}
	var file RateLimiterEviction
	dec := gob.NewDecoder(f)
	err = dec.Decode(&file)
	if err != nil {
		return evictions, fmt.Errorf("Failed decode eviction file, path: %v, err: %v\n", r.evictionFilePath, err)
	}
	return file, nil
}

func (r *RateLimiter) writeEvictionEntry() error {
	if !r.checkRequiredVariables() {
		return nil
	}
	r.mutex.Lock()
	defer r.mutex.Unlock()
	f, err := os.Create(r.evictionFilePath)
	if err != nil {
		return fmt.Errorf("Failed create eviction file, path: %v, err: %v\n", r.evictionFilePath, err)
	}
	defer f.Close()
	header := RateLimiterEvictionHeader{
		LimitCount:      r.limitCount,
		Interval:        r.interval,
		LastCleanedTime: r.lastCleanedTime,
		StopTime:        time.Now(),
	}
	file := RateLimiterEviction{
		Header: header,
		Entry:  r.entry,
	}
	enc := gob.NewEncoder(f)
	if err := enc.Encode(file); err != nil {
		return fmt.Errorf(fmt.Sprintf("Failed write eviction file, path: %v, err: %v\n", r.evictionFilePath, err))
	}
	return nil
}

func (r *RateLimiter) incrementCount(key string) int {
	v, ok := r.getCount(key)
	nv := v + 1
	if !ok {
		r.setCount(key, 1)
	} else {
		r.setCount(key, nv)
	}
	return nv
}

func (r *RateLimiter) isRateLimited(key string) bool {
	v, ok := r.getCount(key)
	if !ok {
		r.setCount(key, 1)
	} else {
		if v < r.limitCount {
			return false
		} else {
			return true
		}
	}
	return false
}

func (r *RateLimiter) runLimitCleaner() {
	if r.evictionFilePath != "" {
		ev, err := r.readEvictionEntry()
		if err != nil {
			r.runLimitCleanWorker()
			os.Stderr.WriteString(fmt.Sprintf("Failed read eviction file, path: %v, err: %v\n", r.evictionFilePath, err))
			return
		}
		if r.applyEvictionEntry(ev) {
			elapsedTime := ev.Header.StopTime.Sub(ev.Header.LastCleanedTime)
			time.AfterFunc(time.Duration(ev.Header.Interval)*time.Second-elapsedTime, r.runLimitCleanWorker)
		}
	} else {
		r.runLimitCleanWorker()
	}
}

func (r *RateLimiter) runLimitCleanWorker() {
	go func() {
		ticker := time.NewTicker(time.Duration(r.interval) * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				r.flushallEntry()
				r.lastCleanedTime = time.Now()
			case <-r.done:
				r.flushallEntry()
				return
			case <-r.ctx.Done():
				r.stoplimitCleanWorker()
			}
		}
	}()
}

func (r *RateLimiter) stoplimitCleanWorker() {
	r.once.Do(func() {
		if err := r.writeEvictionEntry(); err != nil {
			os.Stderr.WriteString(fmt.Sprintf("Failed write evication entry, err: %v\n", err))
		}
		close(r.done)
	})
}

func newOptions() *CommandOption {
	return &CommandOption{}
}

func (c *CommandOption) setFlags() {
	flag.CommandLine.Init("fs", flag.ExitOnError)
	flagSet = flag.NewFlagSet("fs", flag.ExitOnError)
	flagSet.StringVar(&c.serverHost, "h", "127.0.0.1", "file server hostname")
	flagSet.StringVar(&c.serverPort, "p", "8080", "file server port")
	flagSet.BoolVar(&c.cd, "c", false, "approve change base directory")
	flagSet.BoolVar(&c.open, "o", false, "browser open on file server address")
	flagSet.BoolVar(&c.simple, "s", false, "render simple, only file names")
	flagSet.BoolVar(&c.debug, "d", false, "log level for debug")
}

func (c *CommandOption) overrideWithEnvironments() error {
	var err error
	fsServerPort := os.Getenv("FS_SERVER_PORT")
	if fsServerPort != "" {
		if _, err := strconv.Atoi(fsServerPort); err != nil {
			c.serverPort = fsServerPort
		}
		return fmt.Errorf("Failed to parse FS_SERVER_PORT to number, err: %v\n", err)
	}
	fsServerHost := os.Getenv("FS_SERVER_HOST")
	if fsServerHost != "" {
		c.serverHost = fsServerHost
	}
	fsSimpleMode := os.Getenv("FS_SIMPLE_MODE")
	if fsSimpleMode != "" {
		is, err := strconv.ParseBool(fsSimpleMode)
		if err != nil {
			return fmt.Errorf("Failed to parse FS_SIMPLE_MODE to bool, err: %v\n", err)
		}
		c.simple = is
	}
	fsDebugMode := os.Getenv("FS_DEBUG_MODE")
	if fsDebugMode != "" {
		is, err := strconv.ParseBool(fsDebugMode)
		if err != nil {
			return fmt.Errorf("Failed to parse FS_DEBUG_MODE to bool, err: %v\n", err)
		}
		c.debug = is
	}
	fsOpenMode := os.Getenv("FS_OPEN_MODE")
	if fsOpenMode != "" {
		is, err := strconv.ParseBool(fsOpenMode)
		if err != nil {
			return fmt.Errorf("Failed to parse FS_OPEN_MODE to bool, err: %v\n", err)
		}
		c.open = is
	}
	fsCd := os.Getenv("FS_CHANGE_DIRECTORY")
	if fsCd != "" {
		is, err := strconv.ParseBool(fsCd)
		if err != nil {
			return fmt.Errorf("Failed to parse FS_CHANGE_DIRECTORY to bool, err: %v\n", err)
		}
		c.open = is
	}
	return err
}

func newConfig(options CommandOption) (*Config, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		home = os.Getenv("HOME")
		if home == "" {
			return &Config{}, fmt.Errorf("User HOME directory is not found, Please set $HOME environment variable\n")
		}
	}
	fsTimeFormat := os.Getenv("FS_TIME_FORMAT")
	if fsTimeFormat == "" {
		fsTimeFormat = "06/01/02 15:04:05"
	}
	fsCorsOrigins := os.Getenv("FS_CORS_ORIGINS")
	if fsCorsOrigins == "" {
		fsCorsOrigins = "*"
	}
	fsFrecencyFilePath := os.Getenv("FS_FRECENCY_FILE_PATH")
	if fsFrecencyFilePath == "" {
		fsFrecencyFilePath = filepath.Join(home, ".fs")
	}
	var fsRateLimitInterval int
	fsRateLimitIntervalStr := os.Getenv("FS_RATE_LIMIT_INTERVAL")
	if v, err := strconv.Atoi(fsRateLimitIntervalStr); err != nil || fsRateLimitIntervalStr == "" {
		fsRateLimitInterval = 600
	} else {
		fsRateLimitInterval = v
	}
	var fsRateLimitCount int
	fsRateLimitCountStr := os.Getenv("FS_RATE_LIMIT_COUNT")
	if v, err := strconv.Atoi(fsRateLimitCountStr); err != nil || fsRateLimitCountStr == "" {
		fsRateLimitCount = 300
	} else {
		fsRateLimitCount = v
	}
	var fsAccessLogEvictionInterval int
	fsAccessLogEvictionIntervalStr := os.Getenv("FS_ACCESS_LOG_EVICTION_INTERVAL")
	if v, err := strconv.Atoi(fsAccessLogEvictionIntervalStr); err != nil || fsAccessLogEvictionIntervalStr == "" {
		fsAccessLogEvictionInterval = 300
	} else {
		fsAccessLogEvictionInterval = v
	}
	config := &Config{
		HOME:                             home,
		FS_SERVER_PORT:                   options.serverPort,
		FS_SERVER_HOST:                   options.serverHost,
		FS_DEBUG_MODE:                    options.debug,
		FS_SIMPLE_MODE:                   options.simple,
		FS_CHANGE_DIRECTORY:              options.cd,
		FS_OPEN_MODE:                     options.open,
		FS_OPEN_CMD:                      os.Getenv("FS_OPEN_CMD"),
		FS_ADMIN_IP:                      os.Getenv("FS_ADMIN_IP"),
		FS_AUTH_PATH:                     os.Getenv("FS_AUTH_PATH"),
		FS_AUTH_USER:                     os.Getenv("FS_AUTH_USER"),
		FS_AUTH_PASSWORD:                 os.Getenv("FS_AUTH_PASSWORD"),
		FS_ACCESS_LOG_DIRECTORY:          os.Getenv("FS_ACCESS_LOG_DIRECTORY"),
		FS_RATE_LIMIT_EVICTION_FILE_PATH: os.Getenv("FS_RATE_LIMIT_EVICTION_FILE_PATH"),
		FS_ACCESS_LOG_EVICTION_INTERVAL:  fsAccessLogEvictionInterval,
		FS_CORS_ORIGINS:                  fsCorsOrigins,
		FS_FRECENCY_FILE_PATH:            fsFrecencyFilePath,
		FS_TIME_FORMAT:                   fsTimeFormat,
		FS_RATE_LIMIT_INTERVAL:           fsRateLimitInterval,
		FS_RATE_LIMIT_COUNT:              fsRateLimitCount,
	}
	return config, err
}

func showVersion() (int, error) {
	return fmt.Printf("fs version %s", version)
}

func isExistDirectory(path string) bool {
	dir, err := os.Stat(path)
	return !os.IsNotExist(err) && dir.IsDir()
}

func isExistPath(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}

func fileSize(s int64) string {
	if s < 1024 {
		return fmt.Sprintf("%d", s)
	}
	var exp int
	n := float64(s)
	for exp = 0; exp < 4; exp++ {
		n /= 1024
		if n < 1024 {
			break
		}
	}
	return fmt.Sprintf("%.1f%c", float64(n), "KMGT"[exp])
}

func getFileLineCount(file *os.File) (int, error) {
	scanner := bufio.NewScanner(file)
	lineCount := 0
	for scanner.Scan() {
		lineCount++
	}
	if err := scanner.Err(); err != nil {
		return 0, err
	}
	return lineCount, nil
}

func splitPath(path string) []string {
	if path[0] == '/' {
		path = path[1:]
	}
	var n int
	for i := 0; i < len(path); i++ {
		n++
		p := strings.IndexByte(path[i:], '/')
		if p == -1 {
			break
		}
		if p == len(path)-1 {
			n++
		}
		i = p + i
	}
	s := make([]string, 0, n)
	for {
		p := strings.IndexByte(path, '/')
		if p == -1 {
			s = append(s, path)
			break
		}
		s = append(s, path[:p])
		path = path[p+1:]
	}
	return s
}

func getReqIpAddress(r *http.Request) (net.IP, error) {
	var err error
	remoteAddr := strings.TrimSpace(r.Header.Get("X-Real-IP"))
	if remoteAddr == "" {
		remoteAddr, _, err = net.SplitHostPort(strings.TrimSpace(r.RemoteAddr))
	}
	return net.ParseIP(remoteAddr), err
}

func getLocalIps() ([]net.IP, error) {
	var ips []net.IP
	var err error
	if len(localIps) > 0 {
		return localIps, err
	}
	ifaces, err := net.Interfaces()
	if err != nil {
		return ips, err
	}
	for _, iface := range ifaces {
		addrs, err := iface.Addrs()
		if err != nil {
			return ips, err
		}
		for _, addr := range addrs {
			switch v := addr.(type) {
			case *net.IPNet:
				ips = append(ips, v.IP)
			case *net.IPAddr:
				ips = append(ips, v.IP)
			}
		}
	}
	return ips, err
}

func main() {
	// setOptions
	options := newOptions()
	options.setFlags()
	if err := options.overrideWithEnvironments(); err != nil {
		log.Fatalf("Failed override command options with environments, err: %v", err)
	}

	// parse flags
	flagSet.Parse(os.Args[1:])
	args := flagSet.Args()

	// set config
	config, err := newConfig(*options)
	if err != nil {
		log.Fatalf("Failed set config, err: %v", err)
	}

	// set log level
	if options.debug {
		log.SetFlags(log.LstdFlags | log.Lshortfile)
	}

	// set directory
	var dir string
	if len(args) > 0 {
		if isExistDirectory(args[0]) {
			dir = args[0]
		} else {
			log.Fatalf("No such directory")
		}
	} else {
		dir = "."
	}

	// server
	server, err := newServer(dir, config)
	if err != nil {
		log.Fatalf("Failed initialize server, err: %v", err)
	}

	if err = server.serve(); err != nil || err != http.ErrServerClosed {
		log.Printf("Server closed gracefully")
	} else {
		log.Fatalf("Failed at serve, err: %v", err)
	}
}
