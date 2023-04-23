package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"html/template"
	"io"
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
	HOME                   string
	OPEN_CMD               string
	FS_AUTH_PATH           string
	FS_AUTH_USER           string
	FS_AUTH_PASSWORD       string
	FS_ADMIN_IP            string
	FS_TIME_FORMAT         string
	FS_FRECENCY_FILE_PATH  string
	FS_RATE_LIMIT_COUNT    int
	FS_RATE_LIMIT_INTERVAL int
}

type Options struct {
	serverPort   string
	serverHost   string
	locationName string
	cd           bool
	simple       bool
	version      bool
	debug        bool
	open         bool
}

type Server struct {
	address     string
	directory   string
	path        string
	urlPath     string
	query       url.Values
	files       []File
	config      *Config
	frecency    *Frecency
	rateLimiter *RateLimiter
	timeHelper  *TimeHelper
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
	sync.RWMutex
	limitCount int
	interval   int
	ctx        context.Context
	once       sync.Once
	done       chan struct{}
	entry      map[string]int
}

type TimeHelper struct {
	location   *time.Location
	now        time.Time
	timeFormat string
}

type Frecency struct {
	sync.RWMutex
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
	options  Options
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
	addr := net.JoinHostPort(options.serverHost, options.serverPort)
	_, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return server, fmt.Errorf("Listen addr could not resolve: %s", addr)
	} else {
		server.address = addr
	}

	// iniitalize timeh
	timeh, err := newTimeHelper(options.locationName, config.FS_TIME_FORMAT)
	if err != nil {
		return server, fmt.Errorf("Failed initialize time helper: %v", err)
	} else {
		server.timeHelper = timeh
	}

	// initialize rateLimiter
	server.rateLimiter = newRateLimiter(config.FS_RATE_LIMIT_COUNT, config.FS_RATE_LIMIT_INTERVAL)

	// initialize frecency
	if !isExistPath(config.FS_FRECENCY_FILE_PATH) {
		_, err := os.Create(config.FS_FRECENCY_FILE_PATH)
		if err != nil {
			return server, fmt.Errorf("Failed initialize FrecencyFileDB path: %s, err: %v", config.FS_FRECENCY_FILE_PATH, err)
		}
	}
	server.frecency = newFrecency(config.FS_FRECENCY_FILE_PATH)
	if err := server.frecency.add(server.directory); err != nil {
		return server, fmt.Errorf("Failed frecency add path: %s, err: %v", server.directory, err)
	}
	if err := server.frecency.clean(); err != nil {
		return server, fmt.Errorf("Failed frecency clean err: %v", err)
	}
	return server, err
}

func (s *Server) serve() error {
	var err error
	mux := s.routeHandler()
	log.Printf("Serving %s at %s", s.directory, s.address)

	if options.open {
		err := s.browserOpen("http://" + s.address)
		if err != nil {
			os.Stderr.WriteString("Failed browser open: " + err.Error())
		}
	}

	// context rate limit worker
	ctxRateLimiter, cancelRateLimiter := context.WithCancel(context.Background())
	s.rateLimiter.ctx = ctxRateLimiter
	s.rateLimiter.runClearRateLimitWorker()
	defer cancelRateLimiter()

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
				os.Stderr.WriteString("Failed on Listen And Server, error: " + err.Error())
			}
		}
	}()

	<-stop

	serverCtx, serverCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer serverCancel()

	if err = httpServer.Shutdown(serverCtx); err != nil {
		return fmt.Errorf("Failed gracefully shutdown err: %v", err)
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
	return s.middlewareUrlParser(
		s.middlewareAccessLogger(
			s.middlewareRateLimiter(
				s.middlewareBasicAuthenticator(h),
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
		s.path = path.Join(s.directory, s.urlPath)
		s.query = r.URL.Query()
		h.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), "pathBefore", s.path)))
	})
}

func (s *Server) middlewareAccessLogger(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Printf("%s: %s %s", r.RemoteAddr, r.Method, r.Host+r.RequestURI)
		h.ServeHTTP(w, r)
	})
}

func (s *Server) middlewareRateLimiter(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip, err := getHttpRequestIpAddress(r)
		if err != nil {
			os.Stderr.WriteString(fmt.Sprintf("Failed get remote IP: %v\n", err))
		}
		if s.rateLimiter.isRateLimited(ip.String()) {
			os.Stderr.WriteString(fmt.Sprintf("Rate limited, ip: %s\n", ip))
			http.Error(w, fmt.Sprintf("Rate Lmited, ip: %s\n", ip), http.StatusTooManyRequests)
			return
		} else {
			s.rateLimiter.increment(ip.String())
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
			ip, err := getHttpRequestIpAddress(r)
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
						os.Stderr.WriteString(fmt.Sprintf("Failed frecency add path: %s, err: %v", s.path, err))
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
	defer f.Close()
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("Failed to open path, err: %v\n", err))
		http.Error(w, fmt.Sprintf("Failed to open path, err: %v", err), http.StatusInternalServerError)
		return
	}

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
			ModTimeString: s.timeHelper.parseTime(fs.ModTime()),
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
	defer f.Close()
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("Failed to open path %v\n", err))
		http.Error(w, fmt.Sprintf("Failed to open path %v", err), http.StatusInternalServerError)
		return
	}

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
	if options.simple {
		wb.WriteString("<li class=\"colh\"><div><span onclick=\"s('name')\">FILES</span></div></li>\n")
	} else {
		wb.WriteString("<li class=\"colh\"><div><span onclick=\"s('name')\">FILES</span></div>\n<div><span onclick=\"s('size')\">SIZE</span></div>\n<div><span onclick=\"s('time')\">TIME</span></div></li>\n")
	}

	if s.directory != "/" && s.urlPath == "/" && options.cd {
		wb.WriteString(fmt.Sprintf("<li><span class=\"up\" onclick=\"cd('..')\">%s/</span></li>\n", ".."))
	}

	for _, fi := range s.parentFiles() {
		fiPath := filepath.Join(url.PathEscape(fi.Name))
		wb.WriteString(fmt.Sprintf("<li><span><a href=\"%s/%s\">%s/</a></span></li>\n", fiPath, queryString, fi.Name))
	}

	for _, fi := range s.files {
		fiPath := filepath.Join(url.PathEscape(fi.Name))

		if fi.IsDir {
			if options.simple {
				wb.WriteString(fmt.Sprintf("<li><span><a href=\"%s/%s\">%s/</a></span></li>\n", fiPath, queryString, fi.Name))
			} else {
				wb.WriteString(fmt.Sprintf("<li><span><a href=\"%s/%s\">%s/</a></span><span class=\"size\">%s</span><span class=\"modTime\">%s</span></li>\n", fiPath, queryString, fi.Name, fi.SizeString, fi.ModTimeString))
			}
		} else {
			if options.simple {
				wb.WriteString(fmt.Sprintf("<li><span><a href=\"%s/%s\">%s</a></span></li>\n", fiPath, queryString, fi.Name))
			} else {
				wb.WriteString(fmt.Sprintf("<li><span><a href=\"%s/%s\">%s</a></span><span class=\"size\">%s</span><span class=\"modTime\">%s</span></li>\n", fiPath, queryString, fi.Name, fi.SizeString, fi.ModTimeString))
			}
		}
	}
	wb.WriteString("</ul>\n")

	if options.cd {
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
			ModTimeString: s.timeHelper.parseTime(f.ModTime()),
			SizeString:    fileSize(f.Size()),
		})
	}
	return files, err
}

func (s *Server) browserOpen(url string) error {
	var err error
	openCmd := s.config.OPEN_CMD
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
			return fmt.Errorf("Can't suggest platform\nPlease nset os environment $OPEN_CMD")
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
	remoteIp, err := getHttpRequestIpAddress(r)
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

// timeHelper
func newTimeHelper(locationName string, timeFormat string) (*TimeHelper, error) {
	location, err := time.LoadLocation(locationName)
	return &TimeHelper{
		location:   location,
		now:        time.Now().In(location),
		timeFormat: timeFormat,
	}, err
}

func (timeh TimeHelper) parseTime(t time.Time) string {
	return t.Format(timeh.timeFormat)
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
		return fmt.Errorf("Counld not read file path: %s, err %v", f.filePath, err)
	}

	cr := csv.NewReader(bytes.NewReader(r))
	f.RLock()
	cr.Comma = f.comma
	csvs, err := cr.ReadAll()
	f.RUnlock()
	if err != nil {
		return fmt.Errorf("Counld not read csv: %s, err %v", f.filePath, err)
	}

	for _, c := range csvs {
		if len(c) == 3 {
			rank, err := strconv.Atoi(c[1])
			if err != nil {
				os.Stdout.WriteString("Ignore invalid type column `rank`: %s" + c[1])
			}
			timeStamp, err := strconv.Atoi(c[2])
			if err != nil {
				os.Stdout.WriteString("Ignore invalid type column `timeStamp`: %s" + c[1])
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
	f.RLock()
	fs, err := os.Create(f.filePath)
	f.RUnlock()
	if err != nil {
		return fmt.Errorf("Counld not create file path: %s, err %v", f.filePath, err)
	}

	retries := 5
	for i := 0; i < retries; i++ {
		w := csv.NewWriter(fs)
		f.Lock()
		w.Comma = f.comma
		err = w.WriteAll(csvs)
		f.Unlock()
		if err == nil {
			return nil
		}
		f.backoff(i)
	}
	return fmt.Errorf("Counld not write file path: %s, err %v", f.filePath, err)
}

func (f *Frecency) add(path string) error {
	var err error
	var newItems []FrecencyFileItem

	if !filepath.IsAbs(path) {
		path, err = filepath.Abs(path)
		if err != nil {
			return fmt.Errorf("Failed make absolute path: %s, err: %v", path, err)
		}
	}

	if err := f.read(); err != nil {
		return fmt.Errorf("Failed read fracs path: %s, err: %v", path, err)
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
		return fmt.Errorf("Failed save frecency file items: %v", err)
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

// RateLimiter
func newRateLimiter(limitCount int, interval int) *RateLimiter {
	return &RateLimiter{
		entry:      make(map[string]int),
		limitCount: limitCount,
		interval:   interval,
		ctx:        context.Background(),
		done:       make(chan struct{}),
	}
}

func (r *RateLimiter) set(key string, value int) {
	r.Lock()
	defer r.Unlock()
	r.entry[key] = value
}

func (r *RateLimiter) get(key string) (int, bool) {
	r.RLock()
	defer r.RUnlock()
	value, ok := r.entry[key]
	return value, ok
}

func (r *RateLimiter) del(key string) {
	r.Lock()
	defer r.Unlock()
	delete(r.entry, key)
}

func (r *RateLimiter) flushall() {
	r.Lock()
	defer r.Unlock()
	r.entry = make(map[string]int)
}

func (r *RateLimiter) increment(key string) int {
	v, ok := r.get(key)
	nv := v + 1
	if !ok {
		r.set(key, 1)
	} else {
		r.set(key, nv)
	}
	return nv
}

func (r *RateLimiter) isRateLimited(key string) bool {
	v, ok := r.get(key)
	if !ok {
		r.set(key, 1)
	} else {
		if v < r.limitCount {
			return false
		} else {
			return true
		}
	}
	return false
}

func (r *RateLimiter) runClearRateLimitWorker() {
	go func() {
		ticker := time.NewTicker(time.Duration(r.interval) * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				r.flushall()
			case <-r.done:
				r.flushall()
				return
			case <-r.ctx.Done():
				r.stopClearRateLimitWorker()
			}
		}
	}()
}

func (r *RateLimiter) stopClearRateLimitWorker() {
	r.once.Do(func() {
		close(r.done)
	})
}

// functions
func setOptions() {
	flag.CommandLine.Init("fs", flag.ExitOnError)
	flagSet = flag.NewFlagSet("fs", flag.ExitOnError)
	flagSet.StringVar(&options.serverHost, "h", "127.0.0.1", "file server hostname")
	flagSet.StringVar(&options.serverPort, "p", "8080", "file server port")
	flagSet.StringVar(&options.locationName, "l", "Asia/Tokyo", "time loation")
	flagSet.BoolVar(&options.cd, "c", false, "approve change base directory, not recommended")
	flagSet.BoolVar(&options.simple, "s", false, "render simple, only file names")
	flagSet.BoolVar(&options.version, "v", false, "show version")
	flagSet.BoolVar(&options.debug, "d", false, "log level for debug")
	flagSet.BoolVar(&options.open, "o", false, "browser open on file server address")
}

func newConfig() (*Config, error) {
	var config *Config
	home, err := os.UserHomeDir()
	if err != nil {
		home = os.Getenv("HOME")
		if home == "" {
			return config, fmt.Errorf("User HOME directory is not found, Please set $HOME environment variable")
		}
	}
	fsTimeFormat := os.Getenv("FS_TIME_FORMAT")
	if fsTimeFormat == "" {
		fsTimeFormat = "06/01/02 15:04:05"
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
	config = &Config{
		HOME:                   home,
		OPEN_CMD:               os.Getenv("OPEN_CMD"),
		FS_ADMIN_IP:            os.Getenv("FS_ADMIN_IP"),
		FS_AUTH_PATH:           os.Getenv("FS_AUTH_PATH"),
		FS_AUTH_USER:           os.Getenv("FS_AUTH_USER"),
		FS_AUTH_PASSWORD:       os.Getenv("FS_AUTH_PASSWORD"),
		FS_FRECENCY_FILE_PATH:  fsFrecencyFilePath,
		FS_TIME_FORMAT:         fsTimeFormat,
		FS_RATE_LIMIT_INTERVAL: fsRateLimitInterval,
		FS_RATE_LIMIT_COUNT:    fsRateLimitCount,
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

func getHttpRequestIpAddress(r *http.Request) (net.IP, error) {
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

// endpoints
func init() {
	// set options
	setOptions()
}

func main() {
	// parse flags
	flagSet.Parse(os.Args[1:])
	args := flagSet.Args()

	// set config
	config, err := newConfig()
	if err != nil {
		log.Fatalf("Failed set config %v", err)
	}

	// set log level
	if options.debug {
		log.SetFlags(log.LstdFlags | log.Lshortfile)
	}

	// when version
	if options.version {
		showVersion()
		os.Exit(0)
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
		log.Fatalf("Failed initialize server %v", err)
	}

	if err = server.serve(); err != nil || err != http.ErrServerClosed {
		log.Printf("Server closed gracefully")
	} else {
		log.Fatalf("Failed at serve %v", err)
	}
}
