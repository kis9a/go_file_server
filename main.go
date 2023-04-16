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

type Options struct {
	ServerPort   string
	ServerHost   string
	LocationName string
	Cd           bool
	Simple       bool
	Version      bool
	Debug        bool
	Open         bool
}

type Configs struct {
	HOME             string
	OPEN_CMD         string
	FS_AUTH_PATH     string
	FS_AUTH_USER     string
	FS_AUTH_PASSWORD string
	FS_ADMIN_IP      string
	FS_TIME_FORMAT   string
	FS_FILEDB_PATH   string
}

type Server struct {
	Addr       string
	Directory  string
	Path       string
	UrlPath    string
	Query      url.Values
	Files      []File
	Frecency   *Frecency
	TimeHelper *TimeHelper
}

type File struct {
	Name          string    `json:"name"`
	IsDir         bool      `json:"isDir"`
	ModTime       time.Time `json:"modTime"`
	ModTimeString string    `json:"modTimeString"`
	Size          int64     `json:"size"`
	SizeString    string    `json:"SizeString"`
}

type TimeHelper struct {
	Location *time.Location
	Now      time.Time
}

type Frecency struct {
	Frecs             []Frec
	FrecencyFileItems []FrecencyFileItem
	Comma             rune
}

type Frec struct {
	Score int
	Path  string
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
	mutex    sync.RWMutex
	options  Options
	configs  Configs
	localIPs []net.IP

	// set at go build -ldflags '-X main.Version=xxx'
	Version = "0.0.0"
)

// server
func newServer(dir string) (*Server, error) {
	// set server dir
	server := &Server{
		Directory: dir,
	}

	// check tcp addr
	addr := net.JoinHostPort(options.ServerHost, options.ServerPort)
	_, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return server, fmt.Errorf("Listen addr could not resolve: %s", addr)
	} else {
		server.Addr = addr
	}

	// iniitalize timeh
	timeh, err := newTimeHelper(options.LocationName)
	if err != nil {
		return server, fmt.Errorf("Failed initialize time helper: %v", err)
	} else {
		server.TimeHelper = timeh
	}

	// initialize frecency
	if !isExistPath(configs.FS_FILEDB_PATH) {
		_, err := os.Create(configs.FS_FILEDB_PATH)
		if err != nil {
			return server, fmt.Errorf("Failed initialize FrecencyFileDB path: %s, err: %v", configs.FS_FILEDB_PATH, err)
		}
	}
	server.Frecency, err = newFrecency(configs.FS_FILEDB_PATH)

	if err := server.Frecency.add(server.Directory); err != nil {
		return server, fmt.Errorf("Failed frecency add path: %s, err: %v", server.Directory, err)
	}

	if err := server.Frecency.clean(); err != nil {
		return server, fmt.Errorf("Failed frecency clean err: %v", err)
	}

	return server, err
}

func (s *Server) serve() error {
	var err error
	http.HandleFunc("/", s.routeHandler)
	http.HandleFunc("/favicon.ico", func(w http.ResponseWriter, r *http.Request) {})

	log.Printf("Serving %s at %s", s.Directory, s.Addr)

	if options.Open {
		err := browserOpen("http://" + s.Addr)
		if err != nil {
			os.Stderr.WriteString("Failed browser open: " + err.Error())
		}
	}

	httpServer := &http.Server{
		Addr:              s.Addr,
		ReadHeaderTimeout: 10 * time.Second,
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, os.Interrupt)

	go func() {
		if err = httpServer.ListenAndServe(); err != nil {
			return
		}
	}()

	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err = httpServer.Shutdown(ctx); err != nil {
		return fmt.Errorf("Failed gracefully shutdown err: %v", err)
	}
	return err
}

func (s *Server) routeHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	log.Printf("%s: %s %s", r.RemoteAddr, r.Method, r.Host+r.RequestURI)

	s.UrlPath, err = url.PathUnescape(r.URL.Path)
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("Failed to path unescape request path %v\n", err))
		http.Error(w, fmt.Sprintf("Failed to path unescape request path %v", err), http.StatusInternalServerError)
		return
	}
	pathBefore := s.Path
	s.Path = path.Join(s.Directory, s.UrlPath)
	s.Query = r.URL.Query()

	if configs.FS_AUTH_PATH != "" {
		authPath := configs.FS_AUTH_PATH
		authPath = strings.TrimSuffix(authPath, "/")

		if strings.HasPrefix(s.Path, authPath) {
			if !s.checkBasicAuthorization(r) {
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

	if strings.HasPrefix(s.UrlPath, "/__fs") {
		s.fsHandler(w, r)
		return
	}

	if r.Method == http.MethodGet {
		if pathBefore != s.Path && isExistDirectory(s.Path) {
			if err := s.Frecency.add(s.Path); err != nil {
				os.Stderr.WriteString(fmt.Sprintf("Failed frecency add path: %s, err: %v", s.Path, err))
			}
		}
		s.serveHandler(w, r)
		return
	}
}

func (s *Server) checkBasicAuthorization(r *http.Request) bool {
	user, pw, ok := r.BasicAuth()
	return ok && user == configs.FS_AUTH_USER && pw == configs.FS_AUTH_PASSWORD
}

func (s *Server) fsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		if s.UrlPath == "/__fs/version" {
			s.fsVersionHandler(w)
			return
		}

		if strings.HasPrefix(s.UrlPath, "/__fs/files") {
			s.fsFilesHandler(w)
			return
		}

		if strings.HasPrefix(s.UrlPath, "/__fs/frecs") {
			s.fsFrecsHandler(w)
			return
		}
	}

	if r.Method == http.MethodPost {
		if options.Cd && strings.HasPrefix(s.UrlPath, "/__fs/cd") {
			s.fsCdHandler(w, r)
			return
		}
	}
}

func (s *Server) checkIpAuthorization(r *http.Request) (bool, error) {
	authed := false
	remoteIp, err := getHttpRequestIpAddress(r)
	if err != nil {
		return authed, err
	}
	if configs.FS_ADMIN_IP == "" {
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
		adminIp := net.ParseIP(configs.FS_ADMIN_IP)
		authed = remoteIp.Equal(adminIp)
	}
	return authed, err
}

func (s *Server) fsCdHandler(w http.ResponseWriter, r *http.Request) {
	authed, err := s.checkIpAuthorization(r)
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("Failed check ip authorization body %v\n", err))
		http.Error(w, fmt.Sprintf("Failed check ip authorization %v\n", err), http.StatusInternalServerError)
		return
	}
	if !authed {
		os.Stderr.WriteString("Not an allowed IP")
		http.Error(w, "Not an allowed IP", http.StatusUnauthorized)
		return
	}

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
		s.Directory = body.Path
	} else {
		dir, err := filepath.Abs(filepath.Join(s.Directory, body.Path))
		if err != nil {
			os.Stderr.WriteString(fmt.Sprintf("Failed make absolute path %v\n", err))
			http.Error(w, fmt.Sprintf("Failed make absolute path %v", err), http.StatusInternalServerError)
			return
		} else {
			s.Directory = dir
		}
	}

	if !isExistDirectory(s.Directory) {
		http.Error(w, fmt.Sprintf("No such directory"), http.StatusBadRequest)
		log.Fatalf("No such directory")
		return
	}

	if err := s.Frecency.add(s.Directory); err != nil {
		os.Stderr.WriteString(fmt.Sprintf("Failed frecency add %v", err))
		http.Error(w, fmt.Sprintf("Failed frecency add %v", err), http.StatusInternalServerError)
		return
	}
}

func (s *Server) fsVersionHandler(w http.ResponseWriter) {
	type VersionRes struct {
		Version string `json:"version"`
	}
	j, err := json.Marshal(VersionRes{
		Version: Version,
	})
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("Failed json marshal %v\n", err))
		http.Error(w, fmt.Sprintf("Failed json marshal %v", err), http.StatusInternalServerError)
	}
	w.Write(j)
}

func (s *Server) fsFrecsHandler(w http.ResponseWriter) {
	if err := s.Frecency.read(); err != nil {
		os.Stderr.WriteString(fmt.Sprintf("Failed read fracs err: %v\n", err))
		http.Error(w, fmt.Sprintf("Failed read fracs err: %v\n", err), http.StatusInternalServerError)
	}

	j, err := json.Marshal(s.Frecency.getFrecs(30))
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("Failed json marshal %v\n", err))
		http.Error(w, fmt.Sprintf("Failed json marshal %v", err), http.StatusInternalServerError)
	}
	w.Write(j)
}

func (s *Server) fsFilesHandler(w http.ResponseWriter) {
	p := strings.TrimPrefix(s.UrlPath, "/__fs/files")
	p = filepath.Join(s.Directory, p)

	// check path exsits
	f, err := os.Open(p)
	defer f.Close()
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("Failed to open path %v\n", err))
		http.Error(w, fmt.Sprintf("Failed to open path %v", err), http.StatusInternalServerError)
		return
	}

	fs, err := f.Stat()
	if fs.IsDir() {
		s.Files, err = s.readDirectoryFiles(f)
		if err != nil {
			os.Stderr.WriteString(fmt.Sprintf("Failed read directory files %v\n", err))
			http.Error(w, fmt.Sprintf("Failed read directory files %v", err), http.StatusInternalServerError)
			return
		}

		j, err := json.Marshal(s.Files)
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
			ModTimeString: s.TimeHelper.ParseTime(fs.ModTime()),
			SizeString:    FileSize(fs.Size()),
		})
		if err != nil {
			os.Stderr.WriteString(fmt.Sprintf("Failed json marshal %v\n", err))
			http.Error(w, fmt.Sprintf("Failed json marshal %v", err), http.StatusInternalServerError)
		}
		w.Write(j)
	}
}

func (s *Server) serveHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")

	if !isExistPath(s.Path) {
		os.Stderr.WriteString(fmt.Sprintf("No such file or directory %s\n", s.Path))
		http.Error(w, "file not found", http.StatusNotFound)
		return
	}

	f, err := os.Open(s.Path)
	defer f.Close()
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("Failed to open path %v\n", err))
		http.Error(w, fmt.Sprintf("Failed to open path %v", err), http.StatusInternalServerError)
		return
	}

	if isExistDirectory(s.Path) {
		s.Files, err = s.readDirectoryFiles(f)
		if err != nil {
			os.Stderr.WriteString(fmt.Sprintf("Failed read directory files %v\n", err))
			http.Error(w, fmt.Sprintf("Failed read directory files %v", err), http.StatusInternalServerError)
			return
		}

		sortKey := s.getSortKey(s.Query.Get("sortKey"))
		sortMethod := s.getSortMethod(s.Query.Get("sortMethod"))

		if sortKey != SORT_KEY_UNKNOWN && sortMethod != SORT_METHOD_UNKNOWN {
			switch sortKey {
			case SORT_KEY_NAME:
				s.filesSortByName(sortMethod)
			case SORT_KEY_SIZE:
				s.filesSortBySize(sortMethod)
			case SORT_KEY_TIME:
				s.filesSortByTime(sortMethod)
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
		Path: s.Path,
		Body: template.HTML(body),
	})

	return b.String(), err
}

func (s *Server) renderDirectory(f *os.File, w http.ResponseWriter) error {
	html, err := os.Open(path.Join(s.Path, "index.html"))

	if err == nil {
		io.Copy(w, html)
		os.Stderr.WriteString(fmt.Sprintf("Failed write index.html %v\n", err))
		html.Close()
		return err
	}
	html.Close()

	absDir, err := filepath.Abs(s.Directory)
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("Failed make absolute path %v\n", err))
		http.Error(w, fmt.Sprintf("Failed make absolute path %v", err), http.StatusInternalServerError)
		return err
	}

	absPath, err := filepath.Abs(s.Path)
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
	if strings.HasPrefix(absDir, configs.HOME) {
		absDirName = strings.ReplaceAll(absDir, configs.HOME, "~")
	}

	queryString := s.Query.Encode()
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
			if s.Directory == "/" && k == 0 {
				wb.WriteString(fmt.Sprintf("<span> </span><span><a href=\"/%s/%s\">%s</a></span>\n", strings.Join(ps, "/"), queryString, p))
			} else {
				wb.WriteString(fmt.Sprintf("<span>/</span><span><a href=\"/%s/%s\">%s</a></span>\n", strings.Join(ps, "/"), queryString, p))
			}
		}
	}

	wb.WriteString("</div>\n<ul>\n")
	if options.Simple {
		wb.WriteString("<li class=\"colh\"><div><span onclick=\"s('name')\">FILES</span></div></li>\n")
	} else {
		wb.WriteString("<li class=\"colh\"><div><span onclick=\"s('name')\">FILES</span></div>\n<div><span onclick=\"s('size')\">SIZE</span></div>\n<div><span onclick=\"s('time')\">TIME</span></div></li>\n")
	}

	if s.Directory != "/" && s.UrlPath == "/" && options.Cd {
		wb.WriteString(fmt.Sprintf("<li><span class=\"up\" onclick=\"cd('..')\">%s/</span></li>\n", ".."))
	}

	for _, fi := range s.parentFiles() {
		fiPath := filepath.Join(url.PathEscape(fi.Name))
		wb.WriteString(fmt.Sprintf("<li><span><a href=\"%s/%s\">%s/</a></span></li>\n", fiPath, queryString, fi.Name))
	}

	for _, fi := range s.Files {
		fiPath := filepath.Join(url.PathEscape(fi.Name))

		if fi.IsDir {
			if options.Simple {
				wb.WriteString(fmt.Sprintf("<li><span><a href=\"%s/%s\">%s/</a></span></li>\n", fiPath, queryString, fi.Name))
			} else {
				wb.WriteString(fmt.Sprintf("<li><span><a href=\"%s/%s\">%s/</a></span><span class=\"size\">%s</span><span class=\"modTime\">%s</span></li>\n", fiPath, queryString, fi.Name, fi.SizeString, fi.ModTimeString))
			}
		} else {
			if options.Simple {
				wb.WriteString(fmt.Sprintf("<li><span><a href=\"%s/%s\">%s</a></span></li>\n", fiPath, queryString, fi.Name))
			} else {
				wb.WriteString(fmt.Sprintf("<li><span><a href=\"%s/%s\">%s</a></span><span class=\"size\">%s</span><span class=\"modTime\">%s</span></li>\n", fiPath, queryString, fi.Name, fi.SizeString, fi.ModTimeString))
			}
		}
	}
	wb.WriteString("</ul>\n")

	if options.Cd {
		wb.WriteString("<ul>\n")
		wb.WriteString("<li class=\"frecs colh\"><div><span onclick=\"frecs()\">FRECS</span></div></li>\n")
		for _, fi := range s.Frecency.getFrecs(30) {
			fiPath := strings.ReplaceAll(fi.Path, configs.HOME, "~")
			wb.WriteString(fmt.Sprintf("<li><span class=\"frec\" score=\"%d\" onclick=\"cd('%s')\">%s/</span></li>\n", fi.Score, fi.Path, fiPath))
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
	http.ServeContent(w, r, s.Path, time.Time{}, f)
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

func (s *Server) filesSortByName(method SortMethod) {
	switch method {
	case SORT_ASCENDING:
		sort.Slice(s.Files, func(i, j int) bool {
			return s.Files[i].Name < s.Files[j].Name
		})
		return
	case SORT_DESCENDING:
		sort.Slice(s.Files, func(i, j int) bool {
			return s.Files[i].Name > s.Files[j].Name
		})
		return
	}
}

func (s *Server) filesSortBySize(method SortMethod) {
	switch method {
	case SORT_ASCENDING:
		sort.Slice(s.Files, func(i, j int) bool {
			return s.Files[i].Size < s.Files[j].Size
		})
		return
	case SORT_DESCENDING:
		sort.Slice(s.Files, func(i, j int) bool {
			return s.Files[i].Size > s.Files[j].Size
		})
		return
	}
}

func (s *Server) filesSortByTime(method SortMethod) {
	switch method {
	case SORT_ASCENDING:
		sort.Slice(s.Files, func(i, j int) bool {
			return s.Files[i].ModTime.Before(s.Files[j].ModTime)
		})
		return
	case SORT_DESCENDING:
		sort.Slice(s.Files, func(i, j int) bool {
			return s.Files[i].ModTime.After(s.Files[j].ModTime)
		})
		return
	}
}

func (s *Server) parentFiles() []File {
	if isExistDirectory(path.Join(s.Directory, "..")) && s.UrlPath != "/" {
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
			ModTimeString: s.TimeHelper.ParseTime(f.ModTime()),
			SizeString:    FileSize(f.Size()),
		})
	}
	return files, err
}

// timeHelper
func newTimeHelper(locationName string) (*TimeHelper, error) {
	location, err := time.LoadLocation(locationName)
	return &TimeHelper{
		Location: location,
		Now:      time.Now().In(location),
	}, err
}

func (timeh TimeHelper) ParseTime(t time.Time) string {
	return t.Format(configs.FS_TIME_FORMAT)
}

// frecency
func newFrecency(path string) (*Frecency, error) {
	return &Frecency{
		Comma: '|',
	}, nil
}

func (f *Frecency) read() error {
	var items []FrecencyFileItem
	r, err := os.ReadFile(configs.FS_FILEDB_PATH)
	if err != nil {
		return fmt.Errorf("Counld not read file path: %s, err %v", configs.FS_FILEDB_PATH, err)
	}

	cr := csv.NewReader(bytes.NewReader(r))
	mutex.RLock()
	cr.Comma = f.Comma
	csvs, err := cr.ReadAll()
	mutex.RUnlock()
	if err != nil {
		return fmt.Errorf("Counld not read csv: %s, err %v", configs.FS_FILEDB_PATH, err)
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

	f.FrecencyFileItems = items
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
	for _, v := range f.FrecencyFileItems {
		csvs = append(csvs, []string{v.Path, fmt.Sprintf("%d", v.Rank), fmt.Sprintf("%d", v.TimeStamp)})
	}
	mutex.RLock()
	fs, err := os.Create(configs.FS_FILEDB_PATH)
	mutex.RUnlock()
	if err != nil {
		return fmt.Errorf("Counld not create file path: %s, err %v", configs.FS_FILEDB_PATH, err)
	}

	retries := 5
	for i := 0; i < retries; i++ {
		w := csv.NewWriter(fs)
		mutex.Lock()
		w.Comma = f.Comma
		err = w.WriteAll(csvs)
		mutex.Unlock()
		if err == nil {
			return nil
		}
		f.backoff(i)
	}
	return fmt.Errorf("Counld not write file path: %s, err %v", configs.FS_FILEDB_PATH, err)
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
	for _, v := range f.FrecencyFileItems {
		if v.Path == path {
			v.Rank = v.Rank + 1
			v.TimeStamp = int(time.Now().Unix())
			isExist = true
		}

		if v.Rank >= 1 && isExistDirectory(v.Path) {
			newItems = append(newItems, v)
		}
	}
	f.FrecencyFileItems = newItems

	if !isExist {
		f.FrecencyFileItems = append(newItems, FrecencyFileItem{
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

	for _, v := range f.FrecencyFileItems {
		now := int(time.Now().Unix())
		score := f.frecent(v.Rank, v.TimeStamp, now)

		if score > 9000 {
			newItems = append(newItems, v)
		}
	}
	f.FrecencyFileItems = newItems

	if err := f.save(); err != nil {
		return fmt.Errorf("Failed save frecency file items: %v", err)
	}
	return nil
}

func (f *Frecency) getFrecs(limit int) []Frec {
	var frecs []Frec

	for i, v := range f.FrecencyFileItems {
		now := int(time.Now().Unix())
		score := f.frecent(v.Rank, v.TimeStamp, now)
		frecs = append(frecs, Frec{
			Score: score,
			Path:  v.Path,
		})

		if i == limit-1 {
			break
		}
	}

	sort.Slice(frecs, func(i, j int) bool {
		return frecs[i].Score > frecs[j].Score
	})
	return frecs
}

// functions
func setOptions() {
	flag.CommandLine.Init("fs", flag.ExitOnError)
	flagSet = flag.NewFlagSet("fs", flag.ExitOnError)
	flagSet.StringVar(&options.ServerHost, "h", "127.0.0.1", "file server hostname")
	flagSet.StringVar(&options.ServerPort, "p", "8080", "file server port")
	flagSet.StringVar(&options.LocationName, "l", "Asia/Tokyo", "time loation")
	flagSet.BoolVar(&options.Cd, "c", false, "approve change base directory, not recommended")
	flagSet.BoolVar(&options.Simple, "s", false, "render simple, only file names")
	flagSet.BoolVar(&options.Version, "v", false, "show version")
	flagSet.BoolVar(&options.Debug, "d", false, "log level for debug")
	flagSet.BoolVar(&options.Open, "o", false, "browser open on file server address")
}

func setConfigs() error {
	home, err := os.UserHomeDir()
	if err != nil {
		home = os.Getenv("HOME")
		if home == "" {
			return fmt.Errorf("User HOME directory is not found, Please set $HOME environment variable")
		}
	}
	fsTimeFormat := os.Getenv("FS_TIME_FORMAT")
	if fsTimeFormat == "" {
		fsTimeFormat = "06/01/02 15:04:05"
	}
	fsFileDBPath := os.Getenv("FS_FILEDB_PATH")
	if fsFileDBPath == "" {
		fsFileDBPath = filepath.Join(home, ".fs")
	}

	configs = Configs{
		HOME:             home,
		OPEN_CMD:         os.Getenv("OPEN_CMD"),
		FS_ADMIN_IP:      os.Getenv("FS_ADMIN_IP"),
		FS_AUTH_PATH:     os.Getenv("FS_AUTH_PATH"),
		FS_AUTH_USER:     os.Getenv("FS_AUTH_USER"),
		FS_AUTH_PASSWORD: os.Getenv("FS_AUTH_PASSWORD"),
		FS_FILEDB_PATH:   fsFileDBPath,
		FS_TIME_FORMAT:   fsTimeFormat,
	}
	return nil
}

func showVersion() (int, error) {
	return fmt.Printf("fs version %s", Version)
}

func isExistDirectory(path string) bool {
	dir, err := os.Stat(path)
	return !os.IsNotExist(err) && dir.IsDir()
}

func isExistPath(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}

func FileSize(s int64) string {
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

func browserOpen(url string) error {
	var err error
	openCmd := configs.OPEN_CMD
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
	if len(localIPs) > 0 {
		return localIPs, err
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

	// set configs
	if err := setConfigs(); err != nil {
		log.Fatalf("Failed set configs %v", err)
	}

	// set log level
	if options.Debug {
		log.SetFlags(log.LstdFlags | log.Lshortfile)
	}

	// when version
	if options.Version {
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
	server, err := newServer(dir)
	if err != nil {
		log.Fatalf("Failed initialize server %v", err)
	}

	if err = server.serve(); err != nil {
		if err != http.ErrServerClosed {
			log.Fatalf("Failed at serve %v", err)
		} else {
			log.Printf("Server closed gracefully")
		}
	}
}
