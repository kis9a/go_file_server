package main

import (
	"bytes"
	"flag"
	"fmt"
	"html/template"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

type Options struct {
	ServerPort   string
	ServerHost   string
	LocationName string
	Version      bool
	Debug        bool
	Open         bool
}

type Configs struct {
	HOME     string
	OPEN_CMD string
}

type Server struct {
	Addr       string
	Directory  string
	Path       string
	Query      url.Values
	TimeHelper *TimeHelper
}

type File struct {
	Name          string
	IsDir         bool
	ModTime       time.Time
	ModTimeString string
	Size          int64
	SizeString    string
}

type TimeHelper struct {
	Location *time.Location
	Now      time.Time
}

var (
	flagSet *flag.FlagSet
	options Options
	configs Configs

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
		return server, fmt.Errorf("failed initialize time helper: %v", err)
	} else {
		server.TimeHelper = timeh
	}

	return server, err
}

func (s *Server) serve() error {
	http.HandleFunc("/", s.routeHandler)

	log.Printf("Serving %s at %s", s.Directory, s.Addr)

	if options.Open {
		err := browserOpen("http://" + s.Addr)
		if err != nil {
			os.Stderr.WriteString("failed browser open: " + err.Error())
		}
	}
	return http.ListenAndServe(s.Addr, nil)
}

func (s *Server) routeHandler(w http.ResponseWriter, r *http.Request) {
	log.Printf("%s: %s %s", r.RemoteAddr, r.Method, r.Host+r.RequestURI)

	if r.Method == http.MethodGet {
		s.handler(w, r)
	}
}

func (s *Server) handler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")

	urlPath, err := url.PathUnescape(r.URL.Path)
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("failed to path unescape request path %v", err))
		http.Error(w, fmt.Sprintf("failed to path unescape request path %v", err), http.StatusInternalServerError)
		return
	}
	s.Path = path.Join(s.Directory, urlPath)
	s.Query = r.URL.Query()

	if !isExistPath(s.Path) {
		os.Stderr.WriteString(fmt.Sprintf("No such file or directory %s", s.Path))
		http.Error(w, "file not found", http.StatusNotFound)
		return
	}

	s.render(w, r)
}

func (s *Server) renderBase(w http.ResponseWriter) error {
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
      ul {
        display: table;
        list-style-type: none;
        padding: 0;
        margin: 0.5rem 0.3rem;
        width: 100%;
      }
      li {
        display: table-row;
        margin-right: 0.5rem 0;
      }
      li > * {
        display: table-cell;
        margin-right: 0.5rem;
      }
      li > *:last-child {
        margin-right: 0;
      }
      .nav {
        margin-bottom: 1rem;
      }
    </style>
  </head>
</html>
`

	type Template struct {
		Path string
	}

	tmpl, err := template.New("").Parse(baseTmpl)
	if err != nil {
		return err
	}

	var b bytes.Buffer
	tmpl.Execute(&b, Template{
		Path: s.Path,
	})

	_, err = io.WriteString(w, b.String())
	return err
}

func (s *Server) renderDirectory(f *os.File, w http.ResponseWriter) error {
	html, err := os.Open(path.Join(s.Path, "index.html"))

	if err == nil {
		io.Copy(w, html)
		os.Stderr.WriteString(fmt.Sprintf("failed write index.html %v", err))
		html.Close()
		return err
	}
	html.Close()

	files, err := s.readDirectoryFiles(f)
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("failed read dir files %v", err))
		http.Error(w, fmt.Sprintf("failed read dir files %v", err), http.StatusInternalServerError)
		return err
	}

	absDir, err := filepath.Abs(s.Directory)
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("Could not get directory absolute dir %v", err))
		http.Error(w, fmt.Sprintf("Could not get directory absolute dir %v", err), http.StatusInternalServerError)
		return err
	}

	absPath, err := filepath.Abs(s.Path)
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("Could not get directory absolute path %v", err))
		http.Error(w, fmt.Sprintf("Could not get directory absolute path %v", err), http.StatusInternalServerError)
		return err
	}

	relPath, err := filepath.Rel(absDir, absPath)
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("Could not get directory rel path %v", err))
		http.Error(w, fmt.Sprintf("Could not get directory rel path %v", err), http.StatusInternalServerError)
		return err
	}

	absDirName := absDir
	if strings.HasPrefix(absDir, configs.HOME) {
		absDirName = strings.ReplaceAll(absDir, configs.HOME, "~")
	}

	// write buffer
	var wb bytes.Buffer
	wb.WriteString(fmt.Sprintf("<div class=\"nav\"><span><a href=\"/\">%s</a></span>", absDirName))

	relPathPwd := "."
	splitedPaths := splitPath(relPath)
	var ps []string
	for k, p := range splitedPaths {
		ps = append(ps, p)
		if p != relPathPwd {
			if s.Directory == "/" && k == 0 {
				wb.WriteString(fmt.Sprintf("<span> </span><span><a href=\"/%s/\">%s</a></span>", strings.Join(ps, "/"), p))
			} else {
				wb.WriteString(fmt.Sprintf("<span>/</span><span><a href=\"/%s/\">%s</a></span>", strings.Join(ps, "/"), p))
			}
		}
	}
	wb.WriteString("</div><ul>")
	for _, fi := range files {
		if fi.IsDir {
			wb.WriteString(fmt.Sprintf("<li><span><a href=\"%s/\">%s/</a></span><span class=\"size\">%s</span><span class=\"modTime\">%s</span></li>", url.PathEscape(fi.Name), fi.Name, fi.SizeString, fi.ModTimeString))
		} else {
			wb.WriteString(fmt.Sprintf("<li><span><a href=\"%s\">%s</a></span><span class=\"size\">%s</span><span class=\"modTime\">%s</span></li>", url.PathEscape(fi.Name), fi.Name, fi.SizeString, fi.ModTimeString))
		}
	}
	wb.WriteString("</ul>")

	// write io
	io.WriteString(w, wb.String())
	return err
}

func (s *Server) renderFile(f *os.File, w http.ResponseWriter, r *http.Request) error {
	http.ServeContent(w, r, s.Path, time.Time{}, f)
	return nil
}

func (s *Server) render(w http.ResponseWriter, r *http.Request) {
	// check path exsits
	f, err := os.Open(s.Path)
	defer f.Close()
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf("failed to open path %v", err))
		http.Error(w, fmt.Sprintf("failed to open path %v", err), http.StatusInternalServerError)
		return
	}

	// render
	if isExistDirectory(s.Path) {
		// render base
		if err := s.renderBase(w); err != nil {
			os.Stderr.WriteString(fmt.Sprintf("failed render base %v", err))
			http.Error(w, fmt.Sprintf("failed render base %v", err), http.StatusInternalServerError)
			return
		}

		// render directory
		if err = s.renderDirectory(f, w); err != nil {
			os.Stderr.WriteString(fmt.Sprintf("failed render directory %v", err))
			http.Error(w, fmt.Sprintf("failed render directory %v", err), http.StatusInternalServerError)
			return
		}
	} else {
		if err = s.renderFile(f, w, r); err != nil {
			os.Stderr.WriteString(fmt.Sprintf("failed render file %v", err))
			http.Error(w, fmt.Sprintf("failed render file %v", err), http.StatusInternalServerError)
			return
		}
	}
}

func (s *Server) parentDirectory() File {
	if isExistDirectory(path.Join(s.Directory, "..")) && s.Path != "." && s.Path != s.Directory {
		return File{
			Name:  "..",
			IsDir: true,
		}
	} else {
		return File{}
	}
}

func (s *Server) readDirectoryFiles(f *os.File) ([]File, error) {
	fs, err := f.Readdir(-1)
	files := []File{s.parentDirectory()}
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
	return t.Format("06/01/02 15:04:05")
}

// functions
func setOptions() {
	flag.CommandLine.Init("fs", flag.ExitOnError)
	flagSet = flag.NewFlagSet("fs", flag.ExitOnError)
	flagSet.StringVar(&options.ServerHost, "h", "127.0.0.1", "file server hostname")
	flagSet.StringVar(&options.ServerPort, "p", "8080", "file server port")
	flagSet.StringVar(&options.LocationName, "l", "Asia/Tokyo", "time loation")
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
	configs = Configs{
		HOME:     home,
		OPEN_CMD: os.Getenv("OPEN_CMD"),
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
		log.Fatalf("Failed serve %v", err)
	}
}
