package main

import (
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

)

type Cron struct {
	Name string `xml:"name"`
	Time string `xml:"time"`
	Cmd  string `xml:"cmd"`
	Arg  string `xml:"arg"`
}
type Crons struct {
	Crons []Cron `xml:"cron"`
}

type Crontab struct {
	Crons Crons `xml:"crons"`
}

type Message struct {
	Taskid  string `json:"taskid"`
	Pid     string    `json:"pid"`
	Filelog string `json:"filelog"`
	Status  bool   `json:"status"`
}

type MyMux struct {
	token string
}

func getcron(filename string) (Crontab, error) {
	xmlFile, err := os.Open(filename)
	var conf Crontab
	if err != nil {
		fmt.Println("Error opening file:", err)
		return conf, err
	}
	defer xmlFile.Close()

	if err := xml.NewDecoder(xmlFile).Decode(&conf); err != nil {
		fmt.Println("Error Decode file:", err)
		return conf, err
	}

	return conf, nil

}

func (p *MyMux) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/help" {
		w.Header().Set("Content-Type", "text/json; charset=utf-8")
		result:=`
		上传接口:    http://x.x.x.x:4321/api/u?token=
		定时查询：   http://x.x.x.x:4321/s?name=crontab.xml&token=
		日志查询：   http://x.x.x.x:4321/listlog
		日志详情：   http://x.x.x.x:4321/s?name=log/日志名称&token=
		`
		fmt.Fprintf(w, result)
		return
		
	}

	if r.URL.Path == "/api/cmd" {
		index(w, r, p)
		return
	}

	if r.URL.Path == "/echo" {
		w.Header().Set("Content-Type", "text/json; charset=utf-8")
		fmt.Fprintf(w, "{\"status\":\"ok\"}")
		return
	}
	if r.URL.Path == "/api/u/52871b0b087ec704631523f0a1776c4a97d7836b" {
		upfile(w, r, p)
		return
	}
	if r.URL.Path == "/frruncmd" {
		frruncmd(w, r, p)
		return
	}
	if r.URL.Path == "/upfrruncmd" {
		upfrruncmd(w, r, p)
		return
	}


	if r.URL.Path == "/upfilehand" {
		upfilehand(w, r, p)
		return
	}

	if r.URL.Path == "/listlog" {
       result:=""
		
		fileInfoList, err := ioutil.ReadDir("log")
	if err != nil {
		log.Fatal(err)
	}
	
	for i := range fileInfoList {
		result+=fileInfoList[i].Name()+"\n" //打印当前文件或目录下的文件或目录名
	}
		wr := w.Header()
		wr.Set("Content-Type", "text/json; charset=utf-8")
		fmt.Fprintf(w, result)
		return

	}

	if len(r.URL.Path) > 2 && r.URL.Path[:2] == "/s" {
		cmdtoken := r.URL.Query().Get("token")

		if cmdtoken != p.token && p.token != "" {
			fmt.Fprintf(w, "你没有权限访问该服务")
			return
		}

		filePath := r.URL.Path[len("/s"):]
		if filePath[len(filePath)-4:] == "xlsx" {
			w.Header().Set("content-type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
		} else if filePath[len(filePath)-4:] == "json" {
			w.Header().Set("Content-Type", "text/json; charset=utf-8")
		} else if filePath[len(filePath)-3:] == "log" {
			w.Header().Set("Content-Type", "text/json; charset=utf-8")
		} else if filePath[len(filePath)-3:] == "xml" {
			w.Header().Set("content-type", "text/xml; charset=utf-8")
		} else if filePath[len(filePath)-4:] == "html" {
			w.Header().Set("content-type", "text/html; charset=utf-8")
		} else if filePath[len(filePath)-3:] == "gif" {
			w.Header().Set("content-type", "image/gif")
		} else if filePath[len(filePath)-3:] == "jpg" {
			w.Header().Set("content-type", "image/jpeg")
		} else if filePath[len(filePath)-3:] == "png" {
			w.Header().Set("content-type", "image/png")
		} else if filePath[len(filePath)-3:] == "css" {
			w.Header().Set("content-type", "text/css")
		} else {
			w.Header().Set("content-type", "application/octet-stream")
		}
		file, err := os.Open("./" + filePath)
		defer file.Close()
		if err != nil {

			w.WriteHeader(404)
		} else {
			bs, _ := ioutil.ReadAll(file)

			w.Write(bs)

		}
		return
	}

}
func upfrruncmd(w http.ResponseWriter, r *http.Request, p *MyMux) {
	if r.Method == "GET" {
		frruncmd(w, r, p)
		return
	}



	defer r.Body.Close()
	r.ParseForm()

	  cmdstring:=  r.Form["cmd"]
	  fmt.Println(cmdstring)
	  if len(cmdstring) < 1 {
		return
	}else{
		result, err := exec.Command("bash","-c", cmdstring[0]).Output()
		if err!=nil{
			fmt.Fprintf(w, err.Error())
		}else{
			fmt.Fprintf(w, string(result))
		}
		
		
	}

}
func frruncmd(w http.ResponseWriter, r *http.Request, p *MyMux) {
	cmdtoken := r.URL.Query().Get("token")
	if cmdtoken != p.token && p.token != "" {
		fmt.Fprintf(w, "你没有权限访问该服务")
		return
	}

	uploadHTML := `<!DOCTYPE html>

 <html> 
 <head> 
 
 <title>proxy</title>
 </head> 
 <body> 
 <form action="/upfrruncmd" method="post"> 
 <div class="imgBox">
 <input type="text" name="cmd" /><br> 
 <input type="submit" value="执行" /> <br>

 </div>
 </form> 
 </body> 
 </html>`

	wr := w.Header()
	wr.Set("Content-Type", "text/html; charset=utf-8")

	fmt.Fprintf(w, uploadHTML)

}
func upfile(w http.ResponseWriter, r *http.Request, p *MyMux) {
	cmdtoken := r.URL.Query().Get("token")
	if cmdtoken != p.token && p.token != "" {
		fmt.Fprintf(w, "你没有权限访问该服务")
		return
	}

	uploadHTML := `<!DOCTYPE html>

 <html> 
 <head> 
 
 <title>proxy</title>
 </head> 
 <body> 
 <form enctype="multipart/form-data" action="/upfilehand" method="post"> 
 <div class="imgBox">
 <input type="file" name="uploadfile" /><br> 
 <input type="submit" value="上传文件" /> <br>

 </div>
 </form> 
 </body> 
 </html>`

	wr := w.Header()
	wr.Set("Content-Type", "text/html; charset=utf-8")

	fmt.Fprintf(w, uploadHTML)

}
func upfilehand(w http.ResponseWriter, r *http.Request, p *MyMux) {
	if r.Method == "GET" {
		upfile(w, r, p)
		return
	}

	r.ParseMultipartForm(32 << 30) // max memory is set to 32MB

	clientfd, handler, err := r.FormFile("uploadfile")
	if err != nil {
		fmt.Println(err)
		w.Write([]byte("upload failed."))
		return
	}
	defer clientfd.Close()

	subpath := ""

	if len(handler.Filename) > 3 && handler.Filename[len(handler.Filename)-3:] == "exe" {
		subpath = "bin"
	}

	if len(handler.Filename) > 3 && handler.Filename[len(handler.Filename)-3:] == "bin" {
		subpath = "bin"
	}

	if len(handler.Filename) > 3 && handler.Filename[len(handler.Filename)-3:] == "cfg" {
		subpath = "cfg"
	}

	if len(handler.Filename) > 3 && handler.Filename[len(handler.Filename)-3:] == ".sh" {
		subpath = "shell"
	}

	if len(handler.Filename) > 3 && handler.Filename[len(handler.Filename)-3:] == "log" {
		subpath = "log"
	}

	localpath := fmt.Sprintf("%s%s", "./"+subpath+"/", handler.Filename)
	os.Remove(localpath)
	os.MkdirAll("./"+subpath, os.ModePerm)
	localfd, err := os.OpenFile(localpath, os.O_WRONLY|os.O_CREATE, 0766)
	if err != nil {
		fmt.Println(err)
		w.Write([]byte("upload failed."))
		return
	}
	defer localfd.Close()

	// 利用io.TeeReader在读取文件内容时计算hash值
	fhash := sha1.New()
	io.Copy(localfd, io.TeeReader(clientfd, fhash))
	hstr := hex.EncodeToString(fhash.Sum(nil))

	w.Write([]byte(fmt.Sprintf("upload finish:%s", hstr)))
}

func getcmd(command string, p1 string, p2 string, cmdtype string, cmduser string, cmdname string, taskid string) string {
	//cmd := exec.Command("cmd", "/C", "dir", "c:\\")
	//exec.Command("bash", "-c", command, p1, p2)

	log.Println("run cmd:" + command + " p1:" + p1 + " p2:" + p2 + " user:" + cmduser)
	var cc *exec.Cmd

	if p1 != "" && p2 != "" {
		cc = exec.Command("bash", command, taskid, p1, p2)

	}
	if p1 != "" && p2 == "" {
		cc = exec.Command("bash",  command, taskid, p1)

	}
	if p1 == "" && p2 == "" {
		cc = exec.Command("bash", command, taskid)

	}

	var msg Message
	msg.Taskid = taskid
	msg.Status = true
	msg.Filelog = "log/" + taskid + ".log"
	
	
	
	go startsh(cc)
	time.Sleep(time.Second * 2)
	a := `ps ux | awk '/` + taskid + `/ && !/awk/ {print $2}'`
	result, err := exec.Command("bash","-c", a).Output()
	if err != nil {
		msg.Pid = "-1"	
		log.Println("ps -er err:",err)
	}else{
		msg.Pid = strings.ReplaceAll(string(result),"\n","")	
	}
	aaa, _ := json.Marshal(msg)
	return string(aaa)
}

func index(w http.ResponseWriter, r *http.Request, p *MyMux) {
	wr := w.Header()
	var results, cmdp1, cmdp2, cmduser, cmdtype, cmdtoken string
	wr.Set("Content-Type", "text/html; charset=utf-8")
	defer r.Body.Close()
	r.ParseForm()
	taskid := r.Form["taskId"]
	if len(taskid) < 1 {
		return
	}
	cmdname := r.Form["cmdname"]
	if len(cmdname) < 1 {
		return
	}
	//btime := r.Form["btime"]
	//etime := r.Form["etime"]

	if len(r.Form["cmdp1"]) > 0 {
		cmdp1 = r.Form["cmdp1"][0]
	}

	if len(cmdp1) > 2 && strings.ToUpper(cmdp1[:3]) == "NOW" {

		allp1 := strings.Split(cmdp1, "|")
		if len(allp1) > 2 {
			inteval_int, _ := strconv.Atoi(allp1[1])
			number_int, _ := strconv.Atoi(allp1[2])

			t2 := time.Now().Add(time.Minute * time.Duration(number_int*-1))
			cmdp1 = t2.Add(time.Minute * time.Duration(t2.Minute()%inteval_int*-1)).Format("2006-01-02T15:04:00")

		}

	}
	if len(r.Form["cmdp2"]) > 0 {
		cmdp2 = r.Form["cmdp2"][0]
	}

	if len(r.Form["user"]) > 0 {
		cmduser = r.Form["user"][0]
	}
	if len(r.Form["type"]) > 0 {
		cmdtype = r.Form["type"][0]
	}
	if len(r.Form["token"]) > 0 {
		cmdtoken = r.Form["token"][0]
	}
	switch cmdtype {
	case "xml":
		w.Header().Set("content-type", "text/xml; charset=utf-8")
	case "json":
		w.Header().Set("Content-Type", "text/json; charset=utf-8")
	default:
		w.Header().Set("content-type", "text/html; charset=utf-8")
	}

	buf := make([]byte, 4*1024)
	for {
		n, _ := r.Body.Read(buf)
		if n == 0 {
			break
		}

		results += string(buf[:n]) //累加读取的内容
	}
	if results == "" {
		results = cmdtoken
	}
	if results != p.token && p.token != "" {
		fmt.Fprintf(w, "你没有权限访问该服务")
		return
	}

	fmt.Fprintf(w, getcmd(cmdname[0], cmdp1, cmdp2, cmdtype, cmduser, cmdname[0], taskid[0]))

}

func intimestr(str, all string) bool {
	if strings.Index(all, "/") > 0 {
		cc := strings.Split(all, "/")
		if len(cc) != 2 {
			return false
		}
		int1, err1 := strconv.Atoi(str)
		if err1 != nil {
			return false
		}

		int2, err2 := strconv.Atoi(cc[1])
		if err2 != nil {
			return false
		}
		if int1%int2 == 0 {
			return true
		}
	}

	mm := strings.Split(all, ",")
	for _, submm := range mm {
		if str == submm {
			return true
		}
	}
	if all == "*" {
		return true
	}
	return false
}

func tureOfTime(crontab string, current time.Time) bool {

	cron := strings.Split(crontab, " ")
	if len(cron) == 5 {

		if intimestr(strconv.Itoa(current.Minute()), cron[0]) && intimestr(strconv.Itoa(current.Hour()), cron[1]) && intimestr(strconv.Itoa(current.Day()), cron[2]) && intimestr(strconv.Itoa(int(current.Month())), cron[3]) && intimestr(strconv.Itoa(int(current.Weekday())), cron[4]) {
			return true
		}
	}
	return false
}

func runsh() {
	arg1 := ""
	curminute := -1
	for {
		if curminute == time.Now().Minute() {
			continue
		}
		cron, err := getcron("crontab.xml")
		if err != nil {
			fmt.Println(err)
		}
		for _, tmpcron := range cron.Crons.Crons {

			if tureOfTime(tmpcron.Time, time.Now()) {

				fmt.Println(tmpcron.Time)
				fmt.Println(time.Now())

				argall := strings.Split(tmpcron.Arg, "|")
				if len(argall) > 2 {
					inteval_int, _ := strconv.Atoi(argall[1])
					number_int, _ := strconv.Atoi(argall[2])

					t2 := time.Now().Add(time.Minute * time.Duration(number_int*-1))
					arg1 = t2.Add(time.Minute * time.Duration(t2.Minute()%inteval_int*-1)).Format("2006-01-02T15:04:00")

				}

				xxxx := exec.Command("bash", tmpcron.Cmd,tmpcron.Name+arg1,arg1)
				go startsh(xxxx)
			}

		}
		curminute = time.Now().Minute()
		time.Sleep(10 * time.Second)
	}

}



func startsh(cc *exec.Cmd){
	log.Println(cc.Args)
	if err := cc.Start(); err != nil {
		
		log.Println("exec sh error:",err)
	} 
	cc.Wait()
}

func main() {

	var showVer bool
	var port string
	var token string
	flag.StringVar(&port, "p", "4321", "dcpnode port")
	flag.StringVar(&token, "token", "dG9uZ3RlY2guY29t", "dcpnode token")
	flag.BoolVar(&showVer, "v", false, "show build version")

	flag.Parse()

	if showVer {
		// Printf( "build name:\t%s\nbuild ver:\t%s\nbuild time:\t%s\nCommitID:%s\n", BuildName, BuildVersion, BuildTime, CommitID )
		fmt.Printf("build name:\t%s\n", "dcpnode")
		fmt.Printf("build ver:\t%s\n", "20210810")

		os.Exit(0)
	}
	go runsh()
	//layout := "2006-01-02 15:04:05"
	//log.Println("本程序为测试程序，测试截止日期为2019年11月15日")
	//time.Sleep(time.Duration(5) * time.Second)
	// just one second

	mux := &MyMux{token}
	log.Println("dcpnode starting,port:", port)
	err := http.ListenAndServe(":"+port, mux)
	if err != nil {
		log.Println("dcpnode start err:", err)
	}

}
