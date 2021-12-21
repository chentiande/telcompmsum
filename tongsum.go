package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"bytes"

	_ "github.com/go-sql-driver/mysql"
)

var TIME_LOCATION_CST *time.Location
//全局错误标识，如果有错误，显示执行失败
var errflag bool
//sql执行方法，可以传多个参数，对每个sql执行显示时间

//初始化，增加日志，日志根据分钟自动创建在log目录下
func init() {
	TIME_LOCATION_CST, _ = time.LoadLocation("Asia/Shanghai")
	errflag = false
	_ = os.Mkdir("log", 755)
	file := "./log/tongsum" + time.Now().Format("200601021504") + ".log"

	logFile, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0766)
	if err != nil {
		panic(err)
	}
	log.SetOutput(logFile) // 将文件设置为log输出的文件
	log.SetPrefix("[tongsum]")
	//日志标识
	log.SetFlags(log.LstdFlags)
	return
}


func PKCS7Padding(ciphertext []byte, blockSize int) []byte {
	padding := blockSize - len(ciphertext)%blockSize
	padtext := bytes.Repeat([]byte{byte(padding)}, padding)
	return append(ciphertext, padtext...)
}

func PKCS7UnPadding(origData []byte) []byte {
	length := len(origData)
	unpadding := int(origData[length-1])
	return origData[:(length - unpadding)]
}

func AesEncrypt(origData, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	blockSize := block.BlockSize()
	origData = PKCS7Padding(origData, blockSize)
	blockMode := cipher.NewCBCEncrypter(block, key[:blockSize])
	crypted := make([]byte, len(origData))
	blockMode.CryptBlocks(crypted, origData)
	return crypted, nil
}

func AesDecrypt(crypted, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	blockSize := block.BlockSize()
	blockMode := cipher.NewCBCDecrypter(block, key[:blockSize])
	origData := make([]byte, len(crypted))
	blockMode.CryptBlocks(origData, crypted)
	origData = PKCS7UnPadding(origData)
	return origData, nil
}


//密码加密
func cpwd(pwd string) string {
	key := []byte("12Q45#7@90eR34Wx")
	result, err := AesEncrypt([]byte(pwd), key)
	if err != nil {
		fmt.Println("password decrypt error：", err)
	}
	return base64.StdEncoding.EncodeToString(result)

}
//密码解密
func dpwd(pwd string) string {
	if pwd == "" {
		return ""
	}
	key := []byte("12Q45#7@90eR34Wx")

	data, err1 := base64.StdEncoding.DecodeString(pwd)
	if err1 != nil {
		fmt.Println("password decrypt error：", err1)
	}
	origData, err := AesDecrypt(data, key)
	if err != nil {
		fmt.Println("password decrypt error：", err)
	}
	return string(origData)
}

func main() {

	tt := time.Now()

	var showVer = flag.Bool("v", false, "show build version")
	var confstr = flag.String("conf", "tongsum.cfg", "汇总配置文件路径，默认为tongsum.cfg")
	var starttime = flag.String("starttime", "", "开始时间，sql中可使用#starttime#代替")
	var endtime = flag.String("endtime", "", "结束时间，sql中可使用#endtime#代替")
	var sumlevel = flag.String("sumlevel", "0", "汇总维度 0-小时，1-天，2-周，3-月，sql中可以使用#sumlevel#代替")
	var  mima = flag.String("m", "", "password Encryption")
	flag.Parse()
	if *mima != "" {
		// Printf( "build name:\t%s\nbuild ver:\t%s\nbuild time:\t%s\nCommitID:%s\n", BuildName, BuildVersion, BuildTime, CommitID )
		fmt.Printf("password Encryption:\t%s\n", cpwd(*mima))
		os.Exit(0)
	}
	if *showVer {

		fmt.Printf("build name:\t%s\n", "tongsum for mysql")
		fmt.Printf("build ver:\t%s\n", "20210702")
	

		os.Exit(0)
	}
	dbsql, sqlerr := getsql(*confstr)
	if sqlerr != nil {
		fmt.Println("get "+*confstr+" err:", sqlerr)
	}
	dbconf, dberr := getconf("db.conf")
	if dberr != nil {
		fmt.Println("get db.conf err:", dberr)
	}
	db, err := sql.Open("mysql", dbconf.Dbuser+":"+dpwd(dbconf.Dbpwd)+"@tcp("+dbconf.Dbip+":"+dbconf.Dbport+")/"+dbconf.Dbname+"?charset=utf8&allowOldPasswords=1")
	if err != nil {
		fmt.Println(err)
	}
	defer db.Close()

	

	// See "Important settings" section.
	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(1)

	for x := range dbsql.Sqls {
		sqlname := dbsql.Sqls[x].Sqlname
		strsql := dbsql.Sqls[x].Sql
		arg := dbsql.Sqls[x].Arg
		cm := dbsql.Sqls[x].Cm
		//layout := "2006-01-02 15:04:05"

		if *starttime == "" {
			*starttime = string(time.Now().AddDate(0, 0, -1).Format("2006-01-02")) + " 00:00:00"
		}
		if *endtime == "" {
			*endtime = string(time.Now().AddDate(0, 0, -1).Format("2006-01-02")) + " 23:59:59"
		}

		//替换变量
		strsql = strings.Replace(strsql, "#starttime#", *starttime, -1)
		strsql = strings.Replace(strsql, "#endtime#", *endtime, -1)
		strsql = strings.Replace(strsql, "#sumlevel#", *sumlevel, -1)
		t := time.Now()
		//循环获取arg数据拆分后循环执行

		//如果配置的参数不为空，判断sql或者参数
		if arg != "" {
			log.Println("Arg:", arg)
			//如果参数中是sql语句，则查询后进行参数替换
			if strings.Contains(arg, "select") {
				rows, err := db.Query(arg)
				if err != nil {
					errflag = true
					log.Printf("Err(%s),sql=%s\n", err, arg)
					os.Exit(0)
				}
				cols, _ := rows.Columns()
				rawResult := make([][]byte, len(cols))

				dest := make([]interface{}, len(cols))
				for i := range rawResult {
					dest[i] = &rawResult[i]
				}

				for rows.Next() {

					err = rows.Scan(dest...)
					strsql_t := strsql
					cmstr := cm
					for i, raw := range rawResult {

						strsql_t = strings.Replace(strsql_t, "#arg"+strconv.Itoa(i+1)+"#", string(raw), -1)
					}
					t := time.Now()

					if strsql_t[0:6] == "select" {

						rows, err := db.Query(strsql_t)
						if err != nil {
							errflag = true
							log.Printf("Err(%s),sql=%s\n", err, strsql_t)
							os.Exit(0)
						}
						i := 0
						for rows.Next() {
							i++
						}
						elapsed := int(time.Since(t)) / 1000000

						//如果配置了cm，则比对cm和查询的数据量
						if cm != "" {

							for i, raw := range rawResult {

								cmstr = strings.Replace(cmstr, "#arg"+strconv.Itoa(i+1)+"#", string(raw), -1)
							}
							var count int
							log.Println("Cm:", cmstr)
							db.QueryRow(cmstr).Scan(&count)
							if count == i {
								log.Printf("OK(%s),time=%dms,num=%d,cm=%d,sql=%s\n", sqlname, elapsed, i, count, strsql_t)
							} else {
								errflag = true
								log.Printf("ERR(%s),time=%dms,num=%d,cm=%d,sql=%s\n", sqlname, elapsed, i, count, strsql_t)
							}

						} else {
							log.Printf("OK(%s),time=%dms,num=%d,sql=%s\n", sqlname, elapsed, i, strsql_t)
						}

					} else {
						res, err := db.Exec(strsql_t)

						elapsed := int(time.Since(t)) / 1000000
						if err != nil {
							errflag = true
							log.Printf("Err(%s),sql=%s\n", err, strsql_t)

							if strings.Contains(strsql_t, "create") || strings.Contains(strsql_t, "drop") {

							} else {
								os.Exit(0)
							}

						} else {
							num, _ := res.RowsAffected()
							log.Printf("OK(%s),time=%dms,num=%d,sql=%s\n", sqlname, elapsed, num, strsql_t)
						}
					}
				}

			} else { //如果是字符串，第一次用#拆分循环替换执行sql参数，二次用，拆分循环替换sql的中arg[]

				for _, s := range strings.Split(arg, "#") {
					strsql_t := strsql
					cmstr:=cm
					for i, ss := range strings.Split(s, ",") {
						strsql_t = strings.Replace(strsql_t, "#arg"+strconv.Itoa(i+1)+"#", ss, -1)
					}
					t := time.Now()
					if strsql_t[0:6] == "select" {

						rows, err := db.Query(strsql_t)
						if err != nil {
							errflag = true
							log.Printf("Err(%s),sql=%s\n", err, strsql_t)
							os.Exit(0)
						}
						i := 0
						for rows.Next() {
							i++
						}
						elapsed := int(time.Since(t)) / 1000000
						if cm != "" {

							for i, ss := range strings.Split(s, ",") {
								cmstr = strings.Replace(cmstr, "#arg"+strconv.Itoa(i+1)+"#", ss, -1)
							}
							var count int
							log.Println("Cm:", cmstr)
							db.QueryRow(cmstr).Scan(&count)
							if count == i {
								log.Printf("OK(%s),time=%dms,num=%d,cm=%d,sql=%s\n", sqlname, elapsed, i, count, strsql_t)
							} else {
								errflag = true
								log.Printf("ERR(%s),time=%dms,num=%d,cm=%d,sql=%s\n", sqlname, elapsed, i, count, strsql_t)
							}

						} else {

							log.Printf("OK(%s),time=%dms,num=%d,sql=%s\n", sqlname, elapsed, i, strsql_t)
						}
					} else {
						res, err := db.Exec(strsql_t)

						elapsed := int(time.Since(t)) / 1000000
						if err != nil {
							errflag = true
							log.Printf("Err(%s),sql=%s\n", err, strsql_t)

							if strings.Contains(strsql_t, "create") || strings.Contains(strsql_t, "drop") {

							} else {
								os.Exit(0)
							}

						} else {

							num, _ := res.RowsAffected()
							log.Printf("OK(%s),time=%dms,num=%d,sql=%s\n", sqlname, elapsed, num, strsql_t)
						}
					}
				}
			}
		} else { //如果为空，则直接执行
			if strsql[0:6] == "select" {

				rows, err := db.Query(strsql)
				if err != nil {
					errflag = true
					log.Printf("Err(%s),sql=%s\n", err, strsql)
					os.Exit(0)
				}
				i := 0
				for rows.Next() {
					i++
				}
				elapsed := int(time.Since(t)) / 1000000
				if cm != "" {
					log.Println("Cm:", cm)

					var count int
					db.QueryRow(cm).Scan(&count)
					if count == i {
						log.Printf("OK(%s),time=%dms,num=%d,cm=%d,sql=%s\n", sqlname, elapsed, i, count, strsql)
					} else {
						errflag = true
						log.Printf("ERR(%s),time=%dms,num=%d,cm=%d,sql=%s\n", sqlname, elapsed, i, count, strsql)
					}

				} else {
					log.Printf("OK(%s),time=%dms,num=%d,sql=%s\n", sqlname, elapsed, i, strsql)
				}
			} else {
				res, err := db.Exec(strsql)

				elapsed := int(time.Since(t)) / 1000000
				if err != nil {
					errflag = true
					log.Printf("Err(%s),sql=%s\n", err, strsql)

					if strings.Contains(strsql, "create") || strings.Contains(strsql, "drop") {

					} else {
						os.Exit(0)
					}

				} else {
					num, _ := res.RowsAffected()
					log.Printf("OK(%s),time=%dms,num=%d,sql=%s\n", sqlname, elapsed, num, strsql)
				}
			}

		}

	}

	alltime := int(time.Since(tt)) / 1000000
	if errflag {
		log.Printf("%s 执行失败,time=%dms\n", *confstr, alltime)
		fmt.Printf("%s 执行失败,time=%dms\n", *confstr, alltime)
	}else {
		log.Printf("%s 执行成功,time=%dms\n", *confstr, alltime)
		fmt.Printf("%s 执行成功,time=%dms\n", *confstr, alltime)
	}
	

}
