package main

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"database/sql"
	"encoding/base64"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var TIME_LOCATION_CST *time.Location

//全局错误标识，如果有错误，显示执行失败
var errflag bool

//sql执行方法，可以传多个参数，对每个sql执行显示时间
func sqlexec(db *sql.DB, lqs string, args ...interface{}) {
	t := time.Now()

	if len(args) > 0 {
		res, err := db.Exec(lqs, args...)
		if err != nil {
			errflag = true
			log.Printf("Err(%s),sql=%s,%v\n", err, lqs, args)
		} else {
			num, _ := res.RowsAffected()
			elapsed := int(time.Since(t)) / 1000000
			log.Printf("OK(%s),time=%dms,num=%d,sql=%s,%v\n", "runsql", elapsed, num, lqs, args)
		}

	} else {
		res, err := db.Exec(lqs)
		if err != nil {
			errflag = true
			log.Printf("Err(%s),sql=%s\n", err, lqs)
		} else {
			num, _ := res.RowsAffected()
			elapsed := int(time.Since(t)) / 1000000
			log.Printf("OK(%s),time=%dms,num=%d,sql=%s\n", "runsql", elapsed, num, lqs)
		}
	}

}

// 生成随机的临时表
func tmptable(n int) string {
	letters := []rune("abcdefghijklmnopqrstuvwxyz")

	rand.Seed(time.Now().UnixNano())
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}

	return string(b)
}

//根据配置的字段拆分后加前缀pm，如果有#号分割的，自动加别名和函数名
func timesqlpin(column string, hanshu string) string {
	allc := strings.Split(column, ",")
	allstr := ""
	for _, v := range allc {
		name := strings.Split(v, "#")
		if len(name) == 2 {
			allstr = allstr + hanshu + "(pm." + name[0] + ") " + name[1] + ","
		} else if name[0] != "" {
			allstr = allstr + hanshu + "(pm." + name[0] + ") " + name[0] + ","
		} else {
			return ""
		}
	}
	return allstr[:len(allstr)-1]
}

////根据配置的字段拆分后,和timesqlpin不同的是不加前缀pm，如果有#号分割的，自动加别名和函数名
func sqlpin(column string, hanshu string) string {
	allc := strings.Split(column, ",")
	allstr := ""
	for _, v := range allc {
		name := strings.Split(v, "#")

		if len(name) == 2 {
			allstr = allstr + hanshu + "(" + name[1] + "),"
		} else if name[0] != "" {
			allstr = allstr + hanshu + "(" + name[0] + "),"
		} else {
			return ""
		}

	}
	return allstr[:len(allstr)-1]
}

//根据配置的字段拆分后,，如果有#号分割的，自动加别名，不加函数
func timesqlpin1(column string, hanshu string) string {
	allc := strings.Split(column, ",")
	allstr := ""
	for _, v := range allc {
		name := strings.Split(v, "#")

		if len(name) == 2 {
			allstr = allstr + name[1] + ","
		} else if name[0] != "" {
			allstr = allstr + name[0] + ","
		} else {
			return ""
		}

	}
	return allstr[:len(allstr)-1]
}

//初始化，增加日志，日志根据分钟自动创建在log目录下
func init1(time1 string) {
	TIME_LOCATION_CST, _ = time.LoadLocation("Asia/Shanghai")
	errflag = false
	_ = os.Mkdir("log", 755)
	file := "./log/" + time1 + ".log"

	logFile, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0766)
	if err != nil {
		panic(err)
	}
	log.SetOutput(logFile) // 将文件设置为log输出的文件
	log.SetPrefix("[tongsumex]")
	//日志标识
	log.SetFlags(log.LstdFlags)
	return
}

//根据sql语句生成建表脚本
func cr_cr_sql(db *sql.DB, strsql string, tablename string) string {
	result := ""
	rows, err := db.Query(strsql)
	if err != nil {
		log.Fatal(err)
	}
	cols, _ := rows.Columns()
	colTypes, _ := rows.ColumnTypes()
	for i, s := range colTypes {
		myString := ""
		if s.DatabaseTypeName() == "VARCHAR" || s.DatabaseTypeName() == "CHAR" {
			if cols[i]=="dn"{
				myString = "(1024)"
			}else{
				myString = "(255)"
			}
			
		}
		if s.DatabaseTypeName() == "DECIMAL"  {
			myString="(20, 4)"
			}
			if s.DatabaseTypeName() == "BIGINT"  {
				myString="(64)"
				}
		result = result + cols[i] + " " + s.DatabaseTypeName() + "" + myString + ","
	}

	return "create table " + tablename + " (" + result[:len(result)-1] + ")"
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
	var confstr = flag.String("conf", "tongsumex.cfg", "汇总配置文件路径，默认为tongsumex.cfg")
	var starttime = flag.String("starttime", "", "开始时间")
	var stoptime = flag.String("stoptime", "", "结束时间")
	var interval = flag.Int64("interval", 900, "源表数据维度周期，单位为秒，3600为1小时粒度")
	var timelen = flag.Int64("num", 4, "汇聚时间周期，q-->h 设置为4，h-->d 设置为24")
	var logfile = flag.String("log", "", "日志文件名")
	var mima = flag.String("m", "", "password Encryption")
	flag.Parse()
	if *mima != "" {
		// Printf( "build name:\t%s\nbuild ver:\t%s\nbuild time:\t%s\nCommitID:%s\n", BuildName, BuildVersion, BuildTime, CommitID )
		fmt.Printf("password Encryption:\t%s\n", cpwd(*mima))
		os.Exit(0)
	}
	if *logfile == "" {
		time1 := time.Now().Format("200601021504.999999999")
		init1(time1)
	} else {
		init1(*logfile)

	}

	dbsum, sqlerr := getsum(*confstr)
	if sqlerr != nil {
		fmt.Println("get "+*confstr+" err:", sqlerr)
	}
	dbconf, dberr := getconf("db.conf")
	if dberr != nil {
		fmt.Println("get db.conf err:", dberr)
	}

	db, err := sql.Open("mysql", dbconf.Dbuser+":"+dpwd(dbconf.Dbpwd)+"@tcp("+dbconf.Dbip+":"+dbconf.Dbport+")/"+dbconf.Dbname+"?charset=utf8&allowOldPasswords=1")

	if err != nil {
		log.Fatalln("db err:", err)
	}
	defer db.Close()

	if *showVer {

		fmt.Printf("build name:\t%s\n", "tongsumex for mysql")
		fmt.Printf("build ver:\t%s\n", "20210802")
		fmt.Printf("build author:\t%s\n", "chentiande")

		os.Exit(0)
	}

	// See "Important settings" section.
	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(2)

	var ca_s_column, ca_d_column, ods_s_column, ods_d_column, dest_table string

	if timesqlpin(dbsum.Sum, "sum") != "" {
		ca_s_column = ca_s_column + timesqlpin(dbsum.Sum, "sum") + ","
		ca_d_column = ca_d_column + timesqlpin1(dbsum.Sum, "sum") + ","

	}

	if timesqlpin(dbsum.Avg, "avg") != "" {
		ca_s_column = ca_s_column + timesqlpin(dbsum.Avg, "avg") + ","
		ca_d_column = ca_d_column + timesqlpin1(dbsum.Avg, "avg") + ","

	}

	if timesqlpin(dbsum.Min, "min") != "" {
		ca_s_column = ca_s_column + timesqlpin(dbsum.Min, "min") + ","
		ca_d_column = ca_d_column + timesqlpin1(dbsum.Min, "min") + ","

	}
	if timesqlpin(dbsum.Max, "max") != "" {
		ca_s_column = ca_s_column + timesqlpin(dbsum.Max, "max") + ","
		ca_d_column = ca_d_column + timesqlpin1(dbsum.Max, "max") + ","

	}
	for _, v := range dbsum.Defs.Defs {
		ca_s_column = ca_s_column + v.Expr + " " + v.Column + ","
		ca_d_column = ca_d_column + v.Dsum + "(" + v.Column + "),"

	}

	tmp_pm := "tmp_pm" + tmptable(10)
	tmp_cm := "tmp_cm" + tmptable(10)
	tmp_ods := "tmp_ods" + tmptable(10)

	//layout := "2006-01-02 15:04:05"
	*starttime = strings.Replace(*starttime, "T", " ", -1)
	if *starttime == "" {
		*starttime = string(time.Now().AddDate(0, 0, -1).Format("2006-01-02")) + " 00:00:00"
	}
	if *stoptime != "" {
		layout := "2006-01-02 15:04:05"
		t0, err := time.Parse(layout, *starttime)
		if err != nil {
			log.Println("输入starttime有误，请重新输入", *starttime)
		}
		t1, err1 := time.Parse(layout, *stoptime)
		if err1 != nil {
			log.Println("输入stoptime有误，请重新输入", *stoptime)
		}
		if t0.Before(t1) {
			diff := t1.Unix() - t0.Unix() //

			*timelen = diff / (*interval)

			log.Println(*timelen)
		}
	}

	layout := "2006-01-02 15:04:05"

	//time.Sleep(time.Duration(5) * time.Second)
	// just one second

	//替换变量

	//循环获取arg数据拆分后循环执行

	//如果配置的参数不为空，判断sql或者参数

	t1, _ := time.Parse(layout, *starttime)

	//是否内存表计算
	memtableflag := ""
	if dbconf.Dbmemtable == "true" {
		memtableflag = " engine=memory "
	}

	sqlexec(db, "create  table "+tmp_pm+memtableflag+" like "+strings.Split(dbsum.Pmtable, " ")[0])

	sqlexec(db, "create index idx_"+tmp_pm[:10]+" on "+tmp_pm+"("+dbsum.Pmindex+")")
	var m int64

	Ptimecolumn := strings.Split(dbsum.Ptimecolumn, "#")
	Ptimecolumn1 := Ptimecolumn[0]
	Ptimecolumn2 := ""
	begintime := ""
	if len(Ptimecolumn) > 1 {
		Ptimecolumn2 = Ptimecolumn[1]
	} else {
		Ptimecolumn2 = Ptimecolumn1
	}

	for m = 0; m < *timelen; m++ {
		xxxx, _ := time.ParseDuration(strconv.FormatInt(*interval*m, 10) + "s")
		yyyy, _ := time.ParseDuration(strconv.FormatInt(*interval*(m+1), 10) + "s")
		xx := t1.Add(xxxx)
		yy := t1.Add(yyyy)
		begintime = xx.Format(layout)
		tmpendtime := yy.Format(layout)
		sqlexec(db, "insert into "+tmp_pm+" select * from "+dbsum.Pmtable+" where "+Ptimecolumn1+">=STR_TO_DATE(?,'%Y-%m-%d %H:%i:%s') and "+Ptimecolumn1+"<STR_TO_DATE(?,'%Y-%m-%d %H:%i:%s')", begintime, tmpendtime)

	}

	dest_table = tmp_pm + " pm"
	if dbsum.Cmtable != "" {
		dest_table = dest_table + " join " + tmp_cm + " cm"
		
		sqlexec(db, cr_cr_sql(db, dbsum.Cmtable, tmp_cm))
		
		sqlexec(db, "create index idx_"+tmp_cm[:10]+" on "+tmp_cm+"("+dbsum.Cmindex+")")
		sqlexec(db, "insert into "+tmp_cm+" "+dbsum.Cmtable)
		tmp_ods_sql := "select STR_TO_DATE('" + *starttime + "','%Y-%m-%d %H:%i:%s') " + Ptimecolumn2 + "," + dbsum.Dimcolumn + "," + ca_s_column[:len(ca_s_column)-1] + "  from " + dest_table + " on " + dbsum.Joinstr + " where 1=2  group by " + dbsum.Dimcolumn
		sqlexec(db, cr_cr_sql(db, tmp_ods_sql, tmp_ods))

		sqlexec(db, "insert  into "+tmp_ods+memtableflag+" select STR_TO_DATE('"+*starttime+"','%Y-%m-%d %H:%i:%s') "+Ptimecolumn2+","+dbsum.Dimcolumn+","+ca_s_column[:len(ca_s_column)-1]+"  from "+dest_table+" on "+dbsum.Joinstr+" group by "+dbsum.Dimcolumn)

	} else {
		tmp_ods = tmp_pm
	}

	for _, v := range dbsum.Sums.Sums {

		ods_s_column = ""
		ods_d_column = ""

		if sqlpin(v.Sum, "sum") != "" {
			ods_s_column = ods_s_column + sqlpin(v.Sum, "sum") + ","
			ods_d_column = ods_d_column + timesqlpin1(v.Sum, "sum") + ","

		}

		if sqlpin(v.Avg, "avg") != "" {
			ods_s_column = ods_s_column + sqlpin(v.Avg, "avg") + ","
			ods_d_column = ods_d_column + timesqlpin1(v.Avg, "avg") + ","

		}

		if timesqlpin(v.Min, "min") != "" {
			ods_s_column = ods_s_column + sqlpin(v.Min, "min") + ","
			ods_d_column = ods_d_column + timesqlpin1(v.Min, "min") + ","

		}
		if timesqlpin(v.Max, "max") != "" {
			ods_s_column = ods_s_column + sqlpin(v.Max, "max") + ","
			ods_d_column = ods_d_column + timesqlpin1(v.Max, "max") + ","

		}
		for _, m := range v.Defs.Defs {
			ods_s_column = ods_s_column + m.Expr + " " + m.Column + ","
			ods_d_column = ods_d_column + m.Column + ","

		}

		starttimestr := ""
		dtimecolumn := strings.Split(v.Dtimecolumn, " ")
		if len(dtimecolumn) == 1 {
			starttimestr = dtimecolumn[0]
		} else {
			starttimestr = dtimecolumn[len(dtimecolumn)-1]
		}

		sqlexec(db, "delete from "+v.Desttable+" where "+v.Dtimecolumn+"=STR_TO_DATE(?,'%Y-%m-%d %H:%i:%s')", *starttime)

		sqlexec(db, "insert into "+v.Desttable+" ("+starttimestr+","+v.Dimcolumn+","+ods_d_column[:len(ods_d_column)-1]+") select STR_TO_DATE(?,'%Y-%m-%d %H:%i:%s'),"+v.Dimcolumn+","+ods_s_column[:len(ods_s_column)-1]+" from "+tmp_ods+" group by "+v.Dimcolumn, *starttime)
	}

	sqlexec(db, "drop table "+tmp_pm)
	if dbsum.Cmtable != "" {
		sqlexec(db, "drop table "+tmp_cm)
		sqlexec(db, "drop table "+tmp_ods)
	}

	alltime := int(time.Since(tt)) / 1000000
	if errflag {
		log.Printf("%s 执行失败,time=%dms\n", *confstr, alltime)
		fmt.Printf("%s 执行失败,time=%dms\n", *confstr, alltime)
	} else {
		log.Printf("%s 执行成功,time=%dms\n", *confstr, alltime)
		fmt.Printf("%s 执行成功,time=%dms\n", *confstr, alltime)
	}

}
