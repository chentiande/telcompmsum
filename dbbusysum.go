package main

import (
	"database/sql"

	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/shopspring/decimal"

	//"hash/crc32"
	"github.com/roy2220/bptree"
)

var TIME_LOCATION_CST *time.Location

//全局错误标识，如果有错误，显示执行失败
var errflag bool

func sqlexec(db *sql.DB, lqs string, args ...interface{}) {
	t := time.Now()

	if len(args) > 0 {
		_, err := db.Exec(lqs, args...) //res
		if err != nil {
			log.Fatalln("Err(%s),sql=%s,args=%v\n", err, lqs, args)
		}
		//  else {
		// 	log.Println(sql,args)
		// 	num, _ := res.RowsAffected()
		// 	elapsed := int(time.Since(t)) / 1000000
		// 	log.Printf("OK(%s),time=%dms,num=%d,sql=%s,args=%v\n", "runsql", elapsed, num, sql, args)
		// }

	} else {
		res, err := db.Exec(lqs)
		if err != nil {
			log.Fatalln("Err(%s),sql=%s\n", err, lqs)
		} else {
			num, _ := res.RowsAffected()
			elapsed := int(time.Since(t)) / 1000000
			log.Printf("OK(%s),time=%dms,num=%d,sql=%s\n", "runsql", elapsed, num, lqs)
		}
	}

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
	log.SetPrefix("[dbbusysum]")
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

	var source = flag.String("mt", "dw_ct_pm_4g_cel_h_cel", "要计算的忙时表")
	var source1 = flag.String("st", "dw_ct_pm_4g_cel_h_cel", "要抽提的表")
	var dest = flag.String("dt", "dw_ct_pm_4g_cel_bh_cel", "要输出的表")
	var neid = flag.String("neid", "rdn", "主键")
	var ptimecolumn = flag.String("ptimecolumn", "starttime", "时间字段")
	var allcom = flag.String("allcom", "dtch_prb_assn_mean_dl,prb_dl_avail", "需要插入忙时表的字段")
	var maxstr = flag.String("max", "ifnull(divnull(dtch_prb_assn_mean_dl,prb_dl_avail),0) aaa#1|ifnull(pdcp_sdu_oct_dl,0) bbb#2|ifnull(divnull(dtch_prb_assn_mean_ul,prb_ul_avail),0) ccc#3|ifnull(pdcp_sdu_oct_ul,0) ddd#4|ifnull(rrc_att_conn_estab_net+rrc_att_conn_estab_ue,0) eee#5|ifnull(vltevoice_time,0) fff#6", "设置使用max计算的字段，用,分割")
	var logfile = flag.String("log", "", "日志文件名")
	var showVer = flag.Bool("v", false, "显示版本信息")
	var interval = flag.Int64("interval", 3600, "源表数据维度周期，单位为秒，3600为1小时粒度")
	var timelen = flag.Int64("timelen", 24, "汇聚时间周期，q-->h 设置为4，h-->d 设置为24")
	//var num = flag.Uint64("num", 1, "数据拆分数量")
	var num int64
	num = 1
	var starttime = flag.String("starttime", "2020-10-11 10:00:00", "开始时间")
	var stoptime = flag.String("stoptime", "", "结束时间")
	var mima = flag.String("m", "", "password Encryption")
	flag.Parse()
	if *mima != "" {
		// Printf( "build name:\t%s\nbuild ver:\t%s\nbuild time:\t%s\nCommitID:%s\n", BuildName, BuildVersion, BuildTime, CommitID )
		fmt.Printf("password Encryption:\t%s\n", cpwd(*mima))
		os.Exit(0)
	}

	*starttime = strings.Replace(*starttime, "T", " ", -1)
	if *starttime == "" {
		*starttime = string(time.Now().AddDate(0, 0, -1).Format("2006-01-02")) + " 00:00:00"
	}

	if *stoptime != "" {
		layout := "2006-01-02 15:04:05"
		t0, err := time.Parse(layout, *starttime)
		if err != nil {
			log.Println("输入stoptime有误，请重新输入", *starttime)
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


	if *logfile == "" {
		time1 := time.Now().Format("200601021504.999999999")
		init1(time1)
	} else {
		init1(*logfile)

	}
	dbconf, dberr := getconf("db.conf")
	if dberr != nil {
		log.Println("get db.conf err:", dberr)
	}

	db, err := sql.Open("mysql", dbconf.Dbuser+":"+dpwd(dbconf.Dbpwd)+"@tcp("+dbconf.Dbip+":"+dbconf.Dbport+")/"+dbconf.Dbname+"?charset=utf8&allowOldPasswords=1")
	if err != nil {
		log.Fatalln("db err:", err)
	}
	defer db.Close()

	if *showVer {
		// Printf( "build name:\t%s\nbuild ver:\t%s\nbuild time:\t%s\nCommitID:%s\n", BuildName, BuildVersion, BuildTime, CommitID )
		fmt.Printf("build name:\t%s\n", "tongtech dbbusysum")
		fmt.Printf("build ver:\t%s\n", "20210930")

		os.Exit(0)
	}
	log.Printf("max字段：%s", *maxstr)

	var i uint64
	var intChan chan int
	intChan = make(chan int, num)

	//如果加了num参数，将取模后对数据进行拆分，输入文件前面为1_ 2_

	runtask(*allcom, *ptimecolumn, *neid, *starttime, *stoptime, *interval, *timelen, i+1, *dest, *source, *source1, intChan, *maxstr, db)

}

//通过btree存储主键，循环实现数据计算
func runtask(allcom, ptimecolumn, neid, starttime, stoptime string, interval, timelen int64, i uint64, dest, source,source1 string, ch_run chan int, maxstr string, db *sql.DB) {
	insertsql := ""
	selectsql := ""
	
	if maxstr != "" {
		selectsql = selectsql + maxstr + ","
	}

	allzb := strings.Split(maxstr, "|")
	for x := range allzb {
		y := strings.Split(allzb[x], "#")
		selectsql = y[0] + ","

		deletesql := "delete from " + dest + " where starttime=? and bh_type=?"
	    sqlexec(db, deletesql, starttime,y[1])

		insertsql = `insert into ` + dest + ` (bh_type,starttime,busy_time,` + neid + `,` + allcom + `)
	          select  ` + y[1] + `,?,starttime,` + neid + `,` + allcom + ` from ` + source1 + ` where ` + neid + `=? and starttime=?`
		selectsql = "select " + neid + "," + selectsql + ptimecolumn + " from " + source + " where " + ptimecolumn + "=?"

		mm := 0
		bpt := new(bptree.BPTree).Init(
			5, // maximum degree
			func(key1, key2 interface{}) int64 { // key comparer
				return int64(strings.Compare(key1.(string), key2.(string)))
			},
		)

		bb := make(map[string]string)
		xmap := make(map[int]string)
		var allne []string

		layout := "2006-01-02 15:04:05"
		t1, _ := time.Parse(layout, starttime)
		var j int64
		for j = 0; j < timelen; j++ {
			xxxx, _ := time.ParseDuration(strconv.FormatInt(interval*j, 10) + "s")
			xx := t1.Add(xxxx)
			log.Printf("selectsql:%s,time:%s\n", selectsql, xx.Format(layout))

			rows, err := db.Query(selectsql, xx.Format(layout))
			if err != nil {
				log.Fatalln("err:", err)
			}

			cols, _ := rows.Columns()

			rawResult := make([]string, len(cols))

			dest := make([]interface{}, len(cols))
			for i := range rawResult {
				dest[i] = &rawResult[i]
			}

			for k, v := range cols {
				xmap[k] = v

			}

			for rows.Next() {

				err = rows.Scan(dest...)

				vvvv, ok := bpt.HasRecord(string(rawResult[0]))

				if ok {

					_ = json.Unmarshal([]byte(vvvv.(string)), &bb)

					rata, _ := decimal.NewFromString(string(rawResult[1]))

					aa, _ := decimal.NewFromString(bb[xmap[1]])

					if rata == decimal.Max(aa, rata) {
						bb[xmap[1]] = string(rawResult[1])
						bb[xmap[2]] = string(rawResult[2])
					}

					mjson, _ := json.Marshal(bb)

					_, _ = bpt.UpdateRecord(string(rawResult[0]), string(mjson))
				} else {

					if string(rawResult[1]) == "" || string(rawResult[1]) == "NULL" {
						aa, _ := decimal.NewFromString("0")
						bb[xmap[1]] = aa.String()
						bb[xmap[2]] = string(rawResult[2])
					} else {
						aa, _ := decimal.NewFromString(string(rawResult[1]))
						bb[xmap[1]] = aa.String()
						bb[xmap[2]] = string(rawResult[2])
					}

					mjson, _ := json.Marshal(bb)
					bpt.AddRecord(rawResult[0], string(mjson))
					allne = append(allne, string(rawResult[0]))
				}
			}

			//log.Println(str)
			//w1.Write(strings.Split(str[0:len(str)-1],"|")) //写入表头数据

		}

		// 	 //log.Println(str)
		// 	 w1.Write(strings.Split(str[0:len(str)-1],"|")) //写入表头数据
		//  w1.Flush()

		for _, ne := range allne {

			vvvvv, _ := bpt.HasRecord(ne)

			_ = json.Unmarshal([]byte(vvvvv.(string)), &bb)

			valuestr := ne + "|"
			for i := 1; i < len(bb)+1; i++ {

				valuestr = valuestr + bb[xmap[i]] + "|"

			}

			values := strings.Split(valuestr[0:len(valuestr)-1], "|")
			if len(values) > 0 {

				s := make([]interface{}, len(values))
				s[0] = starttime
				s[1] = values[0]
				s[2] = values[2]

				sqlexec(db, insertsql, s...)
			}
			mm++
		}
		//log.Println(insertsql)
		log.Printf("任务执行完毕，处理数据条数%d条,表名：%s", mm, dest)

	}
}
func isin(allstr, str string) bool {
	xx := strings.Split(allstr, ",")
	for v := range xx {
		if xx[v] == str {
			return true
		}
	}
	return false
}
