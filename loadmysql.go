package main

//从mysql数据库导出文件，可导出xlsx和csv两种格式
//作者：chentiande
//

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strings"
    "flag"
	"strconv"
	"github.com/axgle/mahonia"
	_ "github.com/go-sql-driver/mysql"
)

//增加GBK到utf8函数转换，将数据库取出的数据转成uft8然后保存到excel
func ConvertToString(src string, srcCode string, tagCode string) string {
	srcCoder := mahonia.NewDecoder(srcCode)
	srcResult := srcCoder.ConvertString(src)
	tagCoder := mahonia.NewDecoder(tagCode)
	_, cdata, _ := tagCoder.Translate([]byte(srcResult), true)
	result := string(cdata)
	return result
}

func cr_cr_sql(constr string,strsql string,tablename string) string{
	result:=""
	str:= strings.Replace(constr, "\\", "", -1)

	db, err := sql.Open("mysql", str)
	
	rows, err := db.Query(strsql)
	if err != nil {
		log.Fatal(err)
	}
	cols, _ := rows.Columns()
	colTypes, _ := rows.ColumnTypes()
	
	
	for i,s := range colTypes {
		
 
	 myString :=""
	 if s.DatabaseTypeName()=="VARCHAR" || s.DatabaseTypeName()=="CHAR" {
		myString="(255)"
	 }
		result=result+ cols[i] +" "  +s.DatabaseTypeName()+""+myString +","
  
	  }
	
	return "create table "+tablename+" ("+result[:len(result)-1]+")"
}

func createtable(filename string, tablename string)  string {

	fp, err := os.Open(filename)
	if err != nil {
		log.Fatalf("无法打开文件 %s\n",filename)
	}
	nr := csv.NewReader(fp)
	wcomma := []rune(",")
	nr.Comma = wcomma[0]

	recode, err := nr.Read()
	if err != nil {
		log.Fatalf("csv读取错误,err=%s\n", err)
	}
	colstr := strings.Join(recode, " varchar(255),\n")+" varchar(255)"
	colstr = strings.Replace(colstr, "\xEF\xBB\xBF", "", -1)



	return "create table "+ tablename +"\n ("+colstr+"\n)"
}

func loadcsv(i uint64,ch_run chan int,constr string,filename string, tablename string) {

	fp, err := os.Open(filename)
	if err != nil {
		log.Fatalf("无法打开文件 %s\n",filename)
	}
	nr := csv.NewReader(fp)
	wcomma := []rune(",")
	nr.Comma = wcomma[0]

	recode, err := nr.Read()
	if err != nil {
		log.Fatalf("csv读取错误,err=%s\n", err)
	}
	colstr := strings.Join(recode, ",")
	colstr = strings.Replace(colstr, "\xEF\xBB\xBF", "", -1)
	valstr := ""
	for _, _ = range recode {
		valstr = valstr + ",?"
	}
	valstr = valstr[1:]

	strsql := "insert  into " + tablename + " (" + colstr + ") values (" + valstr + ")"
	log.Printf("开始对表%s插入数据,导入文件名=%s\n", tablename, filename)

	
	//解决密码中有特殊字符进行转义后去掉转义斜杠
	str:= strings.Replace(constr, "\\", "", -1)

	db, err := sql.Open("mysql", str)
	
	_,err=db.Exec("select 1")
	if err != nil {
		log.Fatalf("打开数据库错误,err=%s\n", err)
	}
	var txbool bool = false
	var tx *sql.Tx
	j := 1
	for {
		rows, err := nr.Read()
		if err != nil {
			goto xxx
		}
		//var aaa []interface{} = rows[:]
		//log.Println("sql=", strsql, rows[:]...)

		var dest []interface{} = make([]interface{}, len(rows))

		for m, v := range rows {

			if v == "NullInt" {
				dest[m] = interface{}(sql.NullInt64{Int64: 0, Valid: false})
			} else if v == "NullString" {
				dest[m] = interface{}(sql.NullString{String: "", Valid: false})
			} else if v == "NULL" {
				dest[m] = interface{}(sql.NullString{String: "", Valid: false})
				} else if v == "" {
					dest[m] = interface{}(sql.NullString{String: "", Valid: false})
				
			} else {
				dest[m] = interface{}(v)
			}

		}

		//log.Println("sql=", strsql, dest)
		if !txbool {

			tx, _ = db.Begin()
		}

		_, err = tx.Exec(strsql, dest...)
		txbool = true
		if err != nil {
			log.Fatalf("提交发生错误：err=%s,dest=%v,strsql=%s",  err, dest,strsql)
		}
		if (j-1)%10000 == 0 && txbool && j != 1 {
			log.Printf("插入%d条数据\n", j-1)
			tx.Commit()
			txbool = false

		}
		j++
	}
xxx:
	if txbool == true {
		tx.Commit()
	}

	log.Printf("文件%s插入%s数据成功,共插入%d条数据\n", filename, tablename, j-1)
	
	

	err = fp.Close()
	if err != nil {
		log.Fatalf("文件关闭错误,err=%s\n", err)
	}
	ch_run<-1
}

func main() {


	var constr = flag.String("constr", "root:root@tcp(localhost:3306)/mysql", "mysql连接串")
	var csvfile = flag.String("f", "pm3.csv", "要导入的csv文件")
	var tablename = flag.String("t", "aaa", "要导入的数据库表名")
	var showVer = flag.Bool("v", false, "显示版本信息")
	var showsql = flag.Bool("sql", true, "生成创建表sql")
	var num=flag.Uint64("num",1,"数据拆分数量")
	

	flag.Parse()
	if *showVer {
		// Printf( "build name:\t%s\nbuild ver:\t%s\nbuild time:\t%s\nCommitID:%s\n", BuildName, BuildVersion, BuildTime, CommitID )
		fmt.Printf("build name:\t%s\n", "tongtech loadmysql")
		fmt.Printf("build ver:\t%s\n", "20210525")

		os.Exit(0)
	}


	if *showsql {
		// Printf( "build name:\t%s\nbuild ver:\t%s\nbuild time:\t%s\nCommitID:%s\n", BuildName, BuildVersion, BuildTime, CommitID )
	//	fmt.Println(createtable(*csvfile,*tablename))
	xx:=cr_cr_sql(*constr,"select * from ca_cm_pm_2g_bsc_h","test")
	fmt.Println(xx)
		os.Exit(0)
	}
	var  i uint64
	var intChan chan int
	intChan = make(chan int,*num)

	//如果加了num参数，将取模后对数据进行拆分，输入文件前面为1_ 2_
	for i=0;i<*num;i++{
        
		if *num==1{
			go loadcsv(i+1,intChan,*constr,*csvfile,*tablename)
		}else{
			go loadcsv(i+1,intChan,*constr,strconv.FormatUint(i+1,10)+"_"+*csvfile,*tablename)
		}
		 
	}

	for i=0;i<*num;i++{
	<-intChan
	}



	
}
