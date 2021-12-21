package main

import (
	"encoding/xml"
	"fmt"
	"os"
)

type sqls struct {
	Sqlname string `xml:"sqlname"`
	Sql     string `xml:"sql"`
	Arg     string `xml:"arg"`
	Cm      string `xml:"cm"`
}
type sqlconf struct {
	Sqls []sqls `xml:"sqls"`
}
type Sum struct {
	Desttable   string `xml:"desttable"`
	Dtimecolumn string `xml:"dtimecolumn"`
	Dimcolumn   string `xml:"dimcolumn"`
	Avg         string `xml:"avg"`
	Sum         string `xml:"sum"`
	Max         string `xml:"max"`
	Min         string `xml:"min"`
	Defs        Defs `xml:"defs"`
}

type Def struct {
	Column string `xml:"column"`
	Expr   string `xml:"expr"`
	Dsum string `xml:"dsum"`
}

type Sums struct {
	Sums        []Sum `xml:"sum"`
}
type Defs struct {
	Defs       []Def `xml:"def"`
}
type Sumconf struct {
	Sums        Sums `xml:"sums"`
	Defs        Defs `xml:"defs"`
	Pmtable     string `xml:"pmtable"`
	Pmindex     string `xml:"pmindex"`
	
	Cmtable     string `xml:"cmtable"`
	Cmindex     string `xml:"cmindex"`
	Ptimecolumn string `xml:"ptimecolumn"`
	Joinstr     string `xml:"joinstr"`
	Dimcolumn   string `xml:"dimcolumn"`
	Avg         string `xml:"avg"`
	Sum         string `xml:"sum"`
	Max         string `xml:"max"`
	Min         string `xml:"min"`
}

type Configuration struct {
	Dbip   string `xml:"dbip"`
	Dbport string `xml:"dbport"`
	Dbname string `xml:"dbname"`
	Dbuser string `xml:"dbuser"`
	Dbpwd  string `xml:"dbpwd"`
	Dbmemtable string `xml:"dbmemtable"`
}

func getconf(filename string) (Configuration, error) {
	xmlFile, err := os.Open(filename)
	var conf Configuration
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

func getsum(filename string) (Sumconf, error) {
	xmlFile, err := os.Open(filename)
	var conf Sumconf
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

func getsql(filename string) (sqlconf, error) {
	xmlFile, err := os.Open(filename)
	var conf sqlconf
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

