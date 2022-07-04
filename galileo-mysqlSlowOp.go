package main

import (
	"github.com/hpcloud/tail"
	"math"
    "strconv"
    "encoding/json"
    "fmt"
    "io/ioutil"
	"os"
	"gopkg.in/yaml.v2"
    "net/http"
    "strings"
    "time"
)


type postData struct {
    ConfigId            string       `json:"configId"`
    ExecuteTime         int64        `json:"executeTime"`
    Sql                 string       `json:"sql"`
}

func slowCheck(UrlDBSlow ,mySQLLogPath ,configIdList ,dbNameList string ,slowOpStd int64) {
	var line ,executeTime string
	var executeTimeInt int64
	seek := &tail.SeekInfo{}
	seek.Offset = 0
	seek.Whence = os.SEEK_END
	config := tail.Config{}
	config.Follow = true
	config.Location = seek
	f, err := tail.TailFile(mySQLLogPath, config)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() {
		if info := recover(); info != nil {
			fmt.Println("触发了宕机", info)
		} else {
			fmt.Println("程序正常退出")
		}
	}()

	Loop:
	_, err = os.Stat(mySQLLogPath)
	if err != nil {
		return
	}
	for sql := range f.Lines { 
		line = sql.Text
		var sqlRes string
		if strings.Contains(line, "User@Host") {
			sqlRes = line
			for queryTimeLine := range f.Lines {
		//	queryTimeLine ,_ := rd.ReadBytes('\n')
				queryTimeLineStr := queryTimeLine.Text
				queryTimeLineStr = strings.Replace(queryTimeLineStr, "\n", "" ,-1)
				executeTime = strings.Split(queryTimeLineStr ," ")[2]
				executeTime = strings.Replace(executeTime, "\n", "" ,-1)	
				executeTime = strings.Trim(executeTime ," ")
				executeTimeFloat ,_ := strconv.ParseFloat(executeTime ,64)
				executeTimeFloat = executeTimeFloat * 1000
				executeTimeInt = int64(math.Ceil(executeTimeFloat))						
        	    fmt.Println(executeTimeInt)
				break
			}	

			for sqlLine := range f.Lines {
				sqlLineStr := sqlLine.Text
				sqlLineStr = strings.Replace(sqlLineStr, "\n", "" ,-1)
				sqlLineStr = strings.Trim(sqlLineStr ," ")
				if strings.Contains(sqlLineStr, "#") {
					fmt.Println("判断是否上报")
					fmt.Println(sqlRes)
					i := 0
					for _, dbNameStr := range strings.Split(dbNameList, ",") {
						configIdStr := strings.Split(configIdList ,",")[i]
						i++
						fmt.Println(sqlRes ,dbNameStr)
		/*				fmt.Println(i)
		*/				if strings.Contains(sqlRes ,dbNameStr) {
							sqlRes = strings.Trim(sqlRes ," ")
							mysqlSlowUpload := postData{ConfigId: string(configIdStr), ExecuteTime: executeTimeInt, Sql: sqlRes}
							if executeTimeInt > slowOpStd {
								dataJson, _ := json.Marshal(mysqlSlowUpload)
								dataJsonStr := string(dataJson)
								fmt.Println(dataJsonStr)
								Post(UrlDBSlow ,dataJsonStr)
							}
						} else {
							goto Loop
						}
						goto Loop
					}
				} else {
					if len(strings.Trim(sqlLineStr ," ")) != 0 {
/*						fmt.Println("拼接字符")
*/						sqlRes = sqlRes + " " + sqlLineStr
/*						fmt.Println(sqlRes)
*/					}
				}
			} 
		} else {
/*			fmt.Println("下一次循环")
*/			continue
		}
	}
}


func heartBeat(urlDBSlow ,configId ,mySQLLogPath string) {
	var line string
	var executeTimeInt int64 = 0
	for {
		f ,err := os.Open(mySQLLogPath)
		if err != nil {
			continue
		}
		time.Sleep(900 * time.Second)
		fmt.Println(time.Now().String())
		for _, configIdStr := range strings.Split(configId, ",") {
			data := postData{ConfigId: string(configIdStr), ExecuteTime: executeTimeInt, Sql: line}
			dataJson, _ := json.Marshal(data)
			dataJsonStr := string(dataJson)
			fmt.Println(dataJsonStr)
			postcount := 0
			for {
				resourceInfoRes, statusCode := Post(urlDBSlow ,dataJsonStr)
				if (statusCode != 200) && postcount < 3 || (len(string(statusCode)) == 0) {
					postcount++
					continue
				} else {
					break
				}
				fmt.Println(resourceInfoRes ,statusCode)
			}
		}
		defer func() {
			if info := recover(); info != nil {
				fmt.Println("触发了宕机", info)
			} else {
				fmt.Println("程序正常退出")
			}
		}()
		defer f.Close()
	}
}


type conf struct {
	UrlDBSlow           string `yaml:"urlDBSlow"`
	ConfigId            string `yaml:"configId"`
	MySQLLogPath        string `yaml:"mySQLLogPath"`
    DbNameList          string `yaml:"dbNameList"`
	SlowOpStd           int64 `yaml:"slowOpStd"`
}


func (c *conf) getYaml() *conf {
	yamlFile ,err := ioutil.ReadFile("mysqlSlowOp.yaml")
	if err != nil {
		fmt.Println("yamlFile.Get err", err.Error())
	}
	err = yaml.Unmarshal(yamlFile, c)
	if err != nil {
		fmt.Println("Unmarshal: ", err.Error())
	}
	return c
}

func fileMonitoring(urlDBSlow ,mySQLLogPath ,configIdList ,dbNameList string , slowOpStd int64, f func(string ,string ,string ,string ,int64)) {
	for {
		f(urlDBSlow ,mySQLLogPath ,configIdList ,dbNameList ,slowOpStd)
		time.Sleep(1 * time.Second)
	}
}

func main() {
	var yamlConfig conf
	yamlconf := yamlConfig.getYaml()
	urlDBSlow := yamlconf.UrlDBSlow
	configIdList := yamlconf.ConfigId
	dbNameList := yamlconf.DbNameList
	mySQLLogPath := yamlconf.MySQLLogPath
	slowOpStd := yamlconf.SlowOpStd
	fmt.Println("slow log check start!!")
	go fileMonitoring(urlDBSlow ,mySQLLogPath ,configIdList ,dbNameList ,slowOpStd ,slowCheck)
	go heartBeat(urlDBSlow ,configIdList ,mySQLLogPath)
	select{}
}


func Post(url string, data string) (string ,int) {
	jsoninfo := strings.NewReader(data)
	client := &http.Client{}
	req, err := http.NewRequest("POST", url, jsoninfo)
	if err != nil {
		 fmt.Println(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("token", "xxx")
	
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println(err)
	}
	
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
			return
	}
		fmt.Println("Process panic done Post")
	}()
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(string(body))
	fmt.Println(resp.StatusCode)
	return string(body) ,resp.StatusCode
}
