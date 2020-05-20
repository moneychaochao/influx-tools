package main

import (
	"encoding/json"
	"fmt"
	_ "github.com/influxdata/influxdb1-client"
	client "github.com/influxdata/influxdb1-client/v2"
	"net/http"
	"strconv"
	"test/influx"
	"time"
)

func main() {
	http.HandleFunc("/hello", handler)
	err := http.ListenAndServe(":12345", nil)
	if err != nil {
		fmt.Println("ListenAndServe: ", err)
	}
}

func handler(w http.ResponseWriter, r *http.Request)  {
	getData := r.URL.Query()

	pageSize , err := strconv.Atoi(getData.Get("pagesize"))
	if err != nil {
		_, _ = w.Write([]byte("page size error: " + err.Error()))
		return
	}

	pageNo , err := strconv.Atoi(getData.Get("pageno"))
	if err != nil {
		_, _ = w.Write([]byte("page no error: " + err.Error()))
		return
	}

	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr: "http://192.168.33.10:8086",
		Timeout: time.Second * 5,
	})

	if err != nil {
		_, _ = w.Write([]byte("connect error: " + err.Error()))
		return
	}

	defer c.Close()

	queryApi := influx.NewQueryApi(c, "test")

	totalNums, err := queryApi.QueryCount("select count(*) from cpu_load_short")

	if err != nil {
		_, _ = w.Write([]byte("query count error: " + err.Error()))
		return
	}

	queryPageApi := influx.NewQueryPageApi(influx.NewPageInfo(uint(pageSize), uint(pageNo), totalNums), queryApi)

	result, err := queryPageApi.QueryPageRows("select * from cpu_load_short")

	if err != nil {
		_, _ = w.Write([]byte("query error: " + err.Error()))
		return
	}

	rJson, err := json.Marshal(result)
	if err != nil {
		_, _ = w.Write([]byte("json error: " + err.Error()))
		return
	}

	_, _ = w.Write(rJson)
}

