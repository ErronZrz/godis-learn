package config

import (
	"bufio"
	"godis-learn/lib/logger"
	"io"
	"os"
	"reflect"
	"strconv"
	"strings"
)

type ServerProperties struct {
	Bind              string   `cfg:"bind"`
	Port              int      `cfg:"port"`
	AppendOnly        bool     `cfg:"appendonly"`
	AppendFilename    string   `cfg:"appendfilename"`
	MaxClients        int      `cfg:"maxclients"`
	RequirePass       string   `cfg:"requirepass"`
	DatabaseCount     int      `cfg:"databasecount"`
	RDBFilename       string   `cfg:"dbfilename"`
	MasterAuth        string   `cfg:"masterauth"`
	SlaveAnnouncePort int      `cfg:"slave-announce-port"`
	SlaveAnnounceIP   string   `cfg:"slave-announce-ip"`
	ReplTimeout       int      `cfg:"repl-timeout"`
	Peers             []string `cfg:"peers"`
	Self              string   `cfg:"self"`
}

var Properties *ServerProperties

func SetupConfigProperties(filename string) {
	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer func(f *os.File) {
		_ = f.Close()
	}(file)
	Properties = parse(file)
}

func init() {
	Properties = &ServerProperties{
		Bind:       "127.0.0.1",
		Port:       6379,
		AppendOnly: false,
	}
}

func parse(reader io.Reader) *ServerProperties {
	res := &ServerProperties{}
	m := make(map[string]string)
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) > 0 && line[0] == '#' {
			continue
		}
		pivot := strings.IndexAny(line, " ")
		if pivot > 0 && pivot < len(line)-1 {
			key := line[0:pivot]
			val := strings.Trim(line[pivot+1:], " ")
			m[strings.ToLower(key)] = val
		}
	}
	if err := scanner.Err(); err != nil {
		logger.Fatal(err)
	}
	fillProperties(res, m)
	return res
}

func fillProperties(p *ServerProperties, m map[string]string) {
	fields := reflect.TypeOf(p).Elem()
	values := reflect.ValueOf(p).Elem()
	n := fields.NumField()
	for i := 0; i < n; i++ {
		field := fields.Field(i)
		fieldVal := values.Field(i)
		key, ok := field.Tag.Lookup("cfg")
		if !ok {
			key = field.Name
		}
		val, ok := m[strings.ToLower(key)]
		if !ok {
			continue
		}
		switch field.Type.Kind() {
		case reflect.String:
			fieldVal.SetString(val)
		case reflect.Int:
			intV, err := strconv.ParseInt(val, 10, 64)
			if err == nil {
				fieldVal.SetInt(intV)
			}
		case reflect.Bool:
			boolV := "yes" == val
			fieldVal.SetBool(boolV)
		case reflect.Slice:
			if field.Type.Elem().Kind() == reflect.String {
				sliceV := strings.Split(val, ",")
				fieldVal.Set(reflect.ValueOf(sliceV))
			}
		}
	}
}
