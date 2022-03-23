package file

import (
	"github.com/bmizerany/assert"
	"github.com/chararch/gobatch/util"
	"log"
	"reflect"
	"testing"
	"time"
)

type Outer struct {
	Id     int       `order:"1" header:"id"`
	Type   string    `order:"2" header:"type"`
	Amount uint64    `order:"3" header:"amount"`
	Status *string   `order:"4" header:"status"`
	Valid  bool      `order:"12" header:"valid" format:"YES"`
	CDate  time.Time `order:"11" header:"cdate" format:"20060102"`
	S1     Innner1
	S2     *Innner2
	S3     **Innner3
}

type Innner1 struct {
	I1 int      `order:"5" header:"i1"`
	F2 *float64 `order:"6" header:"f2"`
}

type Innner2 struct {
	F1 float64    `order:"7" header:"f1"`
	T2 *time.Time `order:"8" header:"t2"`
}

type Innner3 struct {
	S1 *string `order:"9" header:"s1" default:"aaa"`
	I2 int     `order:"10"`
}

func TestUnmarshalByOrder(t *testing.T) {
	fields := []string{"", "111", "T", "100", "init", "5", "0.2", "0.5", "2021-12-02 22:10:10", "abc", "10", "20211202", "Y"}
	//r, err := xsvUnmarshalByOrder(fields, reflect.TypeOf(Outer{}))
	r, err := xsvUnmarshal(fields, slice2Map(fields), reflect.TypeOf(Outer{}))
	if err != nil {
		panic(err)
	}
	o := r.Interface().(*Outer)
	assert.Equal(t, o.Id, 111)
	assert.Equal(t, o.Type, "T")
	assert.Equal(t, o.Amount, uint64(100))
	assert.Equal(t, *o.Status, "init")
	assert.Equal(t, o.Valid, true)
	assert.Equal(t, o.CDate, time.Date(2021, 12, 2, 0, 0, 0, 0, time.Local))
	assert.Equal(t, o.S1.I1, 5)
	assert.Equal(t, *o.S1.F2, 0.2)
	assert.Equal(t, o.S2.F1, 0.5)
	assert.Equal(t, *o.S2.T2, time.Date(2021, 12, 2, 22, 10, 10, 0, time.Local))
	assert.Equal(t, *(*o.S3).S1, "abc")
	assert.Equal(t, (*o.S3).I2, 10)
	json, err := util.JsonString(o)
	if err != nil {
		panic(err)
	}
	log.Printf("%v\n", json)
}

func TestUnmarshalByHeader(t *testing.T) {
	headers := []string{"id", "type", "amount", "status", "i1", "f2", "f1", "t2", "s1", "i2", "cdate", "valid"}
	fields := []string{"111", "T", "100", "init", "5", "0.2", "0.5", "2021-12-02 22:10:10", "abc", "10", "20211202", "Y"}
	r, err := xsvUnmarshalByHeader(fields, slice2Map(headers), reflect.TypeOf(Outer{}))
	if err != nil {
		panic(err)
	}
	o := r.Interface().(*Outer)
	assert.Equal(t, o.Id, 111)
	assert.Equal(t, o.Type, "T")
	assert.Equal(t, o.Amount, uint64(100))
	assert.Equal(t, *o.Status, "init")
	assert.Equal(t, o.Valid, true)
	assert.Equal(t, o.CDate, time.Date(2021, 12, 2, 0, 0, 0, 0, time.Local))
	assert.Equal(t, o.S1.I1, 5)
	assert.Equal(t, *o.S1.F2, 0.2)
	assert.Equal(t, o.S2.F1, 0.5)
	assert.Equal(t, *o.S2.T2, time.Date(2021, 12, 2, 22, 10, 10, 0, time.Local))
	assert.Equal(t, *(*o.S3).S1, "abc")
	assert.Equal(t, (*o.S3).I2, 0)
	json, err := util.JsonString(o)
	if err != nil {
		panic(err)
	}
	log.Printf("%v\n", json)
}

func TestMarshalByOrder(t *testing.T) {
	s := "init"
	f := 3.14
	tm := time.Date(2021, 12, 2, 0, 0, 0, 0, time.Local)
	outer := &Outer{
		Id:     123,
		Type:   "simple",
		Amount: 1000,
		Status: &s,
		Valid:  true,
		CDate:  tm,
		S1: Innner1{
			I1: 10,
			F2: &f,
		},
		S2: &Innner2{
			F1: 1.58,
			T2: &tm,
		},
		S3: nil,
	}
	meta, _ := getMetadata(reflect.TypeOf(outer).Elem())
	fields, err := xsvMarshalByOrder(outer, meta)
	if err != nil {
		log.Printf("err:%v\n", err)
		panic(err)
	}
	assert.Equal(t, len(fields), 13)
	assert.Equal(t, fields[0], "")
	assert.Equal(t, fields[1], "123")
	assert.Equal(t, fields[2], "simple")
	assert.Equal(t, fields[3], "1000")
	assert.Equal(t, fields[4], "init")
	assert.Equal(t, fields[5], "10")
	assert.Equal(t, fields[6], "3.14")
	assert.Equal(t, fields[7], "1.58")
	assert.Equal(t, fields[8], "2021-12-02")
	assert.Equal(t, fields[9], "aaa")
	assert.Equal(t, fields[10], "0")
	assert.Equal(t, fields[11], "20211202")
	assert.Equal(t, fields[12], "YES")
	log.Printf("%v\n", fields)
}

func TestMarshalByHeader(t *testing.T) {
	s := "init"
	f := 3.14
	tm := time.Date(2021, 12, 2, 0, 0, 0, 0, time.Local)
	outer := &Outer{
		Id:     123,
		Type:   "simple",
		Amount: 1000,
		Status: &s,
		Valid:  true,
		CDate:  tm,
		S1: Innner1{
			I1: 10,
			F2: &f,
		},
		S2: &Innner2{
			F1: 1.58,
			T2: &tm,
		},
		S3: nil,
	}
	meta, _ := getMetadata(reflect.TypeOf(outer).Elem())
	fields, err := xsvMarshalByHeader(outer, meta)
	if err != nil {
		log.Printf("err:%v\n", err)
		panic(err)
	}
	assert.Equal(t, len(fields), 13)
	assert.Equal(t, fields[0], "")
	assert.Equal(t, fields[1], "123")
	assert.Equal(t, fields[2], "simple")
	assert.Equal(t, fields[3], "1000")
	assert.Equal(t, fields[4], "init")
	assert.Equal(t, fields[5], "10")
	assert.Equal(t, fields[6], "3.14")
	assert.Equal(t, fields[7], "1.58")
	assert.Equal(t, fields[8], "2021-12-02")
	assert.Equal(t, fields[9], "aaa")
	assert.Equal(t, fields[10], "0")
	assert.Equal(t, fields[11], "20211202")
	assert.Equal(t, fields[12], "YES")
	log.Printf("header:%v\n", meta.structFields)
	log.Printf("values:%v\n", fields)
}
