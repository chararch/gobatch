package file

import (
	"fmt"
	"github.com/pkg/errors"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
)

type xsvMetadata struct {
	structType              reflect.Type
	defaultByOrder          map[int]string
	structFields            []string
	structFieldMap          map[string]int
	structUnOrderedFields   []string
	structUnOrderedFieldMap map[string]int
	structOrderedFields     []string
	structOrderedFieldMap   map[string]int
	defaultByHeader         map[string]string
	fileHeaders             []string
	fileHeaderMap           map[string]int
}

func slice2Map(s []string) map[string]int {
	m := make(map[string]int)
	for i, v := range s {
		m[v] = i
	}
	return m
}

func getMetadata(tp reflect.Type) (*xsvMetadata, error) {
	meta := &xsvMetadata{
		structType:     tp,
		defaultByOrder: make(map[int]string),
		//structFields:            make([]string, 0),
		//structFieldMap:          make(map[string]int),
		structUnOrderedFields:   make([]string, 0),
		structUnOrderedFieldMap: make(map[string]int),
		structOrderedFields:     make([]string, 0),
		structOrderedFieldMap:   make(map[string]int),
		defaultByHeader:         make(map[string]string),
	}

	for i := 0; i < tp.NumField(); i++ {
		tf := tp.Field(i)
		if tf.Tag != "" {
			if tf.Tag.Get("order") != "" {
				ord := tf.Tag.Get("order")
				idx, err := strconv.Atoi(ord)
				if err != nil || idx < 0 {
					return nil, errors.New("invalid order value in tag")
				}
				if _, ok := meta.defaultByOrder[idx]; !ok {
					if tf.Tag.Get("default") != "" {
						meta.defaultByOrder[idx] = tf.Tag.Get("default")
					} else {
						val := reflect.New(tf.Type).Elem()
						meta.defaultByOrder[idx], _ = formatVal(val, tf.Type, tf.Tag)
					}
				} else {
					return nil, errors.Errorf("can not serialize struct with duplicate order:%v", idx)
				}
				header := tf.Name
				if tf.Tag.Get("header") != "" {
					header = tf.Tag.Get("header")
				}
				meta.structOrderedFieldMap[header] = idx
				meta.structOrderedFields = append(meta.structOrderedFields, header)
				if _, ok := meta.defaultByHeader[header]; !ok {
					if tf.Tag.Get("default") != "" {
						meta.defaultByHeader[header] = tf.Tag.Get("default")
					} else {
						val := reflect.New(tf.Type).Elem()
						meta.defaultByHeader[header], _ = formatVal(val, tf.Type, tf.Tag)
					}
				} else {
					return nil, errors.Errorf("can not serialize struct with duplicate header:%v", idx)
				}
			} else if tf.Tag.Get("header") != "" {
				header := tf.Tag.Get("header")
				if _, ok := meta.structUnOrderedFieldMap[header]; !ok {
					meta.structUnOrderedFieldMap[header] = len(meta.structUnOrderedFields)
					meta.structUnOrderedFields = append(meta.structUnOrderedFields, header)

					if tf.Tag.Get("default") != "" {
						meta.defaultByHeader[header] = tf.Tag.Get("default")
					} else {
						val := reflect.New(tf.Type).Elem()
						meta.defaultByHeader[header], _ = formatVal(val, tf.Type, tf.Tag)
					}
				} else {
					return nil, errors.Errorf("can not serialize struct with duplicate header:%v", header)
				}
			}
		} else if tf.Type.Kind() == reflect.Struct {
			meta2, err := getMetadata(tf.Type)
			if err != nil {
				return nil, err
			}
			for idx, val := range meta2.defaultByOrder {
				if _, ok := meta.defaultByOrder[idx]; !ok {
					meta.defaultByOrder[idx] = val
				} else {
					return nil, errors.Errorf("can not serialize struct with duplicate order:%v", idx)
				}
			}
			for header, val := range meta2.defaultByHeader {
				if _, ok := meta.defaultByHeader[header]; !ok {
					meta.defaultByHeader[header] = val
				} else {
					return nil, errors.Errorf("can not serialize struct with duplicate header:%v", header)
				}
			}
			for header, idx := range meta2.structOrderedFieldMap {
				if _, ok := meta.structOrderedFieldMap[header]; !ok {
					meta.structOrderedFieldMap[header] = idx
					meta.structOrderedFields = append(meta.structOrderedFields, header)
				} else {
					return nil, errors.Errorf("can not serialize struct with duplicate header:%v", header)
				}
			}
			for header := range meta2.structUnOrderedFieldMap {
				if _, ok := meta.structUnOrderedFieldMap[header]; !ok {
					meta.structUnOrderedFieldMap[header] = len(meta.structUnOrderedFields)
					meta.structUnOrderedFields = append(meta.structUnOrderedFields, header)
				} else {
					return nil, errors.Errorf("can not serialize struct with duplicate header:%v", header)
				}
			}
		} else if tf.Type.Kind() == reflect.Ptr {
			tt := tf.Type.Elem()
			for tt.Kind() == reflect.Ptr {
				tt = tt.Elem()
			}
			if tt.Kind() == reflect.Struct && tt.String() != "time.Time" {
				meta2, err := getMetadata(tt)
				if err != nil {
					return nil, err
				}
				for idx, val := range meta2.defaultByOrder {
					if _, ok := meta.defaultByOrder[idx]; !ok {
						meta.defaultByOrder[idx] = val
					} else {
						return nil, errors.Errorf("can not serialize struct with duplicate order:%v", idx)
					}
				}
				for header, val := range meta2.defaultByHeader {
					if _, ok := meta.defaultByHeader[header]; !ok {
						meta.defaultByHeader[header] = val
					} else {
						return nil, errors.Errorf("can not serialize struct with duplicate header:%v", header)
					}
				}
				for header, idx := range meta2.structOrderedFieldMap {
					if _, ok := meta.structOrderedFieldMap[header]; !ok {
						meta.structOrderedFieldMap[header] = idx
						meta.structOrderedFields = append(meta.structOrderedFields, header)
					} else {
						return nil, errors.Errorf("can not serialize struct with duplicate header:%v", header)
					}
				}
				for header := range meta2.structUnOrderedFieldMap {
					if _, ok := meta.structUnOrderedFieldMap[header]; !ok {
						meta.structUnOrderedFieldMap[header] = len(meta.structUnOrderedFields)
						meta.structUnOrderedFields = append(meta.structUnOrderedFields, header)
					} else {
						return nil, errors.Errorf("can not serialize struct with duplicate header:%v", header)
					}
				}
			}
		}
	}

	meta.structFields = make([]string, len(meta.structUnOrderedFields))
	meta.structFieldMap = make(map[string]int)
	copy(meta.structFields, meta.structUnOrderedFields)
	for header, idx := range meta.structUnOrderedFieldMap {
		meta.structFieldMap[header] = idx
	}
	fieldCount := len(meta.structFields)

	if len(meta.structOrderedFields) > 0 {
		sort.Slice(meta.structOrderedFields, func(i, j int) bool {
			fieldi := meta.structOrderedFields[i]
			fieldj := meta.structOrderedFields[j]
			return meta.structOrderedFieldMap[fieldi] < meta.structOrderedFieldMap[fieldj]
		})
		for _, field := range meta.structOrderedFields {
			idx := meta.structOrderedFieldMap[field]
			if idx >= fieldCount {
				for i := 0; i < idx-fieldCount; i++ {
					meta.structFields = append(meta.structFields, "-")
				}
				meta.structFields = append(meta.structFields, field)
			} else {
				meta.structFields = append(meta.structFields, "")
				for i := fieldCount; i > idx; i-- {
					meta.structFields[i] = meta.structFields[i-1]
				}
				meta.structFields[idx] = field
			}
			meta.structFieldMap[field] = idx
			fieldCount = len(meta.structFields)
		}
	}

	return meta, nil
}

func xsvUnmarshal(fields []string, headerIndexMap map[string]int, tp reflect.Type) (reflect.Value, error) {
	val := reflect.New(tp)
	for i := 0; i < tp.NumField(); i++ {
		tf := tp.Field(i)
		vf := val.Elem().Field(i)
		if tf.Tag != "" && (tf.Tag.Get("order") != "" || tf.Tag.Get("header") != "") {
			if !vf.CanSet() {
				continue
			}
			fieldVal := ""
			if tf.Tag.Get("order") != "" {
				ord := tf.Tag.Get("order")
				idx, err := strconv.Atoi(ord)
				if err != nil || idx < 0 {
					return reflect.Value{}, errors.New("invalid order value in tag")
				}
				if idx >= len(fields) {
					return reflect.Value{}, errors.Errorf("order of %v is out of bound, order:%v, fieldCount:%v", tf.Name, idx, len(fields))
				}
				fieldVal = fields[idx]
			} else {
				header := tf.Tag.Get("header")
				idx := headerIndexMap[header]
				fieldVal = fields[idx]
			}
			fieldVal = strings.TrimSpace(fieldVal)
			if fieldVal == "" {
				continue
			}
			if vf.Kind() == reflect.Struct && tf.Type.String() == "time.Time" {
				tm, err := parseDate(fieldVal, tf.Tag.Get("format"))
				if err != nil {
					return reflect.Value{}, err
				}
				vf.Set(reflect.ValueOf(tm))
			} else {
				_, err := setValue(fieldVal, vf.Addr(), tf.Type, tf.Tag)
				if err != nil {
					return reflect.Value{}, err
				}
			}
		} else if tf.Type.Kind() == reflect.Struct {
			if !vf.CanSet() {
				continue
			}
			v, err := xsvUnmarshal(fields, headerIndexMap, tf.Type)
			if err != nil {
				return reflect.Value{}, err
			}
			vf.Set(v.Elem())
		} else if tf.Type.Kind() == reflect.Ptr {
			tt := tf.Type
			for {
				if tt.Elem().Kind() != reflect.Ptr {
					break
				}
				tt = tt.Elem()
			}
			if tt.Elem().Kind() == reflect.Struct {
				v, err := xsvUnmarshal(fields, headerIndexMap, tt.Elem())
				if err != nil {
					return reflect.Value{}, err
				}
				t1 := tf.Type
				v1 := vf
				for {
					if t1.Elem().Kind() == reflect.Ptr {
						if !v1.CanSet() {
							continue
						}
						v1.Set(reflect.New(t1.Elem()))
						v1 = v1.Elem()
						t1 = t1.Elem()
					} else {
						break
					}
				}
				v1.Set(v)
			}
		}
	}
	return val, nil
}

func xsvUnmarshalByOrder(fields []string, tp reflect.Type) (reflect.Value, error) {
	val := reflect.New(tp)
	for i := 0; i < tp.NumField(); i++ {
		tf := tp.Field(i)
		vf := val.Elem().Field(i)
		if tf.Tag != "" && tf.Tag.Get("order") != "" {
			ord := tf.Tag.Get("order")
			idx, err := strconv.Atoi(ord)
			if err != nil || idx < 0 {
				return reflect.Value{}, errors.New("invalid order value in tag")
			}
			if idx >= len(fields) {
				return reflect.Value{}, errors.Errorf("order of %v is out of bound, order:%v, max:%v", tf.Name, idx, len(fields))
			}
			fieldVal := fields[idx]
			if !vf.CanSet() {
				continue
			}
			fieldVal = strings.TrimSpace(fieldVal)
			if fieldVal == "" {
				continue
			}
			if vf.Kind() == reflect.Struct && tf.Type.String() == "time.Time" {
				tm, err := parseDate(fieldVal, tf.Tag.Get("format"))
				if err != nil {
					return reflect.Value{}, err
				}
				vf.Set(reflect.ValueOf(tm))
			} else {
				_, err = setValue(fieldVal, vf.Addr(), tf.Type, tf.Tag)
				if err != nil {
					return reflect.Value{}, err
				}
			}
		} else if tf.Type.Kind() == reflect.Struct {
			if !vf.CanSet() {
				continue
			}
			v, err := xsvUnmarshalByOrder(fields, tf.Type)
			if err != nil {
				return reflect.Value{}, err
			}
			vf.Set(v.Elem())
		} else if tf.Type.Kind() == reflect.Ptr {
			tt := tf.Type
			for {
				if tt.Elem().Kind() != reflect.Ptr {
					break
				}
				tt = tt.Elem()
			}
			if tt.Elem().Kind() == reflect.Struct {
				v, err := xsvUnmarshalByOrder(fields, tt.Elem())
				if err != nil {
					return reflect.Value{}, err
				}
				t1 := tf.Type
				v1 := vf
				for {
					if t1.Elem().Kind() == reflect.Ptr {
						if !v1.CanSet() {
							break
						}
						v1.Set(reflect.New(t1.Elem()))
						v1 = v1.Elem()
						t1 = t1.Elem()
					} else {
						break
					}
				}
				if !v1.CanSet() {
					break
				}
				v1.Set(v)
			}
		}
	}
	return val, nil
}

func xsvUnmarshalByHeader(fields []string, headerIndexMap map[string]int, tp reflect.Type) (reflect.Value, error) {
	val := reflect.New(tp)
	for i := 0; i < tp.NumField(); i++ {
		tf := tp.Field(i)
		vf := val.Elem().Field(i)
		if tf.Tag != "" && tf.Tag.Get("header") != "" {
			header := tf.Tag.Get("header")
			idx := headerIndexMap[header]
			fieldVal := fields[idx]
			if !vf.CanSet() {
				continue
			}
			fieldVal = strings.TrimSpace(fieldVal)
			if fieldVal == "" {
				continue
			}
			if vf.Kind() == reflect.Struct && tf.Type.String() == "time.Time" {
				tm, err := parseDate(fieldVal, tf.Tag.Get("format"))
				if err != nil {
					return reflect.Value{}, err
				}
				vf.Set(reflect.ValueOf(tm))
			} else {
				_, err := setValue(fieldVal, vf.Addr(), tf.Type, tf.Tag)
				if err != nil {
					return reflect.Value{}, err
				}
			}
		} else if tf.Type.Kind() == reflect.Struct {
			if !vf.CanSet() {
				continue
			}
			v, err := xsvUnmarshalByHeader(fields, headerIndexMap, tf.Type)
			if err != nil {
				return reflect.Value{}, err
			}
			vf.Set(v.Elem())
		} else if tf.Type.Kind() == reflect.Ptr {
			tt := tf.Type
			for {
				if tt.Elem().Kind() != reflect.Ptr {
					break
				}
				tt = tt.Elem()
			}
			if tt.Elem().Kind() == reflect.Struct {
				v, err := xsvUnmarshalByHeader(fields, headerIndexMap, tt.Elem())
				if err != nil {
					return reflect.Value{}, err
				}
				t1 := tf.Type
				v1 := vf
				for {
					if t1.Elem().Kind() == reflect.Ptr {
						if !v1.CanSet() {
							continue
						}
						v1.Set(reflect.New(t1.Elem()))
						v1 = v1.Elem()
						t1 = t1.Elem()
					} else {
						break
					}
				}
				v1.Set(v)
			}
		}
	}
	return val, nil
}

func setValue(fieldVal string, addr reflect.Value, tp reflect.Type, tag reflect.StructTag) (reflect.Value, error) {
	switch tp.Kind() {
	case reflect.String:
		addr.Elem().SetString(fieldVal)

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v, err := strconv.ParseInt(fieldVal, 10, 64)
		if err != nil {
			return reflect.Value{}, err
		}
		addr.Elem().SetInt(v)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v, err := strconv.ParseUint(fieldVal, 10, 64)
		if err != nil {
			return reflect.Value{}, err
		}
		addr.Elem().SetUint(v)
	case reflect.Float32, reflect.Float64:
		v, err := strconv.ParseFloat(fieldVal, 64)
		if err != nil {
			return reflect.Value{}, err
		}
		addr.Elem().SetFloat(v)
	case reflect.Bool:
		v, err := strconv.ParseBool(fieldVal)
		if err != nil {
			if fieldVal == "Y" || fieldVal == "y" || fieldVal == "Yes" || fieldVal == "YES" {
				v = true
			} else if fieldVal == "N" || fieldVal == "n" || fieldVal == "No" || fieldVal == "NO" {
				v = false
			} else {
				return reflect.Value{}, err
			}
		}
		addr.Elem().SetBool(v)
	case reflect.Ptr:
		te := tp.Elem()
		v := reflect.New(te)
		_, err := setValue(fieldVal, v, te, tag)
		if err != nil {
			return reflect.Value{}, err
		}
		addr.Elem().Set(v)
	case reflect.Struct:
		if tp.String() == "time.Time" {
			tm, err := parseDate(fieldVal, tag.Get("format"))
			if err != nil {
				return reflect.Value{}, err
			}
			addr.Elem().Set(reflect.ValueOf(tm))
		} else {
			//not support
		}
	}
	return addr, nil
}

func parseDate(fieldVal string, fmt string) (time.Time, error) {
	if fmt != "" {
		return time.ParseInLocation(fmt, fieldVal, time.Local)
	}
	if len(fieldVal) == 10 {
		return time.ParseInLocation("2006-01-02", fieldVal, time.Local)
	} else if len(fieldVal) == 19 {
		return time.ParseInLocation("2006-01-02 15:04:05", fieldVal, time.Local)
	} else {
		return time.Time{}, errors.New("unrecognized date format")
	}
}

func xsvMarshal(item interface{}, meta *xsvMetadata) ([]string, error) {
	refItem := reflect.ValueOf(item)
	for refItem.Type().Kind() == reflect.Ptr && !refItem.IsZero() {
		refItem = refItem.Elem()
	}
	valueMap, valueMapByHeader, err := serializeStruct(refItem, meta.structType)
	if err != nil {
		return nil, err
	}
	fieldCount := len(meta.structFields)
	fields := make([]string, fieldCount, fieldCount)
	for i := 0; i < fieldCount; i++ {
		field := meta.structFields[i]
		if val, ok := valueMap[i]; ok {
			fields[i] = val
		} else if val2, ok2 := valueMapByHeader[field]; ok2 {
			fields[i] = val2
		} else if val3, ok3 := meta.defaultByOrder[i]; ok3 {
			fields[i] = val3
		} else {
			fields[i] = meta.defaultByHeader[field]
		}
	}
	return fields, nil
}

func xsvMarshalByOrder(item interface{}, meta *xsvMetadata) ([]string, error) {
	refItem := reflect.ValueOf(item)
	for refItem.Type().Kind() == reflect.Ptr && !refItem.IsZero() {
		refItem = refItem.Elem()
	}
	valueMap, _, err := serializeStruct(refItem, meta.structType)
	if err != nil {
		return nil, err
	}
	fieldCount := len(meta.structFields)
	fields := make([]string, fieldCount, fieldCount)
	for i := 0; i < fieldCount; i++ {
		if val, ok := valueMap[i]; ok {
			fields[i] = val
		} else {
			fields[i] = meta.defaultByOrder[i]
		}
	}
	return fields, nil
}

func serializeStruct(item reflect.Value, tp reflect.Type) (map[int]string, map[string]string, error) {
	fieldValueMap := make(map[int]string)
	fieldValueByHeader := make(map[string]string)
	if item.IsZero() {
		return fieldValueMap, fieldValueByHeader, nil
	}
	for i := 0; i < tp.NumField(); i++ {
		tf := tp.Field(i)
		vf := item.Field(i)
		if tf.Tag != "" && (tf.Tag.Get("order") != "" || tf.Tag.Get("header") != "") {
			val, err := formatField(tf, vf)
			if err != nil {
				return nil, nil, err
			}
			if tf.Tag.Get("order") != "" {
				ord := tf.Tag.Get("order")
				idx, err := strconv.Atoi(ord)
				if err != nil || idx < 0 {
					return nil, nil, errors.New("invalid order value in tag")
				}
				if _, ok := fieldValueMap[idx]; !ok {
					fieldValueMap[idx] = val
				} else {
					return nil, nil, errors.Errorf("can not serialize struct with duplicate order:%v", idx)
				}
			}
			if tf.Tag.Get("header") != "" {
				header := tf.Tag.Get("header")
				if _, ok := fieldValueByHeader[header]; !ok {
					fieldValueByHeader[header] = val
				} else {
					return nil, nil, errors.Errorf("can not serialize struct with duplicate header:%v", header)
				}
			}
		} else if tf.Type.Kind() == reflect.Struct {
			fieldFieldValueMap, fieldFieldValueByHeader, err := serializeStruct(vf, tf.Type)
			if err != nil {
				return nil, nil, err
			}
			for idx, val := range fieldFieldValueMap {
				if _, ok := fieldValueMap[idx]; !ok {
					fieldValueMap[idx] = val
				} else {
					return nil, nil, errors.Errorf("can not serialize struct with duplicate order:%v", idx)
				}
			}
			for header, val := range fieldFieldValueByHeader {
				if _, ok := fieldValueByHeader[header]; !ok {
					fieldValueByHeader[header] = val
				} else {
					return nil, nil, errors.Errorf("can not serialize struct with duplicate header:%v", header)
				}
			}
		} else if tf.Type.Kind() == reflect.Ptr && !vf.IsZero() {
			tt := tf.Type.Elem()
			vv := vf.Elem()
			for tt.Kind() == reflect.Ptr && !vv.IsZero() {
				tt = tt.Elem()
				vv = vv.Elem()
			}
			if tt.Kind() == reflect.Struct && tt.String() != "time.Time" && !vv.IsZero() {
				fieldFieldValueMap, fieldFieldValueByHeader, err := serializeStruct(vv, tt)
				if err != nil {
					return nil, nil, err
				}
				for idx, val := range fieldFieldValueMap {
					if _, ok := fieldValueMap[idx]; !ok {
						fieldValueMap[idx] = val
					} else {
						return nil, nil, errors.Errorf("can not serialize struct with duplicate order:%v", idx)
					}
				}
				for header, val := range fieldFieldValueByHeader {
					if _, ok := fieldValueByHeader[header]; !ok {
						fieldValueByHeader[header] = val
					} else {
						return nil, nil, errors.Errorf("can not serialize struct with duplicate header:%v", header)
					}
				}
			}
		}
	}
	return fieldValueMap, fieldValueByHeader, nil
}

func xsvMarshalByHeader(item interface{}, meta *xsvMetadata) ([]string, error) {
	refItem := reflect.ValueOf(item)
	for refItem.Type().Kind() == reflect.Ptr && !refItem.IsZero() {
		refItem = refItem.Elem()
	}
	valueMap, err := serializeStructByHeader(refItem, meta.structType)
	if err != nil {
		return nil, err
	}
	fields := make([]string, len(meta.structFields), len(meta.structFields))
	for header, idx := range meta.structFieldMap {
		if val, ok := valueMap[header]; ok {
			fields[idx] = val
		} else {
			fields[idx] = meta.defaultByHeader[header]
		}
	}
	return fields, nil
}

func serializeStructByHeader(item reflect.Value, tp reflect.Type) (map[string]string, error) {
	fieldValueMap := make(map[string]string)
	if item.IsZero() {
		return fieldValueMap, nil
	}
	for i := 0; i < tp.NumField(); i++ {
		tf := tp.Field(i)
		vf := item.Field(i)
		if tf.Tag != "" && tf.Tag.Get("header") != "" {
			val, err := formatField(tf, vf)
			if err != nil {
				return nil, err
			}
			header := tf.Tag.Get("header")
			if _, ok := fieldValueMap[header]; !ok {
				fieldValueMap[header] = val
			} else {
				return nil, errors.Errorf("can not serialize struct with duplicate header:%v", header)
			}
		} else if tf.Type.Kind() == reflect.Struct {
			fieldFieldValueMap, err := serializeStructByHeader(vf, tf.Type)
			if err != nil {
				return nil, err
			}
			for header, val := range fieldFieldValueMap {
				if _, ok := fieldValueMap[header]; !ok {
					fieldValueMap[header] = val
				} else {
					return nil, errors.Errorf("can not serialize struct with duplicate header:%v", header)
				}
			}
		} else if tf.Type.Kind() == reflect.Ptr && !vf.IsZero() {
			tt := tf.Type.Elem()
			vv := vf.Elem()
			for tt.Kind() == reflect.Ptr && !vv.IsZero() {
				tt = tt.Elem()
				vv = vv.Elem()
			}
			if tt.Kind() == reflect.Struct && tt.String() != "time.Time" && !vv.IsZero() {
				fieldFieldValueMap, err := serializeStructByHeader(vv, tt)
				if err != nil {
					return nil, err
				}
				for header, val := range fieldFieldValueMap {
					if _, ok := fieldValueMap[header]; !ok {
						fieldValueMap[header] = val
					} else {
						return nil, errors.Errorf("can not serialize struct with duplicate header:%v", header)
					}
				}
			}
		}
	}
	return fieldValueMap, nil
}

func formatField(tf reflect.StructField, vf reflect.Value) (string, error) {
	val, err := formatVal(vf, tf.Type, tf.Tag)
	if err != nil {
		return "", errors.Errorf("format field:%v err:%v", tf.Name, err)
	}
	return val, nil
}

func formatVal(vf reflect.Value, tf reflect.Type, tag reflect.StructTag) (string, error) {
	if vf.IsZero() {
		if tag.Get("default") != "" {
			return tag.Get("default"), nil
		} else if vf.Kind() == reflect.Ptr {
			vf = reflect.New(tf).Elem()
		}
	}
	switch tf.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64, reflect.String:
		if tag.Get("format") != "" {
			return fmt.Sprintf(tag.Get("format"), vf.Interface()), nil
		}
		return fmt.Sprintf("%v", vf.Interface()), nil
	case reflect.Bool:
		fmt := tag.Get("format")
		if fmt == "Y" {
			if vf.Bool() {
				return "Y", nil
			}
			return "N", nil
		} else if fmt == "y" {
			if vf.Bool() {
				return "y", nil
			}
			return "n", nil
		} else if fmt == "Yes" {
			if vf.Bool() {
				return "Yes", nil
			}
			return "No", nil

		} else if fmt == "YES" {
			if vf.Bool() {
				return "YES", nil
			}
			return "NO", nil

		} else if fmt == "1" {
			if vf.Bool() {
				return "1", nil
			}
			return "0", nil

		} else if fmt == "T" {
			if vf.Bool() {
				return "T", nil
			}
			return "F", nil

		} else if fmt == "True" {
			if vf.Bool() {
				return "True", nil
			}
			return "False", nil

		} else if fmt == "TRUE" {
			if vf.Bool() {
				return "TRUE", nil
			}
			return "FALSE", nil

		} else {
			if vf.Bool() {
				return "true", nil
			}
			return "false", nil

		}
	case reflect.Struct:
		if tf.String() == "time.Time" {
			tm := vf.Interface().(time.Time)
			if tag.Get("format") != "" {
				return tm.Format(tag.Get("format")), nil
			}
			return tm.Format("2006-01-02"), nil
		}
	case reflect.Ptr:
		if vf.IsZero() {
			tt := tf.Elem()
			for tt.Kind() == reflect.Ptr {
				tt = tt.Elem()
			}
			return formatVal(vf, tt, tag)
		}
		tt := tf.Elem()
		vv := vf.Elem()
		for tt.Kind() == reflect.Ptr && !vv.IsZero() {
			tt = tt.Elem()
			vv = vv.Elem()
		}
		return formatVal(vv, tt, tag)
	}
	return "", errors.Errorf("can not format type:%v", tf.String())
}
