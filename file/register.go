package file

// file type
type fileHandler struct {
	reader        FileItemReader
	writer        FileItemWriter
	mergeSplitter MergeSplitter
}

var fileHandlers = map[string]*fileHandler{}

// RegisterFileType register file type
func RegisterFileType(ftype string, reader FileItemReader, writer FileItemWriter, mergeSplitter MergeSplitter) {
	fileHandlers[ftype] = &fileHandler{
		reader:        reader,
		writer:        writer,
		mergeSplitter: mergeSplitter,
	}
}

func getFileHandler(ftype string) *fileHandler {
	return fileHandlers[ftype]
}

// GetFileItemReader get FileItemReader by type
func GetFileItemReader(ftype string) FileItemReader {
	switch ftype {
	case TSV:
		return &tsvFileItemReader{}
	case CSV:
		return &csvFileItemReader{}
	case JSON:
		return &jsonFileItemReader{}
	default:
		fh := getFileHandler(ftype)
		if fh != nil && fh.reader != nil {
			return fh.reader
		}
	}
	return nil
}

// GetFileItemWriter get FileItemWriter by type
func GetFileItemWriter(ftype string) FileItemWriter {
	switch ftype {
	case TSV:
		return &tsvFileItemWriter{}
	case CSV:
		return &csvFileItemWriter{}
	case JSON:
		return &jsonFileItemWriter{}
	default:
		fh := getFileHandler(ftype)
		if fh != nil && fh.writer != nil {
			return fh.writer
		}
	}
	return nil
}

// GetFileMergeSplitter get MergeSplitter by type
func GetFileMergeSplitter(ftype string) MergeSplitter {
	switch ftype {
	case TSV:
		return &tsvFileMergeSplitter{}
	case CSV:
		return &csvFileMergeSplitter{}
	case JSON:
		return &jsonFileMergeSplitter{}
	default:
		fh := getFileHandler(ftype)
		if fh != nil && fh.mergeSplitter != nil {
			return fh.mergeSplitter
		}
	}
	return nil
}

// checksumer
var checksumers = map[string]Checksumer{}

func RegisterChecksumer(key string, ch Checksumer) {
	checksumers[key] = ch
}

// GetChecksumer get Checksumer by type
func GetChecksumer(key string) Checksumer {
	switch key {
	case OKFlag:
		return &OKFlagChecksumer{}
	case MD5:
		return &MD5Checksumer{}
	case SHA1:
		return &SHA1Checksumer{}
	case SHA256:
		return &SHA256Checksumer{}
	case SHA512:
		return &SHA512Checksumer{}
	default:
		return checksumers[key]
	}
}
