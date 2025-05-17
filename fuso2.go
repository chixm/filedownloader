package filedownloader

import (
	"errors"
	"io"
	"net/http"
	"os"
	"strconv"
	"sync"
)

// SplitFileDownload downloads a file in multiple parts.
func (fdl *FileDownloader) SplitFileDownload(url string, localFilePath string, splitPartsCount int) error {
	if fdl.State != StateReady {
		panic(`filedownloader has already started or done`)
	}
	fdl.State = StateDownloading

	// Get total file size
	resp, err := http.Head(url)
	if err != nil {
		return err
	}
	sizeStr := resp.Header.Get("Content-Length")
	if sizeStr == "" {
		return errors.New("Content-Length not found")
	}
	size, err := strconv.ParseInt(sizeStr, 10, 64)
	if err != nil {
		return err
	}

	partCount := splitPartsCount
	partSize := size / int64(partCount)
	var wg sync.WaitGroup
	tmpFiles := make([]string, partCount)
	errs := make([]error, partCount)

	for i := 0; i < partCount; i++ {
		wg.Add(1)
		start := int64(i) * partSize
		end := start + partSize - 1
		if i == partCount-1 {
			end = size - 1
		}
		tmpFile := localFilePath + ".part" + strconv.Itoa(i)
		tmpFiles[i] = tmpFile

		go func(i int, start, end int64, tmpFile string) {
			defer wg.Done()
			req, _ := http.NewRequest("GET", url, nil)
			req.Header.Set("Range", "bytes="+strconv.FormatInt(start, 10)+"-"+strconv.FormatInt(end, 10))
			res, err := http.DefaultClient.Do(req)
			if err != nil {
				errs[i] = err
				return
			}
			defer res.Body.Close()
			out, err := os.Create(tmpFile)
			if err != nil {
				errs[i] = err
				return
			}
			defer out.Close()
			_, err = io.Copy(out, res.Body)
			if err != nil {
				errs[i] = err
			}
		}(i, start, end, tmpFile)
	}
	wg.Wait()

	for _, err := range errs {
		if err != nil {
			return err
		}
	}

	// join tmp files
	out, err := os.Create(localFilePath)
	if err != nil {
		return err
	}
	defer out.Close()
	for _, tmpFile := range tmpFiles {
		in, err := os.Open(tmpFile)
		if err != nil {
			return err
		}
		_, err = io.Copy(out, in)
		in.Close()
		if err != nil {
			return err
		}
		os.Remove(tmpFile)
	}

	fdl.State = StateDone
	return nil
}
