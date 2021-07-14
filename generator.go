package wikiFtsGen

import (
	"compress/bzip2"
	"fmt"
	"github.com/dustin/go-wikiparse"
	"github.com/m-m-f/gowiki"
	"log"
	"os"
	"regexp"
	"strings"
	"time"
)

type Doc struct {
	Id     uint64
	Title  string
	Text   string
	Date   time.Time
	Author string
}

func (d Doc) String() string {
	return fmt.Sprintf("%8d | %s | %s | %s", d.Id, d.Title, d.Date.Format(time.RFC850), d.Author)
}

type pg struct {
}

func (P pg) Get(_ gowiki.WikiLink) (string, error) {
	return "", nil
}

func Generate(files []string, startId uint64, endId uint64, ch chan Doc) error {
	defer close(ch)

	for _, f := range files {

		file, err := os.Open(f)
		if err != nil {
			return err
		}

		reader := bzip2.NewReader(file)
		p, err := wikiparse.NewParser(reader)
		if err != nil {
			return err
		}

		count, errCount := 0, 0
		for err == nil {
			if errCount > 100 {
				return fmt.Errorf("%d errors from parser", errCount)
			}

			var page *wikiparse.Page
			page, err = p.Next()
			if err != nil {
				errCount++

				continue
			}
			if page.ID < startId {
				continue
			} else if page.ID > endId {
				return nil
			}
			if  strings.HasPrefix(strings.ToLower(page.Title), "liste") {
				continue
			}

			art, err := gowiki.ParseArticle(page.Title, page.Revisions[0].Text, pg{})
			if err != nil {
				log.Println(err)
				errCount++
				continue
			}
			count++

			var ts time.Time
			ts, err = time.Parse(time.RFC3339, page.Revisions[0].Timestamp)
			if err != nil {
				ts = time.Time{}
			}
			t := cleanText(art)
			ch <- Doc{
				Id: page.ID,
				Title:  page.Title,
				Text:   t,
				Date:   ts,
				Author: fmt.Sprintf("%s@wikipedia.de", page.Revisions[0].Contributor.Username),
			}
		}

		if err := file.Close(); err != nil {
			log.Println("error closing file", f, err)
		}
	}
	return nil
}

var rgStop = regexp.MustCompile(`(?mi)^\s?(einzelnachweise|weblinks|literatur)\s$`)
var rgRem1 = regexp.MustCompile(`(?mi)^(?:[\t ]*(?:\r?\n|\r))+`)
var rgRem2 = regexp.MustCompile(`(?mi)^\s?(\w+\s?){1,4}\s?$`)

func cleanText(art *gowiki.Article) string {

	txt := art.GetText()
	ind := rgStop.FindStringIndex(txt)
	if len(ind) == 2 {
		txt = txt[:ind[0]]
	}

	txt = rgRem1.ReplaceAllString(txt, "\n")
	txt = rgRem2.ReplaceAllString(txt, "")

	return txt
}
