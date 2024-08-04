package main

import (
	"database/sql"
	"fmt"
	"github.com/gocolly/colly"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"github.com/sirupsen/logrus"
	"jobScraper/internal/database"
	"os"
	"strings"
	"sync"
	"time"
)

type Job struct {
	UUID                    uuid.UUID `json:"uuid"`
	DetailUrl               string    `json:"detail_url"`
	Title                   string    `json:"title"`
	Company                 string    `json:"company"`
	Location                string    `json:"location"`
	Level                   string    `json:"level"`
	ApplyURL                string    `json:"applyURL"`
	MinimumQualifications   []string  `json:"minimumQualifications"`
	PreferredQualifications []string  `json:"preferredQualifications"`
	AboutJob                []string  `json:"aboutJob"`
	Responsibilities        []string  `json:"responsibilities"`
	CreatedDate             string    `json:"createdDate"`
}

var (
	detailPageChan = make(chan string, 10) // Channel for detail pages
	jobChan        = make(chan Job, 100)   // Channel for jobs
	wg             sync.WaitGroup
	visitedUrl     = make(map[string]struct{})
	log            = logrus.New()
	maxWorkers     = 10
)

func main() {
	godotenv.Load(".env")
	logLevel, err := logrus.ParseLevel(strings.ToLower(os.Getenv("LOG_LEVEL")))
	if err != nil {
		logLevel = logrus.InfoLevel
	}
	log.SetLevel(logLevel)

	// Get postgres database url
	dbURL := os.Getenv("DB_URL")
	if dbURL == "" {
		log.Fatal("$DB_URL is not found in the environment variables")
	}

	// Open a database instance
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		log.Fatal("Can not connect to database: ", err)
	}
	queries := database.New(db)
	defer db.Close()

	startTime := time.Now()

	// Start workers
	for i := 0; i < maxWorkers; i++ {
		wg.Add(1)
		go worker(queries)
	}

	// Start scraping list pages
	go func() {
		scrapeListPages()
		close(detailPageChan)
	}()
	go func() {
		for job := range jobChan {
			logrus.Info(job)
		}
	}()

	// Wait for all workers to finish
	wg.Wait()
	close(jobChan)
	endTime := time.Now()
	elapsedTime := endTime.Sub(startTime)
	log.Infof("Scraping took %s\n", elapsedTime)
}

func worker(queries *database.Queries) {
	defer wg.Done()
	for url := range detailPageChan {
		scrapeDetailPage(url, queries)
		time.Sleep(100 * time.Millisecond)
	}
}

func scrapeListPages() {
	c := colly.NewCollector()
	baseUrl := "https://www.google.com/about/careers/applications/"
	var foundDetailURLs bool
	c.OnHTML("a.WpHeLc.VfPpkd-mRLv6.VfPpkd-RLmnJb", func(e *colly.HTMLElement) {
		detailURL := baseUrl + e.Attr("href")
		if _, exists := visitedUrl[detailURL]; !exists {
			visitedUrl[detailURL] = struct{}{}
			detailPageChan <- detailURL
			foundDetailURLs = true
		}
	})

	c.OnRequest(func(r *colly.Request) {
		logrus.Infof("Requesting list page URL: %s", r.URL.String())
	})

	c.OnResponse(func(r *colly.Response) {
		logrus.Debugf("Received response from list page URL: %s", r.Request.URL.String())
	})

	c.OnError(func(r *colly.Response, err error) {
		logrus.Errorf("Request listpage failed with response status %d: %v", r.StatusCode, err)
	})

	for page := 1; ; page++ {
		listURL := fmt.Sprintf("https://www.google.com/about/careers/applications/jobs/results?page=%d", page)
		err := c.Visit(listURL)
		if err != nil {
			logrus.Errorf("Failed to visit list page URL: %v", err)
			return
		}

		// If no detail URLs found, stop scraping list pages
		if !foundDetailURLs {
			logrus.Debugf("No new detail URLs found on page %d. Stopping list page scraping.", page)
			return
		}

		time.Sleep(1000 * time.Millisecond)
	}
}

func scrapeDetailPage(url string, queries *database.Queries) {

	logrus.Infof("Scraping detail page: %s", url)
	c := colly.NewCollector()

	// Extract job details
	c.OnHTML("div.DkhPwc", func(e *colly.HTMLElement) {
		job := Job{
			// Populate the Job struct
			UUID:        uuid.New(),
			DetailUrl:   url,
			Title:       e.ChildText("h2.p1N2lc"),
			Company:     e.ChildText("span.RP7SMd span"),
			Location:    e.ChildText("span.r0wTof"),
			Level:       e.ChildText("span.wVSTAb"),
			ApplyURL:    "https://www.google.com/about/careers/applications" + strings.TrimPrefix(e.ChildAttr("a.WpHeLc.VfPpkd-mRLv6.VfPpkd-RLmnJb", "href"), "."),
			CreatedDate: time.Now().Format("2006-01-02 15:04:05"),
		}
		//e.ForEach("div.KwJkGe ul li", func(_ int, el *colly.HTMLElement) {
		//	job.MinimumQualifications = append(job.MinimumQualifications, strings.TrimSpace(el.Text))
		//})
		//e.ForEach("div.KwJkGe ul li", func(_ int, el *colly.HTMLElement) {
		//	job.PreferredQualifications = append(job.MinimumQualifications, strings.TrimSpace(el.Text))
		//})
		e.ForEach("div.aG5W3 p", func(_ int, el *colly.HTMLElement) {
			job.AboutJob = append(job.AboutJob, strings.TrimSpace(el.Text))
		})
		e.ForEach("div.BDNOWe ul li", func(_ int, el *colly.HTMLElement) {
			job.Responsibilities = append(job.Responsibilities, strings.TrimSpace(el.Text))
		})
		job.MinimumQualifications, job.PreferredQualifications = extractQualifications(e)
		jobChan <- job
	})

	//// Create an instance of Job struct
	//job := &Job{}
	//
	//// OnHTML callback for job title
	//c.OnHTML("h2.p1N2lc", func(e *colly.HTMLElement) {
	//	job.Title = e.Text
	//})
	//
	//// OnHTML callback for company name
	//c.OnHTML("span.RP7SMd span", func(e *colly.HTMLElement) {
	//	job.Company = e.Text
	//})
	//
	//// OnHTML callback for location
	//c.OnHTML("span.r0wTof", func(e *colly.HTMLElement) {
	//	job.Location = e.Text
	//})
	//
	//c.OnHTML("span.wVSTAb", func(e *colly.HTMLElement) {
	//	job.Level = e.Text
	//})
	//
	//// OnHTML callback for apply URL
	//c.OnHTML("a.WpHeLc.VfPpkd-mRLv6.VfPpkd-RLmnJb", func(e *colly.HTMLElement) {
	//	job.ApplyURL = "https://www.google.com/about/careers/applications" + strings.TrimPrefix(e.Attr("href"), ".")
	//})
	//
	//// OnHTML callback for about job
	//c.OnHTML("div.job-description", func(e *colly.HTMLElement) {
	//	job.AboutJob = e.Text
	//})
	//
	//// OnHTML callback for responsibilities
	//c.OnHTML("div.responsibilities", func(e *colly.HTMLElement) {
	//	job.Responsibilities = e.Text
	//})
	//
	//// OnHTML callback for minimum qualifications
	//c.OnHTML("div.KwJkGe", func(e *colly.HTMLElement) {
	//
	//	e.ForEach("ul li", func(_ int, el *colly.HTMLElement) {
	//		job.MinimumQualifications = append(job.MinimumQualifications, strings.TrimSpace(el.Text))
	//	})
	//})
	//
	//// OnHTML callback for preferred qualifications
	//c.OnHTML("div.Kp1N2lc", func(e *colly.HTMLElement) {
	//	if strings.Contains(e.Text, "Preferred qualifications:") {
	//		job.PreferredQualifications = e.Text
	//	}
	//})

	c.OnRequest(func(r *colly.Request) {
		logrus.Debugf("Requesting detail page URL: %s", r.URL.String())
	})

	c.OnResponse(func(r *colly.Response) {
		logrus.Debugf("Received response from detail page URL: %s", r.Request.URL.String())
	})

	c.OnError(func(r *colly.Response, err error) {
		logrus.Errorf("Request detail failed with response status %d: %v", r.StatusCode, err)
	})

	err := c.Visit(url)
	if err != nil {
		logrus.Errorf("Failed to visit detail page URL: %v", url)
		return
	}
}

func extractQualifications(e *colly.HTMLElement) (minQuals []string, prefQuals []string) {
	// Extract minimum qualifications
	e.ForEach("div.KwJkGe h3:contains('Minimum qualifications:') + ul li", func(_ int, el *colly.HTMLElement) {
		qualification := strings.TrimSpace(el.Text)
		if qualification != "" {
			minQuals = append(minQuals, qualification)
		}
	})

	// Extract preferred qualifications
	e.ForEach("div.KwJkGe h3:contains('Preferred qualifications:') + ul li", func(_ int, el *colly.HTMLElement) {
		qualification := strings.TrimSpace(el.Text)
		if qualification != "" {
			prefQuals = append(prefQuals, qualification)
		}
	})

	return minQuals, prefQuals
}
