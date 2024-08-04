package main

import (
	"context"
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
	ID                      uuid.UUID `json:"uuid"`
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
	CreatedAt               time.Time `json:"createdAt"`
}

var (
	detailPageChan = make(chan string, 10)
	jobChan        = make(chan Job, 100)
	dbNotification = make(chan string)
	wg             sync.WaitGroup
	visitedUrl     = make(map[string]struct{})
	log            = logrus.New()
	maxWorkers     = 10
	maxRetries     = 3
)

func main() {

	err := godotenv.Load(".env")
	if err != nil {
		log.Fatal("Error loading .env file")
		return
	}
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
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		log.Fatal("Can not connect to database: ", err)
	}
	queries := database.New(db)
	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			logrus.Errorf("Can not close database connection")
		}
	}(db)

	// Start scraping list pages
	go func() {
		wg.Add(1)
		defer wg.Done()
		scrapeListPages()
		close(detailPageChan)
	}()

	// Start workers
	startTime := time.Now()
	for i := 0; i < maxWorkers; i++ {
		wg.Add(1)
		go worker()
	}

	go func() {
		for job := range jobChan {
			_, err := queries.CreateJobPost(context.Background(), database.CreateJobPostParams{
				ID:                      job.ID,
				DetailUrl:               job.DetailUrl,
				Title:                   job.Title,
				Company:                 job.Company,
				Location:                job.Location,
				Level:                   job.Level,
				ApplyUrl:                job.ApplyURL,
				MinimumQualifications:   job.MinimumQualifications,
				PreferredQualifications: job.PreferredQualifications,
				AboutJob:                job.AboutJob,
				Responsibilities:        job.Responsibilities,
				CreatedAt:               job.CreatedAt,
			})
			if err != nil {
				logrus.Errorf("Can not insert job post to database: %v", err)
			}
		}
		logrus.Debugf("All jobs inserted")
		dbNotification <- "finished"
	}()

	// Wait for all workers to finish
	wg.Wait()
	close(jobChan)
	<-dbNotification
	endTime := time.Now()
	elapsedTime := endTime.Sub(startTime)
	log.Infof("Scraping took %s\n", elapsedTime)
}

func worker() {
	defer wg.Done()
	for url := range detailPageChan {
		scrapeDetailPage(url)
	}
}

func getNewCollector() (*colly.Collector, error) {
	c := colly.NewCollector(
		colly.AllowedDomains("google.com", "www.google.com"),
		colly.UserAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"),
	)

	err := c.Limit(&colly.LimitRule{
		DomainGlob:  "*",
		RandomDelay: 1 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	return c, nil
}

func scrapeListPages() {

	c, err := getNewCollector()
	if err != nil {
		log.Fatalf("Can not initiate collector: %v", err)
	}
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
		retryRequest(r.Request)
	})

	for page := 125; ; page++ {
		listURL := fmt.Sprintf("https://www.google.com/about/careers/applications/jobs/results?page=%d", page)
		foundDetailURLs = false
		err := c.Visit(listURL)
		if err != nil {
			logrus.Errorf("Failed to visit list page %s: %v", listURL, err)
			return
		}

		// If no detail URLs found, stop scraping list pages
		if !foundDetailURLs {
			logrus.Debugf("No new detail URLs found on page %d. Stopping list page scraping.", page)
			return
		}
	}
}

func scrapeDetailPage(url string) {

	c, err := getNewCollector()
	if err != nil {
		log.Fatalf("Can not initiate collector: %v", err)
	}

	logrus.Infof("Scraping detail page: %s", url)

	// Extract job details
	c.OnHTML("div.DkhPwc", func(e *colly.HTMLElement) {
		job := Job{
			// Populate the Job struct
			ID:        uuid.New(),
			DetailUrl: url,
			Title:     e.ChildText("h2.p1N2lc"),
			Company:   e.ChildText("span.RP7SMd span"),
			Location:  e.ChildText("span.r0wTof"),
			Level:     e.ChildText("span.wVSTAb"),
			AboutJob:  extractAboutJob(e),
			ApplyURL:  "https://www.google.com/about/careers/applications" + strings.TrimPrefix(e.ChildAttr("a.WpHeLc.VfPpkd-mRLv6.VfPpkd-RLmnJb", "href"), "."),
			CreatedAt: time.Now(),
		}

		e.ForEach("div.BDNOWe ul li", func(_ int, el *colly.HTMLElement) {
			job.Responsibilities = append(job.Responsibilities, strings.TrimSpace(el.Text))
		})
		job.MinimumQualifications, job.PreferredQualifications = extractQualifications(e)
		jobChan <- job
	})

	c.OnRequest(func(r *colly.Request) {
		logrus.Debugf("Requesting detail page URL: %s", r.URL.String())
	})

	c.OnResponse(func(r *colly.Response) {
		logrus.Debugf("Received response from detail page URL: %s", r.Request.URL.String())
	})

	c.OnError(func(r *colly.Response, err error) {
		logrus.Errorf("Request detail failed with response status %d: %v", r.StatusCode, err)
		retryRequest(r.Request)
	})

	err = c.Visit(url)
	if err != nil {
		logrus.Errorf("Failed to visit detail page URL: %v", url)
		return
	}
}

func retryRequest(request *colly.Request) {
	retries, ok := request.Ctx.GetAny("retries").(int)
	if !ok {
		retries = 0
	}
	if retries < maxRetries {
		request.Ctx.Put("retries", retries+1)
		time.Sleep(2 * time.Second)
		logrus.Infof("Retrying %s (%d/%d)", request.URL, retries+1, maxRetries)
		err := request.Retry()
		if err != nil {
			logrus.Errorf("Retry failed : %v", err)
		}
	} else {
		logrus.Errorf("Max retries reached for %s", request.URL)
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

func extractAboutJob(e *colly.HTMLElement) []string {
	var aboutJob []string

	// Check for single string case
	if e.DOM.Find("div.aG5W3").ChildrenFiltered("p").Length() == 0 {
		// No <p> tags, treat the entire div as a single string
		aboutJob = append(aboutJob, e.ChildText("div.aG5W3"))
	} else {
		// Paragraphed case, collect all <p> tags
		e.ForEach("div.aG5W3 p", func(_ int, p *colly.HTMLElement) {
			text := p.Text
			if text != "" {
				aboutJob = append(aboutJob, text)
			}
		})
	}
	return aboutJob
}
