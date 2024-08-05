package main

import (
	"cloud.google.com/go/bigquery"
	"context"
	"fmt"
	"github.com/gocolly/colly"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"github.com/sirupsen/logrus"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"
)

/*
Job struct stores information of a job post.

ID is the unique identifier for the job posting
DetailUrl is the url where detail information of a job post could be found. Detail Url can also be used as a third party id.
Company is the name of the company offering the job.
Title is the job title.
Location specifies the location of the job.
Level indicates the job level, such as "Early", "Mid", or "Advanced".
ApplyURL is the URL where applicants can apply for the job.
MinimumQualifications lists the minimum qualifications required for the job.
PreferredQualifications lists the qualifications that are preferred but not required.
AboutJob provides a description of the job responsibilities and expectations.
Responsibilities outlines the key responsibilities associated with the job.
CreatedAt is the date when the job posting was created.
*/
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

/*
detailPageChan is a buffered channel used to handle detail page URLs or identifiers.
jobChan is a buffered channel used to pass Job structs between different parts of the application.
visitedUrl is a map used to track URLs that have already been processed to prevent reprocessing.
log is an instance of logrus.Logger used for logging messages throughout the application.
maxWorkers defines the maximum number of concurrent worker goroutines that can be used.
maxRetries specifies the maximum number of times an operation should be retried in case of failure.
scrapeDelay is the delay milliseconds of scraping two pages, to avoid being blocked by web server.
*/
var (
	detailPageChan = make(chan string, 10)
	jobChan        = make(chan Job, 100)
	visitedUrl     = make(map[string]struct{})
	log            = logrus.New()
	maxWorkers     = 10
	maxRetries     = 3
	scrapeDelay    = 1000
	batchSize      = 1000
	wg             sync.WaitGroup
	mu             sync.Mutex
)

// Streaming insert Jobs to big query.
func insertJobsToBigQuery(jobs []Job, projectID, datasetID, tableID string) error {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return err
	}
	defer client.Close()

	inserter := client.Dataset(datasetID).Table(tableID).Inserter()
	if err := inserter.Put(ctx, jobs); err != nil {
		return err
	}
	return nil
}

func main() {

	// Load configuration from .env file
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

	// Start a go routine to scrap list pages
	go func() {
		wg.Add(1)
		defer wg.Done()
		scrapeListPages()
		close(detailPageChan)
	}()

	// Start workers to scrap detail pages
	for i := 0; i < maxWorkers; i++ {
		wg.Add(1)
		go worker()
	}

	// Start a go routine to insert job posts to big query
	// Insert by batches, default batch is 1000
	projectID := os.Getenv("GOOGLE_CLOUD_PROJECT")
	datasetID := os.Getenv("GOOGLE_DATASET_ID")
	tableID := os.Getenv("GOOGLE_TABLE_ID")

	var jobBuffer []Job
	dbNotification := make(chan string)

	go func() {
		for job := range jobChan {
			mu.Lock()
			jobBuffer = append(jobBuffer, job)
			if len(jobBuffer) >= batchSize {
				batch := jobBuffer
				jobBuffer = make([]Job, 0, batchSize)
				mu.Unlock()
				err := insertJobsToBigQuery(batch, projectID, datasetID, tableID)
				if err != nil {
					logrus.Errorf("Failed to insert job batch to BigQuery: %v", err)
				}
			} else {
				mu.Unlock()
			}
		}

		if len(jobBuffer) > 0 {
			err := insertJobsToBigQuery(jobBuffer, projectID, datasetID, tableID)
			if err != nil {
				logrus.Errorf("Failed to insert remaining job batch to BigQuery: %v", err)
			}
		}

		dbNotification <- "ok"
	}()

	// Wait for all workers to finish
	wg.Wait()
	close(jobChan)

	// Wait for all job posts are inserted into the database
	<-dbNotification
	return
}

/*
worker method runs a worker to scrap detail pages. It fetches url of detail pages from detail page channel
and runs scrapeDetailPage method
*/
func worker() {
	defer wg.Done()
	for url := range detailPageChan {
		scrapeDetailPage(url)
		sleepRandomly()
	}
}

/*
getNewCollector method initiate a new colly Collector.
It sets the allow domains and user agent.
*/
func getNewCollector() (*colly.Collector, error) {

	// The collector can only request in allow domains and use the user agent.
	c := colly.NewCollector(
		colly.AllowedDomains("google.com", "www.google.com"),
		colly.UserAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"),
	)

	return c, nil
}

/*
scrapeListPages handles scraping job listing pages and extracting detail URLs.
It will keep scraping until the visited page has no detail URL.
*/
func scrapeListPages() {

	c, err := getNewCollector()
	if err != nil {
		log.Fatalf("Can not initiate collector: %v", err)
	}

	// The detailed page url in the list page needs to be constructed with a base URL.
	// The foundDetailURLS is a flag variable to check if the visited page contains detail page URL, if not the loop will stop.
	// Extracted detail page URL will be sent to detailPageChan,visitedUrl is a map recording visited detailed pages,
	// if a detail page URL visited it will not be sent to detailPageChan.
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

	// If the response has an error, the collector will retry the request
	c.OnError(func(r *colly.Response, err error) {
		logrus.Errorf("Request listpage failed with response status %d: %v", r.StatusCode, err)
		retryRequest(r.Request)
	})

	// Iterate the list pages. If no detail URLs found, stop scraping list pages
	// Delayed randomly to avoid being blocked by the web server.
	for page := 126; ; page++ {
		listURL := fmt.Sprintf("https://www.google.com/about/careers/applications/jobs/results?page=%d", page)
		foundDetailURLs = false
		err := c.Visit(listURL)
		if err != nil {
			logrus.Errorf("Failed to visit list page %s: %v", listURL, err)
			return
		}

		if !foundDetailURLs {
			logrus.Debugf("No new detail URLs found on page %d. Stopping list page scraping.", page)
			return
		}
		sleepRandomly()
	}
}

// scrape the detail page and extracts job detail information and send job posts to the job channel
func scrapeDetailPage(url string) {

	c, err := getNewCollector()
	if err != nil {
		log.Fatalf("Can not initiate collector: %v", err)
	}

	logrus.Infof("Scraping detail page: %s", url)

	// Extract job details. Create Job Objects and send them to JobChan, the database routine will insert them into database.
	//
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

	// If the response has an error, the collector will retry the request
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

// Sleep a randomly time duration between 1s to 2s
func sleepRandomly() {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	sleepDuration := time.Duration(r.Int63n(int64(scrapeDelay))+1000) * time.Millisecond
	time.Sleep(sleepDuration)
}

// Retry the request in a max retry time.
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

// Helper method to extract minimum qualification and preferred qualification because they have same selectors.
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

// Helper method to extract AboutJob.
// The selector of AboutJob may contains a "<p>..</p>" or not.
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
