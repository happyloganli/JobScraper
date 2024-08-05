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
	"log"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// Job represents a job posting with various details about the position.

// ID is a unique identifier for the job posting.
// DetailUrl is the URL where more detailed information about the job can be found.
// CreatedAt is the timestamp when the job posting was created.
// Title is the job title or position name.
// Location specifies where the job is located.
// Employment describes the type of employment, such as full-time, part-time, or contract.
// PostingDate is the date when the job was posted.
// Description provides a detailed description of the job role and responsibilities.
// JobID is an additional identifier for the job, which may be used by the job listing platform.
// ApplyURL is the URL where applicants can submit their application for the job.
type Job struct {
	ID          uuid.UUID `json:"uuid"`
	DetailUrl   string    `json:"detail_url"`
	CreatedAt   time.Time `json:"CreatedAt"`
	Title       string    `json:"Title"`
	Location    string    `json:"Location"`
	Employment  string    `json:"Employment"`
	PostingDate string    `json:"posting_date"`
	Description string    `json:"Description"`
	JobID       string    `json:"job_id"`
	ApplyURL    string    `json:"apply_url"`
}

// log is an instance of logrus.Logger used for logging messages throughout the application.
// maxWorkers defines the maximum number of concurrent worker goroutines that can be used.
// maxRetries specifies the maximum number of times an operation should be retried in case of failure.
// initialBackoff specifies the initial backoff time
// scrapeDelay is the delay milliseconds of scraping two pages, to avoid being blocked by web server.

var (
	maxWorkers     = 10
	maxRetries     = 3
	initialBackoff = 10 * time.Second
	scrapeDelay    = 1000
	batchSize      = 100
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
	var lastErr error

	for retries := 0; retries < maxRetries; retries++ {
		if err := inserter.Put(ctx, jobs); err != nil {
			logrus.Errorf("Failed to insert jobs to BigQuery: %v", err)
			lastErr = err
			backoff := initialBackoff * (1 << retries)
			logrus.Infof("Retrying in %v...", backoff)
			time.Sleep(backoff)
		} else {
			logrus.Infof("Successfully inserted %d jobs to BigQuery", len(jobs))
			return nil
		}
	}
	return lastErr
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
	logrus.SetLevel(logLevel)

	// Handler for scraping task request. Once receive a get request, start a scraping task.
	// The Google Cloud Scheduler can schedule a task in this way.
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			go startScraping()
			fmt.Fprintln(w, "Scraping started")
		} else {
			http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		}
	})

	// Listen on specific port
	port := os.Getenv("PORT")
	if port == "" {
		log.Fatal("$PORT is not found in the environment variables")
	}
	fmt.Printf("Server is starting on port %s...\n", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

// Start scraping a list. Extract detail page urls for each job post and store them in a channel.
// Initiate workers to fetch detail page urls to scrape pages concurrently.
// Scrapped jobs are sent to job channel and wait for database transfer.
func startScraping() {

	logrus.Infof("Starting scraping jobs...")
	defer logrus.Infof("Scraping jobs done")

	detailPageChan := make(chan string, 10)
	jobChan := make(chan Job, 100)
	visitedUrl := make(map[string]struct{})

	// Start a go routine to scrap list pages
	go func() {
		wg.Add(1)
		defer wg.Done()
		scrapeListPages(visitedUrl, detailPageChan)
		close(detailPageChan)
	}()

	// Start workers to scrap detail pages
	for i := 0; i < maxWorkers; i++ {
		wg.Add(1)
		go worker(detailPageChan, jobChan)
	}

	// Start a go routine to insert job posts to big query
	// Insert by batches, default batch is 1000
	projectID := os.Getenv("GOOGLE_CLOUD_PROJECT")
	datasetID := os.Getenv("GOOGLE_DATASET_ID")
	tableID := os.Getenv("GOOGLE_TABLE_ID")

	var jobBuffer []Job
	dbNotification := make(chan string)

	// Jobs are stored in a buffer that can reduce call to the BigQuery API.
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

// worker method runs a worker to scrap detail pages. It fetches url of detail pages from detail page channel
// and runs scrapeDetailPage method
func worker(detailPageChan chan string, jobChan chan Job) {
	defer wg.Done()
	for url := range detailPageChan {
		scrapeDetailPage(url, jobChan)
		sleepRandomly()
	}
}

// getNewCollector method initiate a new colly Collector.
// It sets the user agent.
func getNewCollector() (*colly.Collector, error) {

	// The collector can only request in allow domains and use the user agent.
	c := colly.NewCollector(
		colly.UserAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"),
	)
	return c, nil
}

// scrapeListPages handles scraping job listing pages and extracting detail URLs.
// It will keep scraping until the visited page has no detail URL.
func scrapeListPages(visitedUrl map[string]struct{}, detailPageChan chan string) {

	c, err := getNewCollector()
	if err != nil {
		log.Fatalf("Can not initiate collector: %v", err)
	}

	// The detailed page url in the list page needs to be constructed with a base URL.
	// The foundDetailURLS is a flag variable to check if the visited page contains detail page URL, if not the loop will stop.
	// Extracted detail page URL will be sent to detailPageChan,visitedUrl is a map recording visited detailed pages,
	// if a detail page URL visited it will not be sent to detailPageChan.
	baseUrl := os.Getenv("BASE_URL")
	var foundDetailURLs bool
	c.OnHTML("a.stretched-link.js-view-job", func(e *colly.HTMLElement) {
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
	for page := 1; ; page++ {
		listURL := fmt.Sprintf("https://careers.salesforce.com/en/jobs/?page=%d#results", page)
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
func scrapeDetailPage(url string, jobChan chan Job) {

	c, err := getNewCollector()
	if err != nil {
		log.Fatalf("Can not initiate collector: %v", err)
	}

	logrus.Infof("Scraping detail page: %s", url)

	// Extract job details. Create SFJob Objects and send them to JobChan, the database routine will insert them into database.
	//
	c.OnHTML("main", func(e *colly.HTMLElement) {
		job := Job{
			// Populate the SFJob struct
			ID:          uuid.New(),
			DetailUrl:   url,
			Title:       e.ChildText("h1.hero-heading"),
			Location:    e.ChildText(".list-unstyled.job-meta li:nth-child(2)"),
			Employment:  e.ChildText(".list-unstyled.job-meta li:nth-child(3)"),
			PostingDate: e.ChildText(".list-unstyled.job-meta li:nth-child(4) time"),
			JobID:       e.ChildText(".list-unstyled.job-meta li:nth-child(5)"),
			Description: e.ChildText("article.cms-content p"),
			ApplyURL:    e.ChildAttr("#js-apply-external", "href"),
			CreatedAt:   time.Now(),
		}

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
		backoff := initialBackoff * (1 << retries)
		logrus.Infof("Retrying in %v...", backoff)
		time.Sleep(backoff)
		err := request.Retry()
		if err != nil {
			logrus.Errorf("Retry failed : %v", err)
		}
	} else {
		logrus.Errorf("Max retries reached for %s", request.URL)
	}
}
