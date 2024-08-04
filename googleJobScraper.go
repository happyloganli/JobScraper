package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/gocolly/colly"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"github.com/sirupsen/logrus"
	"jobScraper/internal/database"
	"os"
	"strings"
	"time"
)

type oldJob struct {
	ThirdId               string    `json:"third_id"`
	Title                 string    `json:"title"`
	Company               string    `json:"company"`
	Location              string    `json:"location"`
	Level                 string    `json:"level"`
	MinimumQualifications []string  `json:"minimum_qualifications"`
	CreatedAt             time.Time `json:"created_at"`
}

func scrapeJobs(log *logrus.Logger) {
	c := colly.NewCollector()

	c.OnHTML("div.sMn82b", func(e *colly.HTMLElement) {
		job := oldJob{
			Title:    e.ChildText("div.ObfsIf-oKdM2c div.ObfsIf-eEDwDf h3.QJPWVe"),
			Company:  e.ChildText("span.RP7SMd span"),
			Location: e.ChildText("span.r0wTof"),
			Level:    e.ChildText("span.wVSTAb"),
		}

		e.ForEach("div.Xsxa1e ul li", func(_ int, el *colly.HTMLElement) {
			job.MinimumQualifications = append(job.MinimumQualifications, strings.TrimSpace(el.Text))
		})

		//jobChan <- job
	})

	c.OnRequest(func(r *colly.Request) {
		log.Info("Visiting", r.URL.String())
	})

	c.OnError(func(r *colly.Response, err error) {
		log.Error("Request URL:", r.Request.URL, "failed with response:", r, "\nError:", err)
	})

	for page := 1; page < 130; page++ {
		// Visit the page
		err := c.Visit(fmt.Sprintf("https://www.google.com/about/careers/applications/jobs/results?page=%d", page))
		if err != nil {
			log.Error(err)
		}

	}
	close(jobChan)
}

func main() {
	godotenv.Load(".env")
	logLevel, err := logrus.ParseLevel(strings.ToLower(os.Getenv("LOG_LEVEL")))
	if err != nil {
		logLevel = logrus.InfoLevel
	}
	log := logrus.New()
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

	wg.Add(1)
	go scrapeJobs(log)         // Start scraping in a goroutine
	go writeToDB(queries, log) // Start database writing in a goroutine

	wg.Wait() // Wait for all goroutines to finish
	endTime := time.Now()
	elapsedTime := endTime.Sub(startTime)
	log.Info("Scraping took %s\n", elapsedTime)
}

func writeToDB(queries *database.Queries, log *logrus.Logger) {
	defer wg.Done()
	for job := range jobChan {
		_, err := queries.CreateJobPost(context.Background(), database.CreateJobPostParams{
			ID:        uuid.New(),
			Title:     job.Title,
			Company:   job.Company,
			Location:  job.Location,
			Level:     job.Level,
			CreatedAt: time.Now(),
		})
		if err != nil {
			log.Error(err)
		}
	}
}
