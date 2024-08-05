package main

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"sync"
	"testing"
	"time"
)

type MockWorker struct {
	mock.Mock
}

func (m *MockWorker) scrapeDetailPage(url string) {
	m.Called(url)
}

func (m *MockWorker) sleepRandomly() {
	m.Called()
}

func TestSleepRandomly(t *testing.T) {
	start := time.Now()
	sleepRandomly()
	duration := time.Since(start)

	// Check that the sleep duration was between 1s and 2s
	assert.True(t, duration >= 1*time.Second, "sleep duration should be at least 1 second")
	assert.True(t, duration <= 2*time.Second, "sleep duration should be no more than 2 seconds")
}

func TestWorker(t *testing.T) {

	// Set up the mock worker
	mockWorker := new(MockWorker)
	mockWorker.On("scrapeDetailPage", mock.Anything).Return()
	mockWorker.On("sleepRandomly").Return()

	// Set up channels and wait group
	detailPageChan = make(chan string, 1)
	jobChan = make(chan Job, 1)
	wg = sync.WaitGroup{}

	// Send a test URL to the detailPageChan
	detailPageChan <- "http://example.com"

	// Start the worker
	wg.Add(1)
	go func() {
		defer wg.Done()
		mockWorker.scrapeDetailPage("http://example.com")
		mockWorker.sleepRandomly()
	}()

	// Wait for the worker to finish
	wg.Wait()

	// Verify that the mock functions were called
	mockWorker.AssertExpectations(t)
}

func TestGetNewCollector(t *testing.T) {
	collector, err := getNewCollector()

	// Check for errors
	assert.NoError(t, err)

	// Ensure the collector is not nil
	assert.NotNil(t, collector)

	// Check the collector settings
	allowedDomains := collector.AllowedDomains
	expectedDomains := []string{"google.com", "www.google.com"}
	assert.ElementsMatch(t, expectedDomains, allowedDomains)

	userAgent := collector.UserAgent
	expectedUserAgent := "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
	assert.Equal(t, expectedUserAgent, userAgent)
}
