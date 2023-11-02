package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/gocolly/colly"
)

type Data struct {
	Batches [][]string `json:"batches"`
}
type Output struct {
	ObjectKey  string `json:"objectKey"`
	BucketName string `json:"bucketName"`
	BatchCount int    `json:"batchCount"`
}

var bucketName = "mamarkt-bucket"

func main() {
	lambda.Start(handler)
}

func handler() (Output, error) {
	var out Output
	allXMLs, err := collectXMLs("https://www.migros.com.tr/hermes/api/sitemaps/sitemap.xml", "//sitemap/loc")
	if err != nil {
		return out, err
	}
	filteredXMLs := filterXMLs(allXMLs, "product")

	var collectedProductUrls []string
	var wg sync.WaitGroup
	var mu sync.Mutex

	for _, v := range filteredXMLs {
		wg.Add(1)
		go func(v string) {
			collectedProductUrlBatch, err := collectXMLs(v, "//url/loc")
			if err != nil {
				log.Printf("Error collecting product URLs for %s: %v", v, err)
			} else {
				mu.Lock()
				collectedProductUrls = append(collectedProductUrls, collectedProductUrlBatch...)
				mu.Unlock()
			}
			wg.Done()
		}(v)
	}

	wg.Wait()

	batches := splitBatches(collectedProductUrls)
	objectKey, err := storeData(batches)
	if err != nil {
		return out, err
	}

	out.BatchCount = len(batches)
	out.ObjectKey = objectKey
	out.BucketName = bucketName
	return out, nil
}

func storeData(batches [][]string) (string, error) {
	dataToStore := Data{Batches: batches}

	data, err := json.Marshal(dataToStore)
	if err != nil {
		log.Printf("Error marshaling combined batch data: %v", err)
		return "", err
	}

	currentDateTime := time.Now().Format("2006-01-02_15-04-05")
	objectKey := "batches_" + currentDateTime + ".json"

	err = uploadToS3(bucketName, objectKey, data)
	if err != nil {
		log.Printf("Error uploading data to S3: %v", err)
		return "", err
	}

	return objectKey, nil
}

func uploadToS3(bucketName string, objectKey string, data []byte) error {
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(os.Getenv("AWS_REGION")),
		Credentials: credentials.NewEnvCredentials(),
	})
	if err != nil {
		return err
	}

	svc := s3.New(sess)

	_, err = svc.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		return err
	}

	return nil
}

func collectXMLs(url string, pattern string) ([]string, error) {
	var collectedXMLs []string

	c := colly.NewCollector()

	c.OnRequest(func(r *colly.Request) {
		fmt.Println("Visiting", r.URL)
	})

	c.OnError(func(_ *colly.Response, err error) {
		log.Println("Error collecting XML:", err)
	})

	c.OnResponse(func(r *colly.Response) {
		fmt.Println("Visited", r.Request.URL)
	})

	c.OnXML(pattern, func(e *colly.XMLElement) {
		collectedXMLs = append(collectedXMLs, e.Text)
	})

	c.OnScraped(func(r *colly.Response) {
		fmt.Println("Finished", r.Request.URL)
	})

	if err := c.Visit(url); err != nil {
		return nil, err
	}

	return collectedXMLs, nil
}

func filterXMLs(collectedXMLs []string, pattern string) []string {
	var filtered []string
	for _, xml := range collectedXMLs {
		if strings.Contains(xml, pattern) {
			filtered = append(filtered, xml)
		}
	}
	return filtered
}

func splitBatches(collectedProductUrls []string) [][]string {
	var batches [][]string
	for i := 0; i < len(collectedProductUrls); i += 25 {
		end := i + 25
		if end > len(collectedProductUrls) {
			end = len(collectedProductUrls)
		}

		batch := collectedProductUrls[i:end]

		batches = append(batches, batch)
	}
	return batches
}
