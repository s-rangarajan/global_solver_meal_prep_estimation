package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/boltdb/bolt"
	uuid "github.com/satori/go.uuid"
)

const (
	bucket      = "bucket"
	commitCount = 100000
)

type MealPrepTimeLookuper interface {
	LookupMealPrepTimeWithContext(context.Context, string, string, time.Time) (string, error)
}

type boltMealPrepTimeLookuper struct {
	sync.RWMutex
	connection *bolt.DB

	filePath     string
	DownloadedAt time.Time
}

func NewBoltMealPrepTimeLookuper(filePath string) *boltMealPrepTimeLookuper {
	return &boltMealPrepTimeLookuper{filePath: filePath}
}

func (b *boltMealPrepTimeLookuper) LookupMealPrepTimeWithContext(_ context.Context, restaurantID, itemCount string, dispatchTime time.Time) (string, error) {
	b.RLock()
	defer b.RUnlock()

	if b.connection == nil {
		var err error
		b.connection, err = bolt.Open(b.filePath, 0600, &bolt.Options{ReadOnly: true})
		if err != nil {
			return "", fmt.Errorf("error opening bolt connection over file: %w", err)
		}
	}
	log.Println("here")
	var mealPrepTime string
	if err := b.connection.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte(bucket)).Cursor()
		if b == nil {
			return fmt.Errorf("unable to find bucket in database")
		}
		dayOfWeek := strconv.Itoa(int(dispatchTime.Weekday()))
		h, m, _ := dispatchTime.Clock()
		timeOfDay := strconv.Itoa(h*60 + m)
		prefix := []byte(mealMealPrepLookPrefix(restaurantID, itemCount, dayOfWeek))
		fmt.Println(dispatchTime, dayOfWeek, h, m)
		fmt.Println(string(prefix), timeOfDay)

		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			log.Println(string(k), string(v))
			parts := strings.Split(string(k), "|")
			if len(parts) != 4 {
				return fmt.Errorf("invalid keys in range scan")
			}
			if parts[3] > timeOfDay {
				return nil
			}
			mealPrepTime = string(v)
		}
		return nil
	}); err != nil {
		return "", fmt.Errorf("error searching for meal prep time in database: %w", err)
	}

	if mealPrepTime == "" {
		return "", fmt.Errorf("unable to find meal prep time for given restaurant/itemcount/time")
	}

	return mealPrepTime, nil
}

func (b *boltMealPrepTimeLookuper) UpdateMealPrepEstimatesWithContext(ctx context.Context, data io.Reader) error {
	log.Println("loading data")
	tempFile, err := ioutil.TempFile("", uuid.NewV4().String())
	if err != nil {
		return fmt.Errorf("error creating tempfile: %w", err)
	}

	defer os.Remove(tempFile.Name())
	defer tempFile.Close()

	boltDB, err := bolt.Open(tempFile.Name(), 0600, nil)
	if err != nil {
		return fmt.Errorf("error creating boltdb over tempfile: %w", err)
	}

	tx, err := boltDB.Begin(true)
	if err != nil {
		return fmt.Errorf("error beginning bolt transaction: %w", err)
	}

	bkt, err := tx.CreateBucket([]byte(bucket))
	if err != nil {
		return fmt.Errorf("error creating bucket: %w", err)
	}
	bkt.FillPercent = 1

	csvReader := csv.NewReader(data)
	commitCounter := 1
	for {
		line, err := csvReader.Read()
		if err == io.EOF {
			break
		}

		bkt.Put([]byte(makeMealPrepLookupKey(line[0], line[3], line[1], line[2])), []byte(line[4]))
		commitCounter++

		if commitCounter%commitCount == 0 {
			err = tx.Commit()
			if err != nil {
				return fmt.Errorf("error committing bolt transaction: %w", err)
			}
			tx, err = boltDB.Begin(true)
			if err != nil {
				return fmt.Errorf("error beginning bolt transaction: %w", err)
			}
			bkt = tx.Bucket([]byte(bucket))
			bkt.FillPercent = 1
			log.Println("loaded", commitCounter)
			log.Println("key", makeMealPrepLookupKey(line[0], line[3], line[1], line[2]), "value", line[4])
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("error committing bolt transaction: %w", err)
	}
	boltDB.Close()

	log.Println("committed")

	b.Lock()
	defer b.Unlock()

	log.Println("locked")

	if b.connection != nil {
		log.Println("closed")
		b.connection.Close()
	}
	if err := os.Rename(tempFile.Name(), b.filePath); err != nil {
		return fmt.Errorf("error renaming tempfile into permanent: %w", err)
	}
	log.Println("opening", b.filePath)
	b.connection, err = bolt.Open(b.filePath, 0600, &bolt.Options{ReadOnly: true, Timeout: 1 * time.Second})
	if err != nil {
		return fmt.Errorf("error opening bolt connection over file: %w", err)
	}
	log.Println("open")

	log.Println("loaded data")
	return nil
}

func makeMealPrepLookupKey(restaurantID, itemCount, dayOfWeek, timeOfDay string) string {
	var sb strings.Builder
	sb.WriteString(mealMealPrepLookPrefix(restaurantID, itemCount, dayOfWeek))
	sb.WriteString("|")
	sb.WriteString(timeOfDay)

	return sb.String()
}

func mealMealPrepLookPrefix(restaurantID, itemCount, dayOfWeek string) string {
	var sb strings.Builder
	sb.WriteString(restaurantID)
	sb.WriteString("|")
	sb.WriteString(itemCount)
	sb.WriteString("|")
	sb.WriteString(dayOfWeek)

	return sb.String()
}
