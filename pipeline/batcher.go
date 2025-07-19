package pipeline

import (
	"context"
	"log"
	"loganalytics/storage"
	"time"
)

func StartBatcher(ctx context.Context, in <-chan FilteredLog, es *storage.ElasticClient, batchSize int, flushInterval time.Duration) <-chan struct{} {
	done := make(chan struct{})
	batch := make([]storage.LogDoc, 0, batchSize)
	ticker := time.NewTicker(flushInterval)
	totalProcessed := 0

	go func() {
		defer close(done)
		defer ticker.Stop()
		log.Printf("Batcher started with batch size %d, flush interval %v", batchSize, flushInterval)

		for {
			select {
			case filteredLog, ok := <-in:
				if !ok {
					log.Printf("Batcher received close signal, flushing final batch of %d docs", len(batch))
					flushBatch(es, batch)
					log.Printf("Batcher completed, total processed: %d", totalProcessed)
					return
				}
				batch = append(batch, filteredLog.ToDoc())
				totalProcessed++
				if len(batch) >= batchSize {
					log.Printf("Flushing batch of %d docs (total processed: %d)", len(batch), totalProcessed)
					flushBatch(es, batch)
					batch = batch[:0]
				}

			case <-ticker.C:
				if len(batch) > 0 {
					log.Printf("Timer flush: %d docs (total processed: %d)", len(batch), totalProcessed)
					flushBatch(es, batch)
					batch = batch[:0]
				}

			case <-ctx.Done():
				log.Printf("Batcher context cancelled, flushing %d docs", len(batch))
				flushBatch(es, batch)
				log.Printf("Batcher stopped, total processed: %d", totalProcessed)
				return
			}
		}
	}()

	return done
}

func flushBatch(es *storage.ElasticClient, docs []storage.LogDoc) {
	if len(docs) == 0 {
		return
	}
	log.Printf("Attempting to insert %d documents to Elasticsearch", len(docs))
	if err := es.BulkInsert(docs); err != nil {
		log.Printf("Error inserting docs: %v", err)
		// 重试机制
		for i := 1; i < 3; i++ {
			log.Printf("beginning retry attempt #%d", i)
			err := es.BulkInsert(docs)
			if err != nil {
				log.Printf("#%d retry fail... reason of err: %v", i, err)
			}
		}
	} else {
		log.Printf("Successfully inserted %d documents to Elasticsearch", len(docs))
	}
}
