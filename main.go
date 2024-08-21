package main

import (
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/cheggaaa/pb/v3"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/jackc/pgx/v4/stdlib"
	"github.com/sirupsen/logrus"
)

const (
	numRecords     = 50
	dynamoDBTable  = "TestTable"
	numRecordsd    = 50
	dynamoDBTabled = "LeaderboardTable"
)

var (
	uuids  = make([]string, 0, numRecords)
	uuidsD = make([]string, 0, numRecords)
	mu     sync.Mutex
)

func main() {
	RunBenchmark()
}

func RunBenchmark() {
	ctx := context.Background()

	connString := ""
	pgPool, err := pgxpool.Connect(ctx, connString)
	if err != nil {
		log.Fatalf("Unable to connect to PostgreSQL: %v", err)
	}
	runMigrations(pgPool)
	defer pgPool.Close()
	postgresCleanUp(ctx, pgPool)

	// DynamoDB setup
	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion("eu-west-1"))
	if err != nil {
		log.Fatalf("Unable to load AWS SDK config: %v", err)
	}
	dynamoClient := dynamodb.NewFromConfig(awsCfg)

	// Run benchmarks
	fmt.Println("Starting benchmarks...")
	benchmark(ctx, pgPool, dynamoClient)
}

func runMigrations(pool *pgxpool.Pool) error {
	// Convert pgxpool.Pool to *sql.DB
	db := stdlib.OpenDB(*pool.Config().ConnConfig)

	if err := runMigration(db, "./migrations/001_create_test_table.sql"); err != nil {
		logrus.Infof("Migration failed: %v", err)
	}

	log.Println("Migrations applied successfully")
	return nil
}

func runMigration(db *sql.DB, migrationFile string) error {
	migration, err := ioutil.ReadFile(migrationFile)
	if err != nil {
		return fmt.Errorf("failed to read migration file: %w", err)
	}

	_, err = db.Exec(string(migration))
	if err != nil {
		return fmt.Errorf("failed to execute migration: %w", err)
	}

	return nil
}

func benchmark(ctx context.Context, pgPool *pgxpool.Pool, dynamoClient *dynamodb.Client) {
	resultsPgInsert := make(chan time.Duration, numRecords)
	resultsPgGet := make(chan time.Duration, numRecords)
	resultsDynamoInsert := make(chan time.Duration, numRecords)
	resultsDynamoGet := make(chan time.Duration, numRecords)

	pgInsertBar := pb.New(numRecords).Start()
	pgGetBar := pb.New(numRecords).Start()
	dynamoInsertBar := pb.New(numRecords).Start()
	dynamoGetBar := pb.New(numRecords).Start()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		postgresInsertBenchmark(ctx, pgPool, resultsPgInsert, pgInsertBar)
		postgresGetBenchmark(ctx, pgPool, resultsPgGet, pgGetBar)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		dynamoInsertBenchmark(ctx, dynamoClient, resultsDynamoInsert, dynamoInsertBar)
		dynamoGetBenchmark(ctx, dynamoClient, resultsDynamoGet, dynamoGetBar)
	}()
	wg.Wait()

	close(resultsPgInsert)
	close(resultsPgGet)
	close(resultsDynamoInsert)
	close(resultsDynamoGet)

	pgInsertBar.Finish()
	pgGetBar.Finish()
	dynamoInsertBar.Finish()
	dynamoGetBar.Finish()

	calculateResults("PostgreSQL Insert", resultsPgInsert)
	calculateResults("PostgreSQL Get", resultsPgGet)
	calculateResults("DynamoDB Insert", resultsDynamoInsert)
	calculateResults("DynamoDB Get", resultsDynamoGet)
}

func calculateResults(operation string, results <-chan time.Duration) {
	var totalDuration time.Duration
	var count int

	for res := range results {
		totalDuration += res
		count++
	}

	averageDuration := totalDuration / time.Duration(count)

	fmt.Printf("\n%s Total operations: %d\n", operation, count)
	fmt.Printf("%s Total time: %s\n", operation, formatDuration(totalDuration))
	fmt.Printf("%s Average time per operation: %s\n", operation, formatDuration(averageDuration))
}

func formatDuration(d time.Duration) string {
	hours := d / time.Hour
	d -= hours * time.Hour
	minutes := d / time.Minute
	d -= minutes * time.Minute
	seconds := d / time.Second
	d -= seconds * time.Second
	milliseconds := d / time.Millisecond
	microseconds := d / time.Microsecond

	if hours > 0 {
		return fmt.Sprintf("%dh %dm %ds %dms", hours, minutes, seconds, milliseconds)
	} else if minutes > 0 {
		return fmt.Sprintf("%dm %ds %dms", minutes, seconds, milliseconds)
	} else if seconds > 0 {
		return fmt.Sprintf("%ds %dms %dµs", seconds, milliseconds, microseconds)
	}
	return fmt.Sprintf("%dms %dµs", milliseconds, microseconds)
}

func postgresInsertBenchmark(ctx context.Context, pool *pgxpool.Pool, results chan<- time.Duration, bar *pb.ProgressBar) {
	for i := 0; i < numRecords; i++ {
		start := time.Now()
		id := uuid.New().String()
		mu.Lock()
		uuids = append(uuids, id)
		mu.Unlock()

		tx, err := pool.Begin(ctx)
		if err != nil {
			log.Printf("Postgres Transaction Begin Error: %v", err)
			continue
		}

		_, err = tx.Exec(ctx, "INSERT INTO test_table (id, value) VALUES ($1, $2)", id, fmt.Sprintf("value%d", i))
		if err != nil {
			log.Printf("Postgres Insert Error: %v", err)
			tx.Rollback(ctx)
			continue
		}

		relatedID := uuid.New().String()
		_, err = tx.Exec(ctx, "INSERT INTO related_table (id, test_id, related_value) VALUES ($1, $2, $3)", relatedID, id, fmt.Sprintf("related_value%d", i))
		if err != nil {
			log.Printf("Postgres Related Insert Error: %v", err)
			tx.Rollback(ctx)
			continue
		}

		anotherRelatedID := uuid.New().String()
		_, err = tx.Exec(ctx, "INSERT INTO another_related_table (id, related_id, another_value) VALUES ($1, $2, $3)", anotherRelatedID, relatedID, fmt.Sprintf("another_value%d", i))
		if err != nil {
			log.Printf("Postgres Another Related Insert Error: %v", err)
			tx.Rollback(ctx)
			continue
		}

		err = tx.Commit(ctx)
		if err != nil {
			log.Printf("Postgres Transaction Commit Error: %v", err)
			continue
		}

		duration := time.Since(start)
		results <- duration
		bar.Increment()
	}
}

func dynamoInsertBenchmark(ctx context.Context, client *dynamodb.Client, results chan<- time.Duration, bar *pb.ProgressBar) {
	for i := 0; i < numRecords; i++ {
		start := time.Now()
		id := uuid.New().String()
		mu.Lock()
		uuidsD = append(uuidsD, id)
		mu.Unlock()

		relatedID := uuid.New().String()
		anotherRelatedID := uuid.New().String()

		item := map[string]types.AttributeValue{
			"Id":    &types.AttributeValueMemberS{Value: id},
			"Value": &types.AttributeValueMemberS{Value: fmt.Sprintf("value%d", i)},
			"Related": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
				"RelatedId":    &types.AttributeValueMemberS{Value: relatedID},
				"RelatedValue": &types.AttributeValueMemberS{Value: fmt.Sprintf("related_value%d", i)},
				"AnotherRelated": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
					"AnotherRelatedId": &types.AttributeValueMemberS{Value: anotherRelatedID},
					"AnotherValue":     &types.AttributeValueMemberS{Value: fmt.Sprintf("another_value%d", i)},
				}},
			}},
		}

		_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(dynamoDBTable),
			Item:      item,
		})
		duration := time.Since(start)
		results <- duration
		bar.Increment()

		if err != nil {
			log.Printf("DynamoDB Insert Error: %v", err)
		}
	}
}

func postgresGetBenchmark(ctx context.Context, pool *pgxpool.Pool, results chan<- time.Duration, bar *pb.ProgressBar) {
	mu.Lock()
	defer mu.Unlock()
	for _, id := range uuids {
		start := time.Now()
		var value, relatedValue, anotherValue string
		err := pool.QueryRow(ctx, `
			SELECT 
				t.value, 
				r.related_value, 
				ar.another_value 
			FROM 
				test_table t 
			JOIN 
				related_table r ON t.id = r.test_id 
			JOIN 
				another_related_table ar ON r.id = ar.related_id 
			WHERE 
				t.id = $1`, id).Scan(&value, &relatedValue, &anotherValue)
		duration := time.Since(start)
		results <- duration
		bar.Increment()

		if err == sql.ErrNoRows {
			log.Printf("Postgres Get Error: no rows found for id %s", id)
		} else if err != nil {
			log.Printf("Postgres Get Error: %v", err)
		}
	}
}

func dynamoGetBenchmark(ctx context.Context, client *dynamodb.Client, results chan<- time.Duration, bar *pb.ProgressBar) {
	mu.Lock()
	defer mu.Unlock()
	for _, id := range uuidsD {
		start := time.Now()
		resp, err := client.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: aws.String(dynamoDBTable),
			Key: map[string]types.AttributeValue{
				"Id": &types.AttributeValueMemberS{Value: id},
			},
		})
		duration := time.Since(start)
		results <- duration
		bar.Increment()

		if err != nil {
			log.Printf("DynamoDB Get Error: %v", err)
		} else if resp.Item == nil {
			log.Printf("DynamoDB Get Error: no item found for id %s", id)
		}
	}
}

/*func DynamoBestScenario() {
	ctx := context.Background()

	// PostgreSQL setup
	connString := ""
	pgPool, err := pgxpool.Connect(ctx, connString)
	if err != nil {
		log.Fatalf("Unable to connect to PostgreSQL: %v", err)
	}
	defer pgPool.Close()
	postgresCleanUp(ctx, pgPool)

	// DynamoDB setup
	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion("us-east-1"),
		config.WithEndpointResolver(aws.EndpointResolverFunc(
			func(service, region string) (aws.Endpoint, error) {
				return aws.Endpoint{URL: "http://localhost:4566"}, nil
			}),
		),
	)
	if err != nil {
		log.Fatalf("Unable to load AWS SDK config: %v", err)
	}

	dynamoClient := dynamodb.NewFromConfig(awsCfg)

	// Run benchmarks
	fmt.Println("Starting DynamoDB benchmark...")
	dynamoStart := time.Now()
	dynamoBenchmark(ctx, dynamoClient)
	dynamoDuration := time.Since(dynamoStart)

	fmt.Println("Starting PostgreSQL benchmark...")
	postgresStart := time.Now()
	postgresBenchmark(ctx, pgPool)
	postgresDuration := time.Since(postgresStart)

	// Print results
	fmt.Printf("DynamoDB Write Benchmark: %s\n", dynamoDuration)
	fmt.Printf("PostgreSQL Write Benchmark: %s\n", postgresDuration)
}*/

func dynamoBenchmark(ctx context.Context, client *dynamodb.Client) {
	var wg sync.WaitGroup
	wg.Add(numRecordsd)

	for i := 0; i < numRecordsd; i++ {
		go func(i int) {
			defer wg.Done()
			userId := uuid.New().String()
			timestamp := time.Now().Unix()

			_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
				TableName: aws.String(dynamoDBTabled),
				Item: map[string]types.AttributeValue{
					"UserId":    &types.AttributeValueMemberS{Value: userId},
					"Timestamp": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", timestamp)},
					"Score":     &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", i)},
				},
			})
			if err != nil {
				log.Printf("DynamoDB Insert Error: %v", err)
			}
		}(i)
	}

	wg.Wait()
	fmt.Println("DynamoDB writes completed")
}

func postgresBenchmark(ctx context.Context, pool *pgxpool.Pool) {
	var wg sync.WaitGroup
	wg.Add(numRecordsd)

	for i := 0; i < numRecordsd; i++ {
		go func(i int) {
			defer wg.Done()
			userId := uuid.New().String()
			timestamp := time.Now().Unix()

			_, err := pool.Exec(ctx, "INSERT INTO leaderboard (userid, timestamp, score) VALUES ($1, $2, $3)", userId, timestamp, i)
			if err != nil {
				log.Printf("PostgreSQL Insert Error: %v", err)
			}
		}(i)
	}

	wg.Wait()
	fmt.Println("PostgreSQL writes completed")
}

func postgresCleanUp(ctx context.Context, pool *pgxpool.Pool) {
	_, err := pool.Exec(ctx, "DROP TABLE IF EXISTS leaderboard; CREATE TABLE leaderboard (userid TEXT, timestamp BIGINT, score INT, PRIMARY KEY (userid, timestamp));")
	if err != nil {
		log.Fatalf("Error setting up PostgreSQL table: %v", err)
	}
}
