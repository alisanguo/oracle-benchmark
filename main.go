package main

import (
	"bufio"
	"context"
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/godror/godror"
	"log"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"
)

const driverName = "godror"

const selectSqlPattern = `(?i)^select\s+`

var selectSqlReg = regexp.MustCompile(selectSqlPattern)

func main() {
	// 定义命令行参数
	user := flag.String("user", "", "Oracle database user")
	password := flag.String("password", "", "Oracle database password")
	host := flag.String("host", "", "Oracle database host")
	port := flag.Int("port", 0, "Oracle database port")
	schema := flag.String("schema", "", "Oracle database schema")
	concurrency := flag.Int("concurrency", 1, "Number of concurrent workers")
	iterations := flag.Int("iterations", 1, "Number of query iterations per worker")
	sqlFile := flag.String("sql_file", "", "Path to the SQL file")
	flag.Parse()

	// 检查必需的参数
	if *user == "" || *password == "" || *host == "" || *port == 0 || *schema == "" || *sqlFile == "" {
		log.Fatal("Data source and SQL file must be provided")
	}

	// 打开文件
	file, err := os.Open(*sqlFile)
	if err != nil {
		log.Fatal("Failed to open SQL file:", err)
	}
	defer file.Close()

	// 创建数据库连接池
	dataSource := fmt.Sprintf(`user="%v" password="%v" connectString="%v:%v/%v"`, *user, *password, *host, *port, *schema)
	db, err := sql.Open(driverName, dataSource)
	if err != nil {
		log.Fatal("Failed to connect to the database:", err)
	}
	db.SetMaxOpenConns(*concurrency * 2)
	db.SetMaxIdleConns(*concurrency)
	db.SetConnMaxIdleTime(time.Minute)
	defer db.Close()

	// 测试连接
	if err := db.Ping(); err != nil {
		log.Fatal("Failed to ping the database:", err)
	}

	// 创建等待组以等待所有工作程序完成
	var wg sync.WaitGroup
	wg.Add(*concurrency)

	// 创建通道来接收要处理的批处理
	batchCh := make(chan []string, *concurrency*2)

	// 创建上下文和取消函数
	ctx, cancel := context.WithCancel(context.Background())

	// 创建一个通道来接收信号
	signalCh := make(chan os.Signal, 1)

	// 通过signal.Notify注册要接收的信号
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)

	// 启动一个goroutine等待信号
	go func() {
		// 通过信号通道接收信号
		sig := <-signalCh
		log.Println("Received exit signal:", sig)
		cancel()
	}()

	// 启动并发的工作程序
	for i := 0; i < *concurrency; i++ {
		go runQueries(ctx, &wg, db, batchCh, *iterations, i)
	}

	time.Sleep(time.Second * 2)

	go func(ctx context.Context) {
		// 逐行读取和处理SQL语句
		scanner := bufio.NewScanner(file)
		batch := make([]string, 0)
		for scanner.Scan() {
			select {
			case <-ctx.Done():
				log.Println("SQL program canceled")
				// 关闭通道，通知工作程序所有批处理已经发送完毕
				break
			default:
				line := scanner.Text()
				if line != "" {
					batch = append(batch, line)

					// 当遇到换行符加分号时，处理当前批次的SQL语句
					if strings.HasSuffix(line, ";") {
						// 将当前批次的SQL语句发送到通道中
						batchCh <- batch
						batch = make([]string, 0)
					}
				}
			}
		}

		// 关闭通道，通知工作程序所有批处理已经发送完毕
		log.Println("finish read file...")
		close(batchCh)

	}(ctx)

	// 等待所有工作程序完成
	wg.Wait()

	log.Println("All workers completed")
}

func runQueries(ctx context.Context, wg *sync.WaitGroup, db *sql.DB, batchCh <-chan []string, iterations, index int) {
	defer wg.Done()
	select {
	case <-ctx.Done():
		log.Printf("SQL program canceled, work %v exit\n", index)
		return
	default:
		// 从通道中接收要处理的批处理
	FOR:
		for batch := range batchCh {
			// 执行查询
			for i := 0; i < iterations; i++ {
				select {
				case <-ctx.Done():
					log.Printf("SQL program canceled, work %v exit\n", index)
					break FOR
				default:
					// 处理批处理
					processBatch(db, batch)
				}
			}
		}
	}
}

func processBatch(db *sql.DB, batch []string) {
	// 拼接批处理的SQL语句
	sqlStatements := strings.Join(batch, " ")
	sqlStatements = strings.Trim(sqlStatements, ";")

	// 执行查询
	log.Printf("exec sql:%s", sqlStatements)
	if selectSqlReg.MatchString(sqlStatements) {
		_, err := db.Query(sqlStatements)
		if err != nil {
			log.Println("Failed to execute sql:", err)
		}
	} else {
		_, err := db.Exec(sqlStatements)
		if err != nil {
			log.Println("Failed to execute sql:", err)
		}
	}

}
