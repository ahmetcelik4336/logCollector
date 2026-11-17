package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

type LogMessage struct {
	ClientAddr string
	Content    string
	Timestamp  string
}

type SystemMetrics struct {
	TotalLogsProcessed uint64 `json:"total_logs_processed"`
	ActiveWorkers      int    `json:"active_workers"`
}

var (
	metrics SystemMetrics
	fileMu  sync.Mutex // Dosyaya aynı anda yazılmasını engellemek için kilit (Mutex)
)

func main() {
	logQueue := make(chan LogMessage, 1000)

	var wg sync.WaitGroup

	workerCount := 2
	metrics.ActiveWorkers = workerCount

	for i := 1; i <= workerCount; i++ {
		wg.Add(1)
		go batchWorker(i, logQueue, &wg)
	}

	go startTCPServer(logQueue)
	go startHTTPServer()

	fmt.Println("🚀 Günlük log kaydetme özellikli sistem çalışıyor! TCP: 8080 | HTTP: 8081")

	select {}
}

func startTCPServer(logQueue chan<- LogMessage) {
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("TCP Sunucusu başlatılamadı: %v", err)
	}
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Bağlantı kabul hatası: %v", err)
			continue
		}
		go handleConnection(conn, logQueue)
	}
}

func handleConnection(conn net.Conn, logQueue chan<- LogMessage) {
	defer conn.Close()
	clientAddr := conn.RemoteAddr().String()

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		// Log geldiği anın zaman damgasını (timestamp) ekliyoruz
		logQueue <- LogMessage{
			ClientAddr: clientAddr,
			Content:    scanner.Text(),
			Timestamp:  time.Now().Format("2006-01-02 15:04:05"),
		}
	}
}

func batchWorker(id int, logQueue <-chan LogMessage, wg *sync.WaitGroup) {
	defer wg.Done()

	batchSize := 5
	flushInterval := 3 * time.Second

	batch := make([]LogMessage, 0, batchSize)
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	for {
		select {
		case logMsg, ok := <-logQueue:
			if !ok {
				return
			}
			batch = append(batch, logMsg)

			if len(batch) >= batchSize {
				flushBatchToDisk(batch)
				atomic.AddUint64(&metrics.TotalLogsProcessed, uint64(len(batch)))
				batch = make([]LogMessage, 0, batchSize)
			}

		case <-ticker.C:
			if len(batch) > 0 {
				flushBatchToDisk(batch)
				atomic.AddUint64(&metrics.TotalLogsProcessed, uint64(len(batch)))
				batch = make([]LogMessage, 0, batchSize)
			}
		}
	}
}

// Logları günlük dosyalara yazan fonksiyon
func flushBatchToDisk(batch []LogMessage) {
	// Aynı anda birden fazla worker dosyaya yazmaya çalışmasın diye kilidi açıyoruz
	fileMu.Lock()
	defer fileMu.Unlock()

	// "logs" adında bir klasör oluşturalım
	dirName := "logs"
	if err := os.MkdirAll(dirName, 0755); err != nil {
		log.Printf("Log klasörü oluşturulamadı: %v", err)
		return
	}

	// Dosya adını bugünün tarihine göre ayarlıyoruz (Örn: logs/2026-06-07.log)
	today := time.Now().Format("2006-01-02")
	fileName := filepath.Join(dirName, today+".log")

	// Dosyayı ekleme (append) modunda açıyoruz, yoksa oluşturuyoruz
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Printf("Log dosyası açılamadı: %v", err)
		return
	}
	defer file.Close()

	// Her bir logu satır satır dosyaya yazıyoruz
	for _, l := range batch {
		logLine := fmt.Sprintf("[%s] [%s] %s\n", l.Timestamp, l.ClientAddr, l.Content)
		if _, err := file.WriteString(logLine); err != nil {
			log.Printf("Dosyaya yazılamadı: %v", err)
		}
	}

	fmt.Printf("💾 %d adet log dosyaya kaydedildi -> %s\n", len(batch), fileName)
}

func startHTTPServer() {
	http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(metrics)
	})

	if err := http.ListenAndServe(":8081", nil); err != nil {
		log.Fatalf("HTTP Sunucusu başlatılamadı: %v", err)
	}
}
