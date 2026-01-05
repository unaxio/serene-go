package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"mime/multipart"
	"net/http"
	"os/exec"
	"runtime"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// EmotionResponse 结构体保持不变
type EmotionResponse struct {
	Code             int            `json:"code"`
	Msg              string         `json:"msg"`
	EmotionSign      string         `json:"emotionSign"`
	EmotionFlag      int            `json:"emotionFlag"`
	EmotionValue     int            `json:"emotionValue"`
	FeelingValue     int            `json:"feelingValue"`
	EmotionValueList map[string]int `json:"emotionValueList"`
}

type WorkerInfo struct {
	StartTime time.Time `json:"startTime"`
	UserID    string    `json:"userId"`
}

type StreamManager struct {
	workers    map[string]context.CancelFunc
	workerMeta map[string]WorkerInfo // 记录每个任务的元数据
	mu         sync.Mutex
	useHW      bool // 全局硬件加速开关
}

func NewStreamManager() *StreamManager {
	return &StreamManager{
		workers:    make(map[string]context.CancelFunc),
		workerMeta: make(map[string]WorkerInfo),
		useHW:      true, // 默认开启加速
	}
}

var (
	rdb      *redis.Client
	ctxRedis = context.Background()
)

// 在 main 之前初始化 Redis
func initRedis() {
	rdb = redis.NewClient(&redis.Options{
		Addr: "localhost:6379", // 你的 Redis 地址
		DB:   0,
	})
}

// 自定义 JPEG 切分器：在二进制流中寻找 JPEG 的边界 (FF D8 ... FF D9)
func splitJPEG(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	start := bytes.Index(data, []byte{0xff, 0xd8})
	if start == -1 {
		return 0, nil, nil
	}
	end := bytes.Index(data[start:], []byte{0xff, 0xd9})
	if end == -1 {
		return 0, nil, nil
	}
	totalLen := start + end + 2
	return totalLen, data[start:totalLen], nil
}

func pushToEmotionStream(userID string, result *EmotionResponse) error {
	listBytes, _ := json.Marshal(result.EmotionValueList)
	return rdb.XAdd(ctxRedis, &redis.XAddArgs{
		Stream: "serene:emotion_stream",
		MaxLen: 100,
		Approx: true,
		Values: map[string]interface{}{
			"userId":    userID,
			"sign":      result.EmotionSign,
			"value":     result.EmotionValue,
			"list":      string(listBytes),
			"feeling":   result.FeelingValue,
			"timestamp": time.Now().Unix(),
		},
	}).Err()
}

// 调用表情识别接口，直接传入内存中的 []byte
func callEmotionService(imageData []byte) (*EmotionResponse, error) {
	url := "http://localhost:9400/emotion_upload"

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	// 这里直接从内存创建文件流
	part, err := writer.CreateFormFile("file", "frame.jpg")
	if err != nil {
		return nil, err
	}
	part.Write(imageData)
	writer.Close()

	req, _ := http.NewRequest("POST", url, body)
	req.Header.Set("Content-Type", writer.FormDataContentType())

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result EmotionResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (m *StreamManager) runPipeline(ctx context.Context, userID string) {
	// 启动时记录元数据
	m.mu.Lock()
	m.workerMeta[userID] = WorkerInfo{StartTime: time.Now(), UserID: userID}
	m.mu.Unlock()

	defer func() {
		m.mu.Lock()
		delete(m.workers, userID)
		m.mu.Unlock()
		log.Printf("[Cleanup] Worker for user %s has exited safely", userID)
	}()

	rtspURL := fmt.Sprintf("rtsp://localhost:8554/%s", userID)

	// 不进行for循环自动重启，加个if保持缩进防止git修改太多
	if true {
		var args []string

		// --- 硬件加速逻辑分发 ---
		if m.useHW {
			if runtime.GOOS == "darwin" { // Mac
				args = []string{"-hwaccel", "videotoolbox", "-rtsp_transport", "tcp", "-i", rtspURL, "-f", "image2pipe", "-vcodec", "mjpeg", "-q:v", "2", "pipe:1"}
			} else { // Linux (Nvidia)
				args = []string{"-hwaccel", "cuda", "-hwaccel_output_format", "cuda", "-rtsp_transport", "tcp", "-i", rtspURL, "-vf", "fps=1/3,scale_npp=640:480", "-f", "image2pipe", "-vcodec", "mjpeg", "-q:v", "2", "pipe:1"}
			}
		} else { // 纯 CPU 模式
			args = []string{"-rtsp_transport", "tcp", "-i", rtspURL, "-vf", "fps=1/3,scale=640:480", "-f", "image2pipe", "-vcodec", "mjpeg", "pipe:1"}
		}

		log.Printf("[Connecting] Stream: %s", rtspURL)

		cmd := exec.CommandContext(ctx, "ffmpeg", args...)

		stdout, err := cmd.StdoutPipe()
		if err != nil {
			log.Printf("[Error] Failed to create pipe: %v", err)
			return
		}

		if err := cmd.Start(); err != nil {
			log.Printf("[Error] Failed to start FFmpeg: %v", err)
			return
		}

		// 使用 Scanner 高效扫描输出流
		scanner := bufio.NewScanner(stdout)
		// 设置 Scanner 缓冲区最大为 1MB (防止大图片)
		buf := make([]byte, 0, 64*1024)
		scanner.Buffer(buf, 1*1024*1024)
		scanner.Split(splitJPEG)

		var lastUploadTime time.Time

		for scanner.Scan() {
			// 关键：立即检查时间，不符合条件直接丢弃，不进行任何内存拷贝或异步操作
			now := time.Now()
			if !lastUploadTime.IsZero() && now.Sub(lastUploadTime) < 3*time.Second {
				continue
			}

			// 只有过了 5 秒（或者是第一帧），才执行下面的逻辑
			lastUploadTime = now

			imgData := scanner.Bytes()

			// 异步调用接口，不阻塞拉流主进程
			go func(data []byte) {
				start := time.Now()
				result, err := callEmotionService(data)
				if err != nil {
					log.Printf("[Error] Analysis failed: %v", err)
					return
				}
				if result.Code != 0 {
					log.Printf("[Fail] Time:%v | Msg:%s | Code:%d",
						time.Since(start), result.Msg, result.Code)
					return
				}
				log.Printf("[Success] Time:%v | Sign:%s | Value:%d",
					time.Since(start), result.EmotionSign, result.EmotionValue)

				err = pushToEmotionStream(userID, result)
				if err != nil {
					log.Printf("[Error] Push to Redis failed: %v", err)
				}
			}(imgData)
		}

		if err := scanner.Err(); err != nil {
			log.Printf("[Error] Stream reading interrupted: %v", err)
		}

		cmd.Wait()

		// 检查循环退出是因为被外部杀掉，还是因为流断了
		select {
		case <-ctx.Done():
			log.Printf("[Stop] Received external instruction to stop user %s", userID)
			return
		default:
			log.Printf("[Break] User %s stream interrupted", userID)
			return
		}
	}
}

// StartTask 开启一个新任务
func (m *StreamManager) StartTask(userID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 1. 关键改动：如果用户已经在运行，直接忽略，不要 Reset
	if _, exists := m.workers[userID]; exists {
		log.Printf("[忽略] 用户 %s 任务已在运行，不再重复创建", userID)
		return
	}

	// 2. 创建一个新的可取消的 Context
	ctx, cancel := context.WithCancel(context.Background())
	m.workers[userID] = cancel

	// 3. 异步启动处理管道
	go m.runPipeline(ctx, userID)
	log.Printf("[Created] User %s", userID)
}

// StopTask 停止一个指定任务
func (m *StreamManager) StopTask(userID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 找到该用户的 cancel 函数并执行
	if cancel, exists := m.workers[userID]; exists {
		cancel()
		delete(m.workers, userID)
		log.Printf("[已销毁] 用户 %s 的处理通道已主动关闭", userID)
	} else {
		log.Printf("[忽略] 用户 %s 任务不存在或已关闭", userID)
	}
}

// 定义 MediaMTX 的 Path 结构 (简化版)
type MediaMTXPaths struct {
	Items []struct {
		Name  string `json:"name"`
		Ready bool   `json:"ready"`
	} `json:"items"`
}

// 新增初始化同步函数
func (m *StreamManager) SyncFromMediaMTX() {
	resp, err := http.Get("http://localhost:9997/v3/paths/list")
	if err != nil {
		log.Printf("[Init] 无法连接 MediaMTX API: %v", err)
		return
	}
	defer resp.Body.Close()

	var data MediaMTXPaths
	json.NewDecoder(resp.Body).Decode(&data)

	for _, item := range data.Items {
		if item.Ready && item.Name != "" {
			log.Printf("[Init] 发现存量流: %s", item.Name)
			m.StartTask(item.Name)
		}
	}
}

func main() {
	initRedis()
	defer rdb.Close()

	// 1. 初始化管理器
	manager := NewStreamManager()
	manager.SyncFromMediaMTX()

	// 2. 路由：MediaMTX 准备好流时调用
	http.HandleFunc("/internal/on_ready", func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Query().Get("id")
		if id == "" {
			http.Error(w, "missing id", 400)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))

		go func(userID string) {
			manager.StartTask(userID)
		}(id)
	})

	// 3. 路由：MediaMTX 断流时或 Nest.js 红牌清理时调用
	http.HandleFunc("/internal/on_stop", func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Query().Get("id")
		if id == "" {
			http.Error(w, "missing id", 400)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
		go func(userID string) {
			manager.StopTask(id)
		}(id)
	})

	// 4. 获取当前所有任务状态
	http.HandleFunc("/internal/status", func(w http.ResponseWriter, r *http.Request) {
		manager.mu.Lock()
		defer manager.mu.Unlock()

		status := map[string]interface{}{
			"count":            len(manager.workerMeta),
			"hardware_enabled": manager.useHW,
			"tasks":            manager.workerMeta,
			"os":               runtime.GOOS,
		}
		json.NewEncoder(w).Encode(status)
	})

	// 5. 动态切换硬件加速开关 (POST /internal/toggle_hw?enabled=true)
	http.HandleFunc("/internal/toggle_hw", func(w http.ResponseWriter, r *http.Request) {
		enabled := r.URL.Query().Get("enabled") == "true"
		manager.mu.Lock()
		manager.useHW = enabled
		manager.mu.Unlock()
		fmt.Fprintf(w, "Hardware acceleration set to: %v", enabled)
	})

	log.Println(">>> Golang 动态分析服务已启动 <<<")
	log.Println("监听地址: :18080")

	if err := http.ListenAndServe(":18080", nil); err != nil {
		log.Fatal(err)
	}
}
