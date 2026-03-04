package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"time"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

// Demo测试开关：设为 false 时只打印控制台，不推 Redis
var EnableASRRedisPush = false

type FunASRResult struct {
	Text    string `json:"text"`
	IsFinal bool   `json:"is_final"`
}

// StartASRProcess 处理音频流并连接到 FunASR
func StartASRProcess(ctx context.Context, userID string, audioReader io.Reader) {
	// 1. 连接到 FunASR WebSocket 服务
	wsURL := "ws://192.168.1.76:10095"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		log.Printf("[ASR Error] User %s: 连接 FunASR 失败: %v", userID, err)
		return
	}
	defer conn.Close()

	// 2. 发送初始配置帧 (FunASR 2-pass 模式要求)
	initMsg := map[string]interface{}{
		"mode":           "2pass",
		"chunk_size":     []int{5, 10, 5},
		"chunk_interval": 10,
		"wav_name":       "rtsp_stream",
		"is_speaking":    true,
	}
	conn.WriteJSON(initMsg)

	// 3. 开启协程：接收识别结果
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				_, message, err := conn.ReadMessage()
				if err != nil {
					log.Printf("[ASR Error] User %s: 读取消息失败: %v", userID, err)
					log.Printf("[ASR Info] User %s: 识别连接已断开", userID)
					return
				}

				var result FunASRResult
				if err := json.Unmarshal(message, &result); err == nil && result.Text != "" {
					// 打印到控制台
					log.Printf("[ASR 实时] User: %s | IsFinal: %v | Text: %s", userID, result.IsFinal, result.Text)

					// 如果开关开启，则推送 Redis
					if EnableASRRedisPush {
						pushToASRStream(userID, result)
					}
				}
			}
		}
	}()

	// 4. 主循环：读取 FFmpeg 的 PCM 流并发送二进制音频数据
	buf := make([]byte, 3200) // 每次读取 100ms 的 16k 16bit 单声道音频
	for {
		select {
		case <-ctx.Done():
			return
		default:
			n, err := audioReader.Read(buf)
			if n > 0 {
				err = conn.WriteMessage(websocket.BinaryMessage, buf[:n])
				if err != nil {
					return
				}
			}
			if err != nil {
				return // 流结束或出错
			}
		}
	}
}

// pushToASRStream 推送识别结果到 Redis Stream
func pushToASRStream(userID string, result FunASRResult) {
	err := rdb.XAdd(ctxRedis, &redis.XAddArgs{
		Stream: "serene:asr_stream",
		MaxLen: 100,
		Approx: true,
		Values: map[string]interface{}{
			"userId":    userID,
			"text":      result.Text,
			"is_final":  result.IsFinal, // 前端可以通过这个判断这句话是不是说完了
			"timestamp": time.Now().UnixMilli(),
		},
	}).Err()

	if err != nil {
		log.Printf("[ASR Redis Error] 推送失败: %v", err)
	}
}
