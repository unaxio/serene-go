package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"math"
	"time"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

// Demo测试开关：设为 false 时只打印控制台，不推 Redis
var EnableASRRedisPush = true

type FunASRResult struct {
	Text    string `json:"text"`
	IsFinal bool   `json:"is_final"`
	Mode    string `json:"mode"`
}

// SimpleVAD 简易语音活动检测器
type SimpleVAD struct {
	EnergyThreshold float64 // 能量阈值 (推荐 200-500)
	MinZCR          float64 // 最小过零率 (过滤低频隆隆声，推荐 0.02)
	MaxZCR          float64 // 最大过零率 (过滤高频电流嘶嘶声，推荐 0.5)
}

// NewSimpleVAD 初始化 VAD
func NewSimpleVAD() *SimpleVAD {
	return &SimpleVAD{
		// 16bit 音频最大值是 32768。
		// 正常人说话能量通常在 1000 以上。
		// 外放残留的微弱回声通常在 100-300 左右。
		EnergyThreshold: 200.0,
		MinZCR:          0.02,
		MaxZCR:          0.50,
	}
}

// bytesToInt16 将 FFmpeg 吐出的小端序 PCM 字节流转换为 16 位整数切片
func bytesToInt16(buf []byte) []int16 {
	res := make([]int16, len(buf)/2)
	for i := 0; i < len(buf)/2; i++ {
		res[i] = int16(buf[i*2]) | (int16(buf[i*2+1]) << 8)
	}
	return res
}

// IsSpeech 判断当前音频帧是否包含人声
func (v *SimpleVAD) IsSpeech(pcmData []int16) bool {
	if len(pcmData) == 0 {
		return false
	}

	var sumEnergy float64
	crossings := 0

	for i := 0; i < len(pcmData); i++ {
		// 1. 累计能量平方
		sample := float64(pcmData[i])
		sumEnergy += sample * sample

		// 2. 计算过零率 (Zero-Crossing)
		if i > 0 {
			if (pcmData[i] >= 0 && pcmData[i-1] < 0) || (pcmData[i] < 0 && pcmData[i-1] >= 0) {
				crossings++
			}
		}
	}

	// 计算 RMS (均方根能量)
	rms := math.Sqrt(sumEnergy / float64(len(pcmData)))
	// 计算 ZCR (过零率)
	zcr := float64(crossings) / float64(len(pcmData))

	// 判断逻辑：能量必须大于阈值，且频率特征必须在人声范围内
	if rms > v.EnergyThreshold && zcr > v.MinZCR && zcr < v.MaxZCR {
		return true
	}

	return false
}

// StartASRProcess 处理音频流并连接到 FunASR
func StartASRProcess(wsURL string, ctx context.Context, userID string, audioReader io.Reader) {
	// 1. 连接到 FunASR WebSocket 服务
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
		"vad_kws": map[string]interface{}{
			"speech_noise_thres":       0.8,
			"sil_to_speech_time_thres": 300,
			"speech_to_sil_time_thres": 150,
		},
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
					return
				}

				var result FunASRResult
				if err := json.Unmarshal(message, &result); err == nil && result.Text != "" {
					// 打印到控制台
					if result.Mode == "2pass-online" {
						log.Printf("[ASR Debug] User %s: %v 2pass-online %s", userID, result.IsFinal, result.Text)
					}
					if result.Mode == "2pass-offline" {
						log.Printf("[ASR Debug] User %s: %v 2pass-offline %s", userID, result.IsFinal, result.Text)
					}

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

	// 初始化我们的纯 Go VAD 引擎
	vadEngine := NewSimpleVAD()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			n, err := audioReader.Read(buf)
			if n > 0 {
				// 获取有效数据切片
				validBuf := buf[:n]

				// 将字节流转为 int16 进行计算
				pcmData := bytesToInt16(validBuf)

				// 核心：让 VAD 判断这 100ms 的声音是不是真人说话
				if vadEngine.IsSpeech(pcmData) {
					// 是真声音，发给 FunASR
					err = conn.WriteMessage(websocket.BinaryMessage, validBuf)
				} else {
					// 是微弱的回声/环境噪音，发一段同等长度的纯静音给 FunASR，维持心跳和时间戳
					silentBuf := make([]byte, n)
					err = conn.WriteMessage(websocket.BinaryMessage, silentBuf)
					// log.Println("[VAD] 拦截了一段回声残余或噪音") // 调试时可以打开
				}

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
			"mode":      result.Mode,
			"text":      result.Text,
			"is_final":  result.IsFinal, // 前端可以通过这个判断这句话是不是说完了
			"timestamp": time.Now().UnixMilli(),
		},
	}).Err()

	if err != nil {
		log.Printf("[ASR Redis Error] 推送失败: %v", err)
	}
}
