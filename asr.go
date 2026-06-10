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

// SmartVAD 带状态机与防切头去尾的智能 VAD
type SmartVAD struct {
	EnergyThreshold float64
	MinZCR          float64
	MaxZCR          float64

	// 状态机核心参数
	PreRollFrames  int // 预录制延迟帧数 (防切头)
	HangoverFrames int // 滞留维持帧数 (防去尾)

	// 内部状态
	isSpeaking    bool
	hangoverCount int
	delayBuffer   [][]byte
}

// NewSmartVAD 初始化 VAD
func NewSmartVAD() *SmartVAD {
	return &SmartVAD{
		EnergyThreshold: 400.0,
		MinZCR:          0.01,
		MaxZCR:          0.50,
		// 你的 buf 是 3200 字节，在 16kHz 16bit 单声道下刚好是 100ms / 帧
		PreRollFrames:  3, // 延迟 300ms 输出，完美保留起音
		HangoverFrames: 6, // 停顿 600ms 以内都算连续说话
	}
}

func bytesToInt16(buf []byte) []int16 {
	res := make([]int16, len(buf)/2)
	for i := 0; i < len(buf)/2; i++ {
		res[i] = int16(buf[i*2]) | (int16(buf[i*2+1]) << 8)
	}
	return res
}

// 内部方法：仅判断当前物理帧是否有声音
func (v *SmartVAD) isFrameActive(pcmData []int16) bool {
	if len(pcmData) == 0 {
		return false
	}
	var sumEnergy float64
	crossings := 0

	for i := 0; i < len(pcmData); i++ {
		sample := float64(pcmData[i])
		sumEnergy += sample * sample
		if i > 0 {
			if (pcmData[i] >= 0 && pcmData[i-1] < 0) || (pcmData[i] < 0 && pcmData[i-1] >= 0) {
				crossings++
			}
		}
	}

	rms := math.Sqrt(sumEnergy / float64(len(pcmData)))
	zcr := float64(crossings) / float64(len(pcmData))

	return rms > v.EnergyThreshold && zcr > v.MinZCR && zcr < v.MaxZCR
}

// Process 核心流水线：进一帧，出一帧 (带状态的洗滤)
func (v *SmartVAD) Process(rawBuf []byte) []byte {
	pcmData := bytesToInt16(rawBuf)
	active := v.isFrameActive(pcmData)

	// 1. 更新状态机
	if active {
		v.isSpeaking = true
		v.hangoverCount = v.HangoverFrames // 只要有声音，就重置滞留期
	} else {
		if v.isSpeaking {
			v.hangoverCount--
			if v.hangoverCount <= 0 {
				v.isSpeaking = false // 彻底撑不住了，断开句子
			}
		}
	}

	// 2. 将当前帧数据深拷贝，推入延迟队列
	bufCopy := make([]byte, len(rawBuf))
	copy(bufCopy, rawBuf)
	v.delayBuffer = append(v.delayBuffer, bufCopy)

	// 3. 队列水池未满前，不往外吐真实数据（用同等长度静音代替，维持 WebSocket 心跳）
	if len(v.delayBuffer) <= v.PreRollFrames {
		return make([]byte, len(rawBuf))
	}

	// 4. 队列满了，取出队首最老的一帧
	oldestBuf := v.delayBuffer[0]
	v.delayBuffer = v.delayBuffer[1:]

	// 5. 根据当前的综合状态，决定吐出真实音频还是静音
	if v.isSpeaking {
		return oldestBuf // 吐出带有 pre-roll 预录制的真实声音
	} else {
		return make([]byte, len(oldestBuf)) // 吐出完美抹平的静音垫
	}
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
	vadEngine := NewSmartVAD()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			n, err := audioReader.Read(buf)
			if n > 0 {
				// 获取有效数据切片
				validBuf := buf[:n]

				// 让 VAD 引擎处理，它会返回该发真实音频还是静音填充
				processedBuf := vadEngine.Process(validBuf)

				// 永远固定向 FunASR 写入处理后的 Buffer，保持时间戳严丝合缝
				err = conn.WriteMessage(websocket.BinaryMessage, processedBuf)
				if err != nil {
					return
				}
			}
			if err != nil {
				return
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
