package main

import (
	"fmt"
	"io"
	"math"
	"os"
	"testing"
)

func TestVADSimulation(t *testing.T) {
	// 🎯 实验 1：你可以试着把这个改成 3，看看能不能把 #173 挡在门外
	minTriggerFrames := 2

	// 🎯 实验 2：你可以试着把这个提到 0.05，看看能不能利用 ZCR 把它卡死
	testMinZCR := 0.01

	testThreshold := 400.0
	testMaxZCR := 0.50

	inputFile := "../debug/0610-14.wav"
	outputFile := "../debug/result_061014.wav"

	in, err := os.Open(inputFile)
	if err != nil {
		t.Fatalf("无法打开输入文件: %v", err)
	}
	defer in.Close()

	out, err := os.Create(outputFile)
	if err != nil {
		t.Fatalf("无法创建输出文件: %v", err)
	}
	defer out.Close()

	headerBuffer := make([]byte, 2048)
	_, _ = in.Read(headerBuffer)
	dataOffset := 44
	for i := 0; i < len(headerBuffer)-4; i++ {
		if headerBuffer[i] == 'd' && headerBuffer[i+1] == 'a' && headerBuffer[i+2] == 't' && headerBuffer[i+3] == 'a' {
			dataOffset = i + 8
			break
		}
	}
	_, _ = out.Write(headerBuffer[:dataOffset])
	_, _ = in.Seek(int64(dataOffset), io.SeekStart)

	buf := make([]byte, 3200)
	frameCount := 0
	triggerCount := 0

	fmt.Println("\n=================== 🎬 15s - 19s 高清微观轨迹追踪 ===================")
	fmt.Printf("%-8s\t%-12s\t%-12s\t%-16s\t%-10s\n", "帧号(时间)", "RMS能量", "过零率(ZCR)", "满足单帧条件?", "VAD最终决策")
	fmt.Println("---------------------------------------------------------------------------------")

	for {
		n, err := in.Read(buf)
		if n > 0 {
			frameCount++
			validBuf := buf[:n]
			pcmData := bytesToInt16(validBuf)

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

			isCurrentFrameValid := rms > testThreshold && zcr > testMinZCR && zcr < testMaxZCR

			isSpeech := false
			if isCurrentFrameValid {
				triggerCount++
				if triggerCount >= minTriggerFrames {
					isSpeech = true
				}
			} else {
				triggerCount = 0
			}

			// 💡 重点改进：只要进入 15s~19s 区间 (#150~#190)，不管满不满足条件，全量强制打印！
			if frameCount >= 320 && frameCount <= 360 {
				timeSec := float64(frameCount) / 10.0
				singleValidStr := "❌ 否"
				if isCurrentFrameValid {
					singleValidStr = "✅ 是"
				}
				statusStr := "🤫 填充静音垫"
				if isSpeech {
					statusStr = "🗣️ 放行 -> 发给FunASR"
				}
				fmt.Printf("#%-4d(%.1fs)\t%-12.2f\t%-12.4f\t%-16s\t%-10s\n", frameCount, timeSec, rms, zcr, singleValidStr, statusStr)
			}

			if isSpeech {
				out.Write(validBuf)
			} else {
				out.Write(make([]byte, len(validBuf)))
			}
		}
		if err == io.EOF {
			break
		}
	}
}
