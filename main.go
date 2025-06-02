package main

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"time"
)

// Traditional Varint Implementation
func encodeVarint(deltas []uint32, buffer []byte) int {
	pos := 0
	for _, delta := range deltas {
		n := binary.PutUvarint(buffer[pos:], uint64(delta))
		pos += n
	}
	return pos
}

func decodeVarint(buffer []byte, deltas []uint32) int {
	pos := 0
	deltaIdx := 0

	for pos < len(buffer) && deltaIdx < len(deltas) {
		value, n := binary.Uvarint(buffer[pos:])
		deltas[deltaIdx] = uint32(value)
		pos += n
		deltaIdx++
	}

	return deltaIdx
}

// Prefixed Length Implementation
func encodePrefixed(deltas []uint32, buffer []byte) int {
	if len(deltas) == 0 {
		return 0
	}

	count := len(deltas)
	prefixBytes := (count*2 + 7) / 8
	dataPos := prefixBytes

	// Process in groups of 4 deltas per prefix byte
	for groupStart := 0; groupStart < count; groupStart += 4 {
		var prefixByte byte
		groupEnd := groupStart + 4
		if groupEnd > count {
			groupEnd = count
		}

		// Process each delta in the group
		for i := groupStart; i < groupEnd; i++ {
			delta := deltas[i]
			bitPos := (i - groupStart) * 2

			// Determine length and write data
			if delta < 256 {
				// 1 byte: length code 0
				buffer[dataPos] = byte(delta)
				dataPos++
			} else if delta < 65536 {
				// 2 bytes: length code 1
				prefixByte |= 1 << bitPos
				buffer[dataPos] = byte(delta)
				buffer[dataPos+1] = byte(delta >> 8)
				dataPos += 2
			} else if delta < 16777216 {
				// 3 bytes: length code 2
				prefixByte |= 2 << bitPos
				buffer[dataPos] = byte(delta)
				buffer[dataPos+1] = byte(delta >> 8)
				buffer[dataPos+2] = byte(delta >> 16)
				dataPos += 3
			} else {
				// 4 bytes: length code 3
				prefixByte |= 3 << bitPos
				buffer[dataPos] = byte(delta)
				buffer[dataPos+1] = byte(delta >> 8)
				buffer[dataPos+2] = byte(delta >> 16)
				buffer[dataPos+3] = byte(delta >> 24)
				dataPos += 4
			}
		}

		// Write the prefix byte
		buffer[groupStart/4] = prefixByte
	}

	return dataPos
}

func decodePrefixed(buffer []byte, deltas []uint32) int {
	if len(buffer) == 0 || len(deltas) == 0 {
		return 0
	}

	prefixBytes := (len(deltas)*2 + 7) / 8
	if len(buffer) < prefixBytes {
		return 0
	}

	pos := prefixBytes
	for i := 0; i < len(deltas) && pos < len(buffer); i++ {
		prefixByteIdx := i / 4
		bitOffset := (i % 4) * 2
		lengthCode := (buffer[prefixByteIdx] >> bitOffset) & 0x03
		length := lengthCode + 1

		// Mirror the encoding bit shifting logic exactly
		switch length {
		case 1:
			if pos+1 <= len(buffer) {
				deltas[i] = uint32(buffer[pos])
			}
		case 2:
			if pos+2 <= len(buffer) {
				deltas[i] = uint32(buffer[pos]) | uint32(buffer[pos+1])<<8
			}
		case 3:
			if pos+3 <= len(buffer) {
				deltas[i] = uint32(buffer[pos]) | uint32(buffer[pos+1])<<8 | uint32(buffer[pos+2])<<16
			}
		case 4:
			if pos+4 <= len(buffer) {
				deltas[i] = uint32(buffer[pos]) | uint32(buffer[pos+1])<<8 | uint32(buffer[pos+2])<<16 | uint32(buffer[pos+3])<<24
			}
		}
		pos += int(length)
	}

	return len(deltas)
}

// Helper function to generate test data
func generateTestDeltas(count int, maxValue uint32) []uint32 {
	rand.Seed(time.Now().UnixNano())
	deltas := make([]uint32, count)

	for i := 0; i < count; i++ {
		// Generate values with different distributions to test various byte lengths
		switch i % 4 {
		case 0:
			deltas[i] = uint32(rand.Intn(256)) // 1 byte
		case 1:
			deltas[i] = uint32(rand.Intn(65536)) // up to 2 bytes
		case 2:
			deltas[i] = uint32(rand.Intn(16777216)) // up to 3 bytes
		case 3:
			deltas[i] = rand.Uint32() % maxValue // up to 4 bytes
		}
	}

	return deltas
}

// Test function to verify correctness
func TestCorrectness() {
	fmt.Println("Testing correctness...")

	// Test with various delta values
	testCases := [][]uint32{
		{1, 2, 3, 4, 5},
		{127, 128, 129, 255, 256},
		{65535, 65536, 16777215, 16777216},
		{0, 1, 127, 128, 255, 256, 65535, 65536},
	}

	for i, deltas := range testCases {
		fmt.Printf("\nTest case %d: %v\n", i+1, deltas)

		// Test Varint
		varintBuffer := make([]byte, 1024)
		varintSize := encodeVarint(deltas, varintBuffer)
		varintDecoded := make([]uint32, len(deltas))
		decodeVarint(varintBuffer[:varintSize], varintDecoded)

		fmt.Printf("Varint - Size: %d bytes, Decoded: %v\n", varintSize, varintDecoded)

		// Test Prefixed
		prefixedBuffer := make([]byte, 1024)
		prefixedSize := encodePrefixed(deltas, prefixedBuffer)
		prefixedDecoded := make([]uint32, len(deltas))
		decodePrefixed(prefixedBuffer[:prefixedSize], prefixedDecoded)

		fmt.Printf("Prefixed - Size: %d bytes, Decoded: %v\n", prefixedSize, prefixedDecoded)

		// Verify correctness
		varintCorrect := true
		prefixedCorrect := true

		for j := 0; j < len(deltas); j++ {
			if varintDecoded[j] != deltas[j] {
				varintCorrect = false
			}
			if prefixedDecoded[j] != deltas[j] {
				prefixedCorrect = false
			}
		}

		fmt.Printf("Varint correct: %v, Prefixed correct: %v\n", varintCorrect, prefixedCorrect)
	}
}

func main() {
	TestCorrectness()

	fmt.Println("\nRunning benchmarks...")
	fmt.Println("Use: go test -bench=. to run benchmarks")

	// Quick performance comparison
	deltas := generateTestDeltas(32, 1000000)
	N := 1000000

	// Varint timing
	varintBuffer := make([]byte, 1024)
	start := time.Now()
	for i := 0; i < N; i++ {
		encodeVarint(deltas, varintBuffer)
	}
	varintEncodeTime := time.Since(start)

	encodedSize := encodeVarint(deltas, varintBuffer)
	varintDecoded := make([]uint32, 32)
	start = time.Now()
	for i := 0; i < N; i++ {
		decodeVarint(varintBuffer[:encodedSize], varintDecoded)
	}
	varintDecodeTime := time.Since(start)

	// Prefixed timing
	prefixedBuffer := make([]byte, 1024)
	start = time.Now()
	for i := 0; i < N; i++ {
		encodePrefixed(deltas, prefixedBuffer)
	}
	prefixedEncodeTime := time.Since(start)

	prefixedSize := encodePrefixed(deltas, prefixedBuffer)
	prefixedDecoded := make([]uint32, 32)
	start = time.Now()
	for i := 0; i < N; i++ {
		decodePrefixed(prefixedBuffer[:prefixedSize], prefixedDecoded)
	}
	prefixedDecodeTime := time.Since(start)

	fmt.Printf("\nPerformance comparison (1M iterations):\n")
	fmt.Printf("Varint   - Encode: %v, Decode: %v, Size: %d bytes\n", varintEncodeTime, varintDecodeTime, encodedSize)
	fmt.Printf("Prefixed - Encode: %v, Decode: %v, Size: %d bytes\n", prefixedEncodeTime, prefixedDecodeTime, prefixedSize)
	fmt.Printf("Encode speedup: %.2fx\n", float64(varintEncodeTime)/float64(prefixedEncodeTime))
	fmt.Printf("Decode speedup: %.2fx\n", float64(varintDecodeTime)/float64(prefixedDecodeTime))
}
