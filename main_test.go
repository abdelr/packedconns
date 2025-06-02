package main

import "testing"

// Benchmark for Varint encoding
func BenchmarkVarintEncode(b *testing.B) {
	deltas := generateTestDeltas(32, 1000000)
	buffer := make([]byte, 1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encodeVarint(deltas, buffer)
	}
}

// Benchmark for Varint decoding
func BenchmarkVarintDecode(b *testing.B) {
	deltas := generateTestDeltas(32, 1000000)
	buffer := make([]byte, 1024)
	encodedSize := encodeVarint(deltas, buffer)
	encodedBuffer := buffer[:encodedSize]

	decodedDeltas := make([]uint32, 32)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		decodeVarint(encodedBuffer, decodedDeltas)
	}
}

// Benchmark for Prefixed encoding
func BenchmarkPrefixedEncode(b *testing.B) {
	deltas := generateTestDeltas(32, 1000000)
	buffer := make([]byte, 1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encodePrefixed(deltas, buffer)
	}
}

// Benchmark for Prefixed decoding
func BenchmarkPrefixedDecode(b *testing.B) {
	deltas := generateTestDeltas(32, 1000000)
	buffer := make([]byte, 1024)
	encodedSize := encodePrefixed(deltas, buffer)
	encodedBuffer := buffer[:encodedSize]

	decodedDeltas := make([]uint32, 32)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		decodePrefixed(encodedBuffer, decodedDeltas)
	}
}

// Benchmark for Varint append to empty buffer
func BenchmarkVarintAppendEmpty(b *testing.B) {
	testValue := uint32(12345)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buffer := make([]byte, 0, 16)
		_, _ = appendVarint(testValue, buffer, 0)
	}
}

// Benchmark for Prefixed append to empty buffer
func BenchmarkPrefixedAppendEmpty(b *testing.B) {
	testValue := uint32(12345)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buffer := make([]byte, 0, 16)
		_, _ = appendPrefixed(testValue, buffer, 0)
	}
}

// Benchmark for Varint append to small buffer (4 elements)
func BenchmarkVarintAppendSmall(b *testing.B) {
	initialData := generateTestDeltas(4, 1000000)
	testValue := uint32(12345)

	// Prepare initial buffer
	initBuffer := make([]byte, 1024)
	initSize := encodeVarint(initialData, initBuffer)
	initBuffer = initBuffer[:initSize]

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buffer := make([]byte, len(initBuffer), cap(initBuffer))
		copy(buffer, initBuffer)
		_, _ = appendVarint(testValue, buffer, 4)
	}
}

// Benchmark for Prefixed append to small buffer (4 elements)
func BenchmarkPrefixedAppendSmall(b *testing.B) {
	initialData := generateTestDeltas(4, 1000000)
	testValue := uint32(12345)

	// Prepare initial buffer
	initBuffer := make([]byte, 1024)
	initSize := encodePrefixed(initialData, initBuffer)
	initBuffer = initBuffer[:initSize]

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buffer := make([]byte, len(initBuffer), cap(initBuffer))
		copy(buffer, initBuffer)
		_, _ = appendPrefixed(testValue, buffer, 4)
	}
}

// Benchmark for Varint append to medium buffer (16 elements)
func BenchmarkVarintAppendMedium(b *testing.B) {
	initialData := generateTestDeltas(16, 1000000)
	testValue := uint32(12345)

	// Prepare initial buffer
	initBuffer := make([]byte, 1024)
	initSize := encodeVarint(initialData, initBuffer)
	initBuffer = initBuffer[:initSize]

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buffer := make([]byte, len(initBuffer), cap(initBuffer))
		copy(buffer, initBuffer)
		_, _ = appendVarint(testValue, buffer, 16)
	}
}

// Benchmark for Prefixed append to medium buffer (16 elements)
func BenchmarkPrefixedAppendMedium(b *testing.B) {
	initialData := generateTestDeltas(16, 1000000)
	testValue := uint32(12345)

	// Prepare initial buffer
	initBuffer := make([]byte, 1024)
	initSize := encodePrefixed(initialData, initBuffer)
	initBuffer = initBuffer[:initSize]

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buffer := make([]byte, len(initBuffer), cap(initBuffer))
		copy(buffer, initBuffer)
		_, _ = appendPrefixed(testValue, buffer, 16)
	}
}

// Benchmark for Varint append to large buffer (32 elements)
func BenchmarkVarintAppendLarge(b *testing.B) {
	initialData := generateTestDeltas(32, 1000000)
	testValue := uint32(12345)

	// Prepare initial buffer
	initBuffer := make([]byte, 1024)
	initSize := encodeVarint(initialData, initBuffer)
	initBuffer = initBuffer[:initSize]

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buffer := make([]byte, len(initBuffer), cap(initBuffer))
		copy(buffer, initBuffer)
		_, _ = appendVarint(testValue, buffer, 32)
	}
}
