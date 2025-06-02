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
