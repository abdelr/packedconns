package main

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"sort"
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

func appendVarint(element uint32, buffer []byte, elementCount int) (bool, []byte) {
	// Find the end position by decoding existing elements
	pos := 0
	for i := 0; i < elementCount && pos < len(buffer); i++ {
		_, n := binary.Uvarint(buffer[pos:])
		if n <= 0 {
			break
		}
		pos += n
	}

	// Calculate space needed for the new element
	var newElementBuffer [binary.MaxVarintLen64]byte
	newElementSize := binary.PutUvarint(newElementBuffer[:], uint64(element))

	// Check if current buffer has enough capacity
	requiredSize := pos + newElementSize

	if cap(buffer) >= requiredSize {
		// We have enough capacity, extend the slice and append
		newBuffer := buffer[:requiredSize]
		copy(newBuffer[pos:], newElementBuffer[:newElementSize])
		return false, newBuffer
	} else {
		// Need to reallocate
		// Create new buffer with some extra capacity for future appends
		newCapacity := requiredSize * 2
		newBuffer := make([]byte, requiredSize, newCapacity)

		// Copy existing data
		copy(newBuffer[:pos], buffer[:pos])

		// Add new element
		copy(newBuffer[pos:], newElementBuffer[:newElementSize])

		return true, newBuffer
	}
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

func appendPrefixed(element uint32, buffer []byte, elementCount int) (bool, []byte) {
	if elementCount == 0 {
		// Empty buffer, create new one with single element
		newBuffer := make([]byte, 0, 16) // Start with some capacity
		tempBuffer := make([]byte, 16)
		size := encodePrefixed([]uint32{element}, tempBuffer)
		newBuffer = append(newBuffer, tempBuffer[:size]...)
		return true, newBuffer
	}

	// Calculate current and new prefix structure
	newElementCount := elementCount + 1
	oldPrefixBytes := (elementCount*2 + 7) / 8
	newPrefixBytes := (newElementCount*2 + 7) / 8

	// Find current data size (total buffer - prefix bytes)
	currentDataSize := len(buffer) - oldPrefixBytes

	// Determine length needed for new element
	var newElementLength int
	if element < 256 {
		newElementLength = 1
	} else if element < 65536 {
		newElementLength = 2
	} else if element < 16777216 {
		newElementLength = 3
	} else {
		newElementLength = 4
	}

	// Calculate total size needed
	newTotalSize := newPrefixBytes + currentDataSize + newElementLength

	// Check if we need to reallocate
	needsRealloc := cap(buffer) < newTotalSize

	var newBuffer []byte
	if needsRealloc {
		// Allocate new buffer with extra capacity
		newBuffer = make([]byte, newTotalSize, newTotalSize*2)
	} else {
		// Use existing buffer
		newBuffer = buffer[:newTotalSize]
	}

	// Handle data movement if prefix size changed
	if newPrefixBytes != oldPrefixBytes {
		// Need to move existing data to accommodate larger prefix
		if needsRealloc {
			// Copy prefix
			copy(newBuffer[:oldPrefixBytes], buffer[:oldPrefixBytes])
			// Copy data to new position
			copy(newBuffer[newPrefixBytes:newPrefixBytes+currentDataSize],
				buffer[oldPrefixBytes:oldPrefixBytes+currentDataSize])
		} else {
			// Move data within same buffer (move backwards to avoid overlap)
			copy(newBuffer[newPrefixBytes:newPrefixBytes+currentDataSize],
				buffer[oldPrefixBytes:oldPrefixBytes+currentDataSize])
			// Copy prefix
			copy(newBuffer[:oldPrefixBytes], buffer[:oldPrefixBytes])
		}
		// Clear new prefix bytes
		for i := oldPrefixBytes; i < newPrefixBytes; i++ {
			newBuffer[i] = 0
		}
	} else if needsRealloc {
		// Same prefix size, just copy everything
		copy(newBuffer, buffer)
	}

	// Add new element data at the end
	dataPos := newPrefixBytes + currentDataSize
	switch newElementLength {
	case 1:
		newBuffer[dataPos] = byte(element)
	case 2:
		newBuffer[dataPos] = byte(element)
		newBuffer[dataPos+1] = byte(element >> 8)
	case 3:
		newBuffer[dataPos] = byte(element)
		newBuffer[dataPos+1] = byte(element >> 8)
		newBuffer[dataPos+2] = byte(element >> 16)
	case 4:
		newBuffer[dataPos] = byte(element)
		newBuffer[dataPos+1] = byte(element >> 8)
		newBuffer[dataPos+2] = byte(element >> 16)
		newBuffer[dataPos+3] = byte(element >> 24)
	}

	// Update prefix for new element
	newElementIndex := elementCount // 0-based index for new element
	prefixByteIdx := newElementIndex / 4
	bitOffset := (newElementIndex % 4) * 2
	lengthCode := byte(newElementLength - 1)

	if prefixByteIdx < newPrefixBytes {
		// Clear the bits first, then set new length code
		newBuffer[prefixByteIdx] &= ^(0x03 << bitOffset)
		newBuffer[prefixByteIdx] |= lengthCode << bitOffset
	}

	return needsRealloc, newBuffer
}

// ValueSelector maintains history of generated values and selects based on probability distribution
type ValueSelector struct {
	generatedValues []uint32
	sortedValues    []uint32
	rng             *rand.Rand
}

// NewValueSelector creates a new value selector with probabilistic distribution
func NewValueSelector() *ValueSelector {
	return &ValueSelector{
		generatedValues: make([]uint32, 0),
		sortedValues:    make([]uint32, 0),
		rng:             rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// GenerateInitialValue generates a new random value and adds it to the history
func (vs *ValueSelector) GenerateInitialValue(maxValue uint32) uint32 {
	// Generate values with different distributions to test various byte lengths
	var value uint32
	switch len(vs.generatedValues) % 4 {
	case 0:
		value = uint32(vs.rng.Intn(256)) // 1 byte
	case 1:
		value = uint32(vs.rng.Intn(65536)) // up to 2 bytes
	case 2:
		value = uint32(vs.rng.Intn(16777216)) // up to 3 bytes
	case 3:
		value = vs.rng.Uint32() % maxValue // up to 4 bytes
	}

	vs.addValue(value)
	return value
}

// GetNextValue returns the next value based on probability distribution:
// 95% probability: largest value generated so far
// 4% probability: second largest value
// 1% probability: third largest value
func (vs *ValueSelector) GetNextValue() uint32 {
	if len(vs.sortedValues) == 0 {
		panic("No values generated yet - call GenerateInitialValue first")
	}

	prob := vs.rng.Float64()

	switch {
	case prob < 0.95:
		// Return largest value (95% probability)
		return vs.sortedValues[len(vs.sortedValues)-1]
	case prob < 0.99:
		// Return second largest value (4% probability)
		if len(vs.sortedValues) >= 2 {
			return vs.sortedValues[len(vs.sortedValues)-2]
		}
		// If only one value exists, return it
		return vs.sortedValues[len(vs.sortedValues)-1]
	default:
		// Return third largest value (1% probability)
		if len(vs.sortedValues) >= 3 {
			return vs.sortedValues[len(vs.sortedValues)-3]
		} else if len(vs.sortedValues) >= 2 {
			return vs.sortedValues[len(vs.sortedValues)-2]
		}
		// If fewer than 3 values exist, return the largest
		return vs.sortedValues[len(vs.sortedValues)-1]
	}
}

// addValue adds a new value to both lists and maintains sorted order
func (vs *ValueSelector) addValue(value uint32) {
	vs.generatedValues = append(vs.generatedValues, value)

	// Insert into sorted slice maintaining order
	insertPos := sort.Search(len(vs.sortedValues), func(i int) bool {
		return vs.sortedValues[i] >= value
	})

	// Insert at the correct position
	vs.sortedValues = append(vs.sortedValues, 0)
	copy(vs.sortedValues[insertPos+1:], vs.sortedValues[insertPos:])
	vs.sortedValues[insertPos] = value
}

// Reset clears the value history
func (vs *ValueSelector) Reset() {
	vs.generatedValues = vs.generatedValues[:0]
	vs.sortedValues = vs.sortedValues[:0]
}

// Helper function to generate test data with probabilistic selection
func generateTestDeltasWithProbability(count int, maxValue uint32) []uint32 {
	vs := NewValueSelector()
	deltas := make([]uint32, count)

	// Generate first value randomly
	if count > 0 {
		deltas[0] = vs.GenerateInitialValue(maxValue)
	}

	// Generate subsequent values using probability distribution
	for i := 1; i < count; i++ {
		// Occasionally generate a completely new value to add variety
		if vs.rng.Float64() < 0.1 { // 10% chance to generate new value
			deltas[i] = vs.GenerateInitialValue(maxValue)
		} else {
			deltas[i] = vs.GetNextValue()
		}
	}

	return deltas
}

// Helper function to generate test data (original version for compatibility)
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

// Helper function to compare slices
func slicesEqual(a, b []uint32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
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
		varintCorrect := slicesEqual(varintDecoded, deltas)
		prefixedCorrect := slicesEqual(prefixedDecoded, deltas)

		fmt.Printf("Varint correct: %v, Prefixed correct: %v\n", varintCorrect, prefixedCorrect)
	}

	// Test append functions
	fmt.Println("\n--- Testing Append Functions ---")

	// Test appending to empty buffers
	fmt.Println("\nTesting append to empty buffers:")
	testValues := []uint32{42, 300, 70000, 20000000}

	for _, val := range testValues {
		fmt.Printf("Appending %d to empty buffer:\n", val)

		// Test Varint append
		varintBuffer := make([]byte, 0, 16)
		reallocated, newVarintBuffer := appendVarint(val, varintBuffer, 0)
		decoded := make([]uint32, 1)
		decodeVarint(newVarintBuffer, decoded)
		fmt.Printf("  Varint - Reallocated: %v, Size: %d, Decoded: %v, Correct: %v\n",
			reallocated, len(newVarintBuffer), decoded[0], decoded[0] == val)

		// Test Prefixed append
		prefixedBuffer := make([]byte, 0, 16)
		reallocated, newPrefixedBuffer := appendPrefixed(val, prefixedBuffer, 0)
		decoded = make([]uint32, 1)
		decodePrefixed(newPrefixedBuffer, decoded)
		fmt.Printf("  Prefixed - Reallocated: %v, Size: %d, Decoded: %v, Correct: %v\n",
			reallocated, len(newPrefixedBuffer), decoded[0], decoded[0] == val)
	}
}

func main() {
	TestCorrectness()

	fmt.Println("\nRunning benchmarks...")
	fmt.Println("Use: go test -bench=. to run benchmarks")

	// Quick performance comparison using original test data
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

	fmt.Println("\n=== Append Performance (Realistic Usage with Probabilistic Values) ===")

	// Test append performance with realistic scenarios using probabilistic value selection
	appendN := 100000
	maxElements := 32

	// Test different starting sizes and grow to 32 elements
	startingSizes := []int{0, 8, 16, 24}

	for _, startSize := range startingSizes {
		if startSize >= maxElements {
			continue
		}

		elementsToAdd := maxElements - startSize
		fmt.Printf("\nGrowing from %d to %d elements (%d appends per iteration) with probabilistic values:\n",
			startSize, maxElements, elementsToAdd)

		// Prepare initial data using probabilistic generation
		var initialData []uint32
		vs := NewValueSelector()
		if startSize > 0 {
			initialData = make([]uint32, startSize)
			for i := 0; i < startSize; i++ {
				initialData[i] = vs.GenerateInitialValue(1000000)
			}
		}

		// Prepare initial Varint buffer
		var varintInitBuffer []byte
		if startSize > 0 {
			varintInitBuffer = make([]byte, 1024)
			varintInitSize := encodeVarint(initialData, varintInitBuffer)
			varintInitBuffer = varintInitBuffer[:varintInitSize]
		} else {
			varintInitBuffer = make([]byte, 0, 256)
		}

		// Prepare initial Prefixed buffer
		var prefixedInitBuffer []byte
		if startSize > 0 {
			prefixedInitBuffer = make([]byte, 1024)
			prefixedInitSize := encodePrefixed(initialData, prefixedInitBuffer)
			prefixedInitBuffer = prefixedInitBuffer[:prefixedInitSize]
		} else {
			prefixedInitBuffer = make([]byte, 0, 256)
		}

		// Pre-generate values for consistent testing
		testIterations := make([][]uint32, appendN)
		for i := 0; i < appendN; i++ {
			vs.Reset()
			// Regenerate initial values for this iteration
			for j := 0; j < startSize; j++ {
				vs.GenerateInitialValue(1000000)
			}

			// Generate values to append
			testIterations[i] = make([]uint32, elementsToAdd)
			for j := 0; j < elementsToAdd; j++ {
				if j == 0 && startSize == 0 {
					// First value when starting from empty
					testIterations[i][j] = vs.GenerateInitialValue(1000000)
				} else {
					// Use probabilistic selection
					if vs.rng.Float64() < 0.1 { // 10% chance for new value
						testIterations[i][j] = vs.GenerateInitialValue(1000000)
					} else {
						testIterations[i][j] = vs.GetNextValue()
					}
				}
			}
		}

		// Benchmark Varint append
		totalReallocationsVarint := 0
		start = time.Now()

		for i := 0; i < appendN; i++ {
			// Reset to initial buffer state for each iteration
			currentBuffer := make([]byte, len(varintInitBuffer), cap(varintInitBuffer))
			copy(currentBuffer, varintInitBuffer)
			currentElementCount := startSize

			// Add elements using pre-generated values
			for j := 0; j < elementsToAdd; j++ {
				valueToAdd := testIterations[i][j]
				reallocated, newBuffer := appendVarint(valueToAdd, currentBuffer, currentElementCount)
				if reallocated {
					totalReallocationsVarint++
				}
				currentBuffer = newBuffer
				currentElementCount++
			}
		}
		varintAppendTime := time.Since(start)

		// Benchmark Prefixed append
		totalReallocationsPrefixed := 0
		start = time.Now()

		for i := 0; i < appendN; i++ {
			// Reset to initial buffer state for each iteration
			currentBuffer := make([]byte, len(prefixedInitBuffer), cap(prefixedInitBuffer))
			copy(currentBuffer, prefixedInitBuffer)
			currentElementCount := startSize

			// Add elements using pre-generated values
			for j := 0; j < elementsToAdd; j++ {
				valueToAdd := testIterations[i][j]
				reallocated, newBuffer := appendPrefixed(valueToAdd, currentBuffer, currentElementCount)
				if reallocated {
					totalReallocationsPrefixed++
				}
				currentBuffer = newBuffer
				currentElementCount++
			}
		}
		prefixedAppendTime := time.Since(start)

		fmt.Printf("Varint Append   - Time: %v, Reallocations: %d (%.3f per iteration)\n",
			varintAppendTime, totalReallocationsVarint, float64(totalReallocationsVarint)/float64(appendN))
		fmt.Printf("Prefixed Append - Time: %v, Reallocations: %d (%.3f per iteration)\n",
			prefixedAppendTime, totalReallocationsPrefixed, float64(totalReallocationsPrefixed)/float64(appendN))

		if prefixedAppendTime > 0 {
			fmt.Printf("Append speedup: %.2fx\n", float64(varintAppendTime)/float64(prefixedAppendTime))
		}

		// Show some statistics about the generated values
		if len(testIterations) > 0 && len(testIterations[0]) > 0 {
			// Analyze first iteration's values
			values := testIterations[0]
			uniqueValues := make(map[uint32]int)
			for _, v := range values {
				uniqueValues[v]++
			}
			fmt.Printf("Value diversity: %d unique values out of %d total values (%.1f%% unique)\n",
				len(uniqueValues), len(values), float64(len(uniqueValues))*100.0/float64(len(values)))
		}
	}

	fmt.Println("\nUse: go test -bench=. to run detailed benchmarks")
}
