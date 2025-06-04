package main

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"slices"
	"sort"
	"time"
)

// Traditional Varint Implementation with uint64 interface and delta encoding
func encodeVarintDeltas(sortedValues []uint64, buffer []byte) int {
	if len(sortedValues) == 0 {
		return 0
	}

	// Sort values and calculate deltas in single pass
	slices.Sort(sortedValues)

	pos := 0
	var prevValue uint64 = 0

	for _, value := range sortedValues {
		delta := uint32(value - prevValue)

		n := binary.PutUvarint(buffer[pos:], uint64(delta))
		pos += n
		prevValue = value
	}
	return pos
}

func decodeVarintDeltas(buffer []byte, size int) []uint64 {
	if len(buffer) == 0 {
		return nil
	}

	pos := 0
	deltaIdx := 0
	values := make([]uint64, 0, size)
	var currentValue uint64 = 0

	for pos < len(buffer) && deltaIdx < size {
		deltaVal, n := binary.Uvarint(buffer[pos:])
		if n <= 0 {
			break
		}

		currentValue += deltaVal
		values = append(values, currentValue)

		pos += n
		deltaIdx++
	}

	return values
}

func appendVarintDelta(element uint64, buffer []byte, elementCount int) (bool, []byte) {
	if elementCount == 0 {
		// Empty buffer case
		var newBuffer [binary.MaxVarintLen64]byte
		n := binary.PutUvarint(newBuffer[:], element)
		result := make([]byte, n, n*2)
		copy(result, newBuffer[:n])
		return true, result
	}

	// Need to find insertion position by decoding
	pos := 0
	insertBytePos := 0
	elementIndex := 0
	var currentValue uint64 = 0
	var lastDelta uint64

	for elementIndex < elementCount && pos < len(buffer) {
		deltaVal, n := binary.Uvarint(buffer[pos:])
		if n <= 0 {
			break
		}

		currentValue += deltaVal
		lastDelta = deltaVal
		if currentValue < element {
			insertBytePos = pos + n
		} else {
			break
		}

		pos += n
		elementIndex++
	}

	if elementIndex == elementCount {
		insertBytePos = pos
	}

	// Calculate delta to insert
	var insertDelta uint32
	if elementIndex == 0 {
		insertDelta = uint32(element)
	} else {
		// Need to decode previous value
		prevValue := currentValue - lastDelta
		insertDelta = uint32(element - prevValue)
	}

	// Encode new delta
	var newElementBuffer [binary.MaxVarintLen64]byte
	newElementSize := binary.PutUvarint(newElementBuffer[:], uint64(insertDelta))

	// Need to adjust subsequent delta if not inserting at end
	var adjustmentSize int
	var adjustmentBuffer [binary.MaxVarintLen64]byte

	if elementIndex < elementCount {
		// Get the next element's current delta and adjust it
		nextDeltaVal, nextDeltaSize := binary.Uvarint(buffer[insertBytePos:])
		adjustedDelta := nextDeltaVal - uint64(insertDelta)
		adjustmentSize = binary.PutUvarint(adjustmentBuffer[:], adjustedDelta)

		// Total size change
		sizeChange := newElementSize + adjustmentSize - nextDeltaSize
		requiredSize := len(buffer) + sizeChange

		needsRealloc := cap(buffer) < requiredSize
		var newBuffer []byte

		if needsRealloc {
			newBuffer = make([]byte, requiredSize, requiredSize*2)
			copy(newBuffer[:insertBytePos], buffer[:insertBytePos])
		} else {
			newBuffer = buffer[:requiredSize]
			copy(newBuffer[insertBytePos+newElementSize+adjustmentSize:],
				buffer[insertBytePos+nextDeltaSize:])
		}

		// Insert new element and adjusted delta
		copy(newBuffer[insertBytePos:], newElementBuffer[:newElementSize])
		copy(newBuffer[insertBytePos+newElementSize:], adjustmentBuffer[:adjustmentSize])

		if needsRealloc {
			copy(newBuffer[insertBytePos+newElementSize+adjustmentSize:],
				buffer[insertBytePos+nextDeltaSize:])
		}

		return needsRealloc, newBuffer
	} else {
		// Inserting at end
		requiredSize := len(buffer) + newElementSize
		needsRealloc := cap(buffer) < requiredSize

		var newBuffer []byte
		if needsRealloc {
			newBuffer = make([]byte, requiredSize, requiredSize*2)
			copy(newBuffer, buffer)
		} else {
			newBuffer = buffer[:requiredSize]
		}

		copy(newBuffer[insertBytePos:], newElementBuffer[:newElementSize])
		return needsRealloc, newBuffer
	}
}

// Prefixed Length Implementation with uint64 interface and delta encoding
func encodePrefixedDeltas(sortedValues []uint64, buffer []byte) (int, uint64) {
	if len(sortedValues) == 0 {
		return 0, 0
	}

	// Sort values and calculate deltas in single pass
	slices.Sort(sortedValues)

	count := len(sortedValues)
	prefixBytes := (count*2 + 7) / 8
	dataPos := prefixBytes
	lastValue := sortedValues[len(sortedValues)-1]

	var prevValue uint64 = 0

	// Process in groups of 4 deltas per prefix byte
	for groupStart := 0; groupStart < count; groupStart += 4 {
		var prefixByte byte
		groupEnd := groupStart + 4
		if groupEnd > count {
			groupEnd = count
		}

		// Process each delta in the group
		for i := groupStart; i < groupEnd; i++ {
			delta := uint32(sortedValues[i] - prevValue)
			bitPos := (i - groupStart) * 2

			// Determine length and write data
			if delta < 256 {
				buffer[dataPos] = byte(delta)
				dataPos++
			} else if delta < 65536 {
				prefixByte |= 1 << bitPos
				buffer[dataPos] = byte(delta)
				buffer[dataPos+1] = byte(delta >> 8)
				dataPos += 2
			} else if delta < 16777216 {
				prefixByte |= 2 << bitPos
				buffer[dataPos] = byte(delta)
				buffer[dataPos+1] = byte(delta >> 8)
				buffer[dataPos+2] = byte(delta >> 16)
				dataPos += 3
			} else {
				prefixByte |= 3 << bitPos
				buffer[dataPos] = byte(delta)
				buffer[dataPos+1] = byte(delta >> 8)
				buffer[dataPos+2] = byte(delta >> 16)
				buffer[dataPos+3] = byte(delta >> 24)
				dataPos += 4
			}

			prevValue = sortedValues[i]
		}

		buffer[groupStart/4] = prefixByte
	}

	return dataPos, lastValue
}

func decodePrefixedDeltas(buffer []byte, elementCount int) []uint64 {
	if len(buffer) == 0 || elementCount == 0 {
		return nil
	}

	prefixBytes := (elementCount*2 + 7) / 8
	if len(buffer) < prefixBytes {
		return nil
	}

	values := make([]uint64, elementCount)
	pos := prefixBytes
	var currentValue uint64 = 0

	for i := 0; i < elementCount && pos < len(buffer); i++ {
		prefixByteIdx := i / 4
		bitOffset := (i % 4) * 2
		lengthCode := (buffer[prefixByteIdx] >> bitOffset) & 0x03
		length := lengthCode + 1

		var delta uint32
		switch length {
		case 1:
			if pos+1 <= len(buffer) {
				delta = uint32(buffer[pos])
			}
		case 2:
			if pos+2 <= len(buffer) {
				delta = uint32(buffer[pos]) | uint32(buffer[pos+1])<<8
			}
		case 3:
			if pos+3 <= len(buffer) {
				delta = uint32(buffer[pos]) | uint32(buffer[pos+1])<<8 | uint32(buffer[pos+2])<<16
			}
		case 4:
			if pos+4 <= len(buffer) {
				delta = uint32(buffer[pos]) | uint32(buffer[pos+1])<<8 | uint32(buffer[pos+2])<<16 | uint32(buffer[pos+3])<<24
			}
		}

		currentValue += uint64(delta)
		values[i] = currentValue
		pos += int(length)
	}

	return values
}

func appendPrefixedDelta(element uint64, buffer []byte, elementCount int, lastValue uint64) (bool, []byte, uint64) {
	if elementCount == 0 {
		// Empty buffer case
		newBuffer := make([]byte, 0, 16)
		tempBuffer := make([]byte, 16)
		size, newLastValue := encodePrefixedDeltas([]uint64{element}, tempBuffer)
		newBuffer = append(newBuffer, tempBuffer[:size]...)
		return true, newBuffer, newLastValue
	}

	// Quick check: if element is >= lastValue, might be able to append efficiently
	if element >= lastValue {
		return appendPrefixedDeltaAtEnd(element, buffer, elementCount, lastValue)
	}

	// Full insertion logic needed
	return insertPrefixedDeltaInMiddle(element, buffer, elementCount, lastValue)
}

func appendPrefixedDeltaAtEnd(element uint64, buffer []byte, elementCount int, lastValue uint64) (bool, []byte, uint64) {
	// Calculate delta from last value
	delta := uint32(element - lastValue)

	// Determine space needed for new element
	var newElementLength int
	if delta < 256 {
		newElementLength = 1
	} else if delta < 65536 {
		newElementLength = 2
	} else if delta < 16777216 {
		newElementLength = 3
	} else {
		newElementLength = 4
	}

	// Calculate new structure
	newElementCount := elementCount + 1
	oldPrefixBytes := (elementCount*2 + 7) / 8
	newPrefixBytes := (newElementCount*2 + 7) / 8

	// Calculate total size needed
	oldDataSize := len(buffer) - oldPrefixBytes
	newTotalSize := newPrefixBytes + oldDataSize + newElementLength

	// Check if we need to reallocate
	needsRealloc := cap(buffer) < newTotalSize
	prefixExpanded := newPrefixBytes != oldPrefixBytes

	var newBuffer []byte

	if needsRealloc {
		newBuffer = make([]byte, newTotalSize, newTotalSize*2)

		if prefixExpanded {
			// Copy and clear new prefix bytes
			copy(newBuffer[:oldPrefixBytes], buffer[:oldPrefixBytes])
			for i := oldPrefixBytes; i < newPrefixBytes; i++ {
				newBuffer[i] = 0
			}
			// Copy data
			copy(newBuffer[newPrefixBytes:newPrefixBytes+oldDataSize], buffer[oldPrefixBytes:])
		} else {
			// Direct copy
			copy(newBuffer[:len(buffer)], buffer)
		}
	} else {
		newBuffer = buffer[:newTotalSize]

		if prefixExpanded {
			// Move data to make room for expanded prefix
			copy(newBuffer[newPrefixBytes:], buffer[oldPrefixBytes:len(buffer)])
			// Clear new prefix bytes
			for i := oldPrefixBytes; i < newPrefixBytes; i++ {
				newBuffer[i] = 0
			}
		}
	}

	// Insert new element data at end
	dataInsertPos := newPrefixBytes + oldDataSize
	switch newElementLength {
	case 1:
		newBuffer[dataInsertPos] = byte(delta)
	case 2:
		newBuffer[dataInsertPos] = byte(delta)
		newBuffer[dataInsertPos+1] = byte(delta >> 8)
	case 3:
		newBuffer[dataInsertPos] = byte(delta)
		newBuffer[dataInsertPos+1] = byte(delta >> 8)
		newBuffer[dataInsertPos+2] = byte(delta >> 16)
	case 4:
		newBuffer[dataInsertPos] = byte(delta)
		newBuffer[dataInsertPos+1] = byte(delta >> 8)
		newBuffer[dataInsertPos+2] = byte(delta >> 16)
		newBuffer[dataInsertPos+3] = byte(delta >> 24)
	}

	// Update prefix for new element (it's at position elementCount)
	newElemPrefixByteIdx := elementCount / 4
	newElemBitOffset := (elementCount % 4) * 2
	newElemLengthCode := byte(newElementLength - 1)

	if newElemPrefixByteIdx < newPrefixBytes {
		newBuffer[newElemPrefixByteIdx] &= ^(0x03 << newElemBitOffset)
		newBuffer[newElemPrefixByteIdx] |= newElemLengthCode << newElemBitOffset
	}

	return needsRealloc, newBuffer, element
}

func insertPrefixedDeltaInMiddle(element uint64, buffer []byte, elementCount int, lastValue uint64) (bool, []byte, uint64) {
	if elementCount == 0 {
		return appendPrefixedDeltaAtEnd(element, buffer, elementCount, lastValue)
	}

	oldPrefixBytes := (elementCount*2 + 7) / 8
	newElementCount := elementCount + 1
	newPrefixBytes := (newElementCount*2 + 7) / 8

	// Find insertion position by decoding only what we need
	pos := oldPrefixBytes
	elementIndex := 0
	var currentValue uint64 = 0
	var insertBytePos int
	var prevValue uint64
	var nextElementIndex int = -1
	var nextElementBytePos int
	var nextElementLength int

	// Scan to find insertion position
	for elementIndex < elementCount && pos < len(buffer) {
		prefixByteIdx := elementIndex / 4
		bitOffset := (elementIndex % 4) * 2
		lengthCode := (buffer[prefixByteIdx] >> bitOffset) & 0x03
		length := int(lengthCode) + 1

		var delta uint32
		switch length {
		case 1:
			delta = uint32(buffer[pos])
		case 2:
			delta = uint32(buffer[pos]) | uint32(buffer[pos+1])<<8
		case 3:
			delta = uint32(buffer[pos]) | uint32(buffer[pos+1])<<8 | uint32(buffer[pos+2])<<16
		case 4:
			delta = uint32(buffer[pos]) | uint32(buffer[pos+1])<<8 | uint32(buffer[pos+2])<<16 | uint32(buffer[pos+3])<<24
		}

		currentValue += uint64(delta)

		if currentValue < element {
			insertBytePos = pos + length
			prevValue = currentValue
		} else if nextElementIndex == -1 {
			// Found the element that will come after our insertion
			nextElementIndex = elementIndex
			nextElementBytePos = pos
			nextElementLength = length
			insertBytePos = pos
			break
		}

		pos += length
		elementIndex++
	}

	// If we reached the end, insert at the end
	if nextElementIndex == -1 {
		return appendPrefixedDeltaAtEnd(element, buffer, elementCount, lastValue)
	}

	// Calculate the delta for our new element
	var newElementDelta uint32
	if nextElementIndex == 0 {
		newElementDelta = uint32(element)
	} else {
		newElementDelta = uint32(element - prevValue)
	}

	// Calculate the adjusted delta for the next element
	adjustedNextDelta := uint32(currentValue - element)

	// Determine lengths needed for new element and adjusted next element
	var newElementLength int
	if newElementDelta < 256 {
		newElementLength = 1
	} else if newElementDelta < 65536 {
		newElementLength = 2
	} else if newElementDelta < 16777216 {
		newElementLength = 3
	} else {
		newElementLength = 4
	}

	var adjustedNextElementLength int
	if adjustedNextDelta < 256 {
		adjustedNextElementLength = 1
	} else if adjustedNextDelta < 65536 {
		adjustedNextElementLength = 2
	} else if adjustedNextDelta < 16777216 {
		adjustedNextElementLength = 3
	} else {
		adjustedNextElementLength = 4
	}

	// Calculate size changes
	dataLengthChange := newElementLength + adjustedNextElementLength - nextElementLength
	prefixBytesChange := newPrefixBytes - oldPrefixBytes
	totalSizeChange := dataLengthChange + prefixBytesChange

	newTotalSize := len(buffer) + totalSizeChange
	needsRealloc := cap(buffer) < newTotalSize

	var newBuffer []byte
	if needsRealloc {
		newBuffer = make([]byte, newTotalSize, newTotalSize*2)
	} else {
		newBuffer = buffer[:newTotalSize]
	}

	// Handle prefix expansion if needed
	if prefixBytesChange > 0 {
		if needsRealloc {
			// Copy prefix bytes
			copy(newBuffer[:oldPrefixBytes], buffer[:oldPrefixBytes])
			// Clear new prefix bytes
			for i := oldPrefixBytes; i < newPrefixBytes; i++ {
				newBuffer[i] = 0
			}
			// Copy data before insertion point
			copy(newBuffer[newPrefixBytes:newPrefixBytes+insertBytePos-oldPrefixBytes],
				buffer[oldPrefixBytes:insertBytePos])
		} else {
			// Move data to make room for expanded prefix
			copy(newBuffer[newPrefixBytes+insertBytePos-oldPrefixBytes+newElementLength+adjustedNextElementLength:],
				buffer[insertBytePos+nextElementLength:])
			copy(newBuffer[newPrefixBytes:newPrefixBytes+insertBytePos-oldPrefixBytes],
				buffer[oldPrefixBytes:insertBytePos])
			// Clear new prefix bytes
			for i := oldPrefixBytes; i < newPrefixBytes; i++ {
				newBuffer[i] = 0
			}
		}
	} else {
		if needsRealloc {
			// Copy everything before insertion point
			copy(newBuffer[:insertBytePos], buffer[:insertBytePos])
		} else {
			// Move data after insertion point
			copy(newBuffer[insertBytePos+newElementLength+adjustedNextElementLength:],
				buffer[insertBytePos+nextElementLength:])
		}
	}

	// Copy prefix bytes if we haven't already
	if !needsRealloc && prefixBytesChange == 0 {
		// Data already in place, just copy the prefix
		copy(newBuffer[:oldPrefixBytes], buffer[:oldPrefixBytes])
	} else if needsRealloc && prefixBytesChange == 0 {
		copy(newBuffer[:oldPrefixBytes], buffer[:oldPrefixBytes])
	}

	// Insert new element data
	newElementInsertPos := newPrefixBytes + (insertBytePos - oldPrefixBytes)
	switch newElementLength {
	case 1:
		newBuffer[newElementInsertPos] = byte(newElementDelta)
	case 2:
		newBuffer[newElementInsertPos] = byte(newElementDelta)
		newBuffer[newElementInsertPos+1] = byte(newElementDelta >> 8)
	case 3:
		newBuffer[newElementInsertPos] = byte(newElementDelta)
		newBuffer[newElementInsertPos+1] = byte(newElementDelta >> 8)
		newBuffer[newElementInsertPos+2] = byte(newElementDelta >> 16)
	case 4:
		newBuffer[newElementInsertPos] = byte(newElementDelta)
		newBuffer[newElementInsertPos+1] = byte(newElementDelta >> 8)
		newBuffer[newElementInsertPos+2] = byte(newElementDelta >> 16)
		newBuffer[newElementInsertPos+3] = byte(newElementDelta >> 24)
	}

	// Insert adjusted next element data
	adjustedNextInsertPos := newElementInsertPos + newElementLength
	switch adjustedNextElementLength {
	case 1:
		newBuffer[adjustedNextInsertPos] = byte(adjustedNextDelta)
	case 2:
		newBuffer[adjustedNextInsertPos] = byte(adjustedNextDelta)
		newBuffer[adjustedNextInsertPos+1] = byte(adjustedNextDelta >> 8)
	case 3:
		newBuffer[adjustedNextInsertPos] = byte(adjustedNextDelta)
		newBuffer[adjustedNextInsertPos+1] = byte(adjustedNextDelta >> 8)
		newBuffer[adjustedNextInsertPos+2] = byte(adjustedNextDelta >> 16)
	case 4:
		newBuffer[adjustedNextInsertPos] = byte(adjustedNextDelta)
		newBuffer[adjustedNextInsertPos+1] = byte(adjustedNextDelta >> 8)
		newBuffer[adjustedNextInsertPos+2] = byte(adjustedNextDelta >> 16)
		newBuffer[adjustedNextInsertPos+3] = byte(adjustedNextDelta >> 24)
	}

	// Copy remaining data after the adjusted element
	if needsRealloc {
		copy(newBuffer[adjustedNextInsertPos+adjustedNextElementLength:],
			buffer[nextElementBytePos+nextElementLength:])
	}

	// Update prefix bits for new element
	newElemPrefixByteIdx := nextElementIndex / 4
	newElemBitOffset := (nextElementIndex % 4) * 2
	newElemLengthCode := byte(newElementLength - 1)

	if newElemPrefixByteIdx < newPrefixBytes {
		newBuffer[newElemPrefixByteIdx] &= ^(0x03 << newElemBitOffset)
		newBuffer[newElemPrefixByteIdx] |= newElemLengthCode << newElemBitOffset
	}

	// Update prefix bits for adjusted next element
	adjustedNextPrefixByteIdx := (nextElementIndex + 1) / 4
	adjustedNextBitOffset := ((nextElementIndex + 1) % 4) * 2
	adjustedNextLengthCode := byte(adjustedNextElementLength - 1)

	if adjustedNextPrefixByteIdx < newPrefixBytes {
		newBuffer[adjustedNextPrefixByteIdx] &= ^(0x03 << adjustedNextBitOffset)
		newBuffer[adjustedNextPrefixByteIdx] |= adjustedNextLengthCode << adjustedNextBitOffset
	}

	// Shift prefix bits for all elements after the insertion point
	for i := nextElementIndex + 2; i < newElementCount; i++ {
		oldIdx := i - 1
		oldPrefixByteIdx := oldIdx / 4
		oldBitOffset := (oldIdx % 4) * 2

		newPrefixByteIdx := i / 4
		newBitOffset := (i % 4) * 2

		if oldPrefixByteIdx < oldPrefixBytes && newPrefixByteIdx < newPrefixBytes {
			lengthCode := (buffer[oldPrefixByteIdx] >> oldBitOffset) & 0x03
			newBuffer[newPrefixByteIdx] &= ^(0x03 << newBitOffset)
			newBuffer[newPrefixByteIdx] |= lengthCode << newBitOffset
		}
	}

	return needsRealloc, newBuffer, lastValue
}

// Enhanced ValueSelector for uint64
type ValueSelector struct {
	generatedValues []uint64
	sortedValues    []uint64
	rng             *rand.Rand
}

func NewValueSelector() *ValueSelector {
	return &ValueSelector{
		generatedValues: make([]uint64, 0),
		sortedValues:    make([]uint64, 0),
		rng:             rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (vs *ValueSelector) GenerateInitialValue(maxValue uint64) uint64 {
	var value uint64
	switch len(vs.generatedValues) % 4 {
	case 0:
		value = uint64(vs.rng.Intn(256)) // Small delta
	case 1:
		value = uint64(vs.rng.Intn(65536)) // Medium delta
	case 2:
		value = uint64(vs.rng.Intn(16777216)) // Large delta
	case 3:
		value = vs.rng.Uint64() % maxValue // Full range
	}

	vs.addValue(value)
	return value
}

func (vs *ValueSelector) GetNextValue() uint64 {
	if len(vs.sortedValues) == 0 {
		return vs.GenerateInitialValue(100)
	}

	prob := vs.rng.Float64()
	var nextValue uint64

	switch {
	case prob < 0.95:
		nextValue = vs.sortedValues[len(vs.sortedValues)-1] + vs.rng.Uint64()%100
		vs.sortedValues = append(vs.sortedValues, nextValue)
	case prob < 0.99:
		if len(vs.sortedValues) >= 2 {
			nextValue = vs.sortedValues[len(vs.sortedValues)-2] + uint64(vs.rng.Float32()*float32(vs.sortedValues[len(vs.sortedValues)-1]-vs.sortedValues[len(vs.sortedValues)-2]))
			temp := vs.sortedValues[len(vs.sortedValues)-1]
			vs.sortedValues[len(vs.sortedValues)-1] = nextValue
			vs.sortedValues = append(vs.sortedValues, temp)
		}
		nextValue = vs.sortedValues[len(vs.sortedValues)-1]
		vs.sortedValues = append(vs.sortedValues, nextValue)
	default:
		if len(vs.sortedValues) >= 3 {
			nextValue = vs.sortedValues[len(vs.sortedValues)-3] + uint64(vs.rng.Float32()*float32(vs.sortedValues[len(vs.sortedValues)-2]-vs.sortedValues[len(vs.sortedValues)-3]))
			temp := vs.sortedValues[len(vs.sortedValues)-1]
			vs.sortedValues[len(vs.sortedValues)-2], vs.sortedValues[len(vs.sortedValues)-1] = nextValue, vs.sortedValues[len(vs.sortedValues)-2]
			vs.sortedValues = append(vs.sortedValues, temp)
		}
		nextValue = vs.sortedValues[len(vs.sortedValues)-1]
		vs.sortedValues = append(vs.sortedValues, nextValue)
	}
	return nextValue
}

func (vs *ValueSelector) addValue(value uint64) {
	vs.generatedValues = append(vs.generatedValues, value)

	insertPos := sort.Search(len(vs.sortedValues), func(i int) bool {
		return vs.sortedValues[i] >= value
	})

	vs.sortedValues = append(vs.sortedValues, 0)
	copy(vs.sortedValues[insertPos+1:], vs.sortedValues[insertPos:])
	vs.sortedValues[insertPos] = value
}

func (vs *ValueSelector) Reset() {
	vs.generatedValues = vs.generatedValues[:0]
	vs.sortedValues = vs.sortedValues[:0]
}

// Helper functions
func generateTestValues(count int, maxValue uint64) []uint64 {
	vs := NewValueSelector()
	return generateTestValuesFromGenerator(count, maxValue, vs)
}

func generateTestValuesFromGenerator(count int, maxValue uint64, vs *ValueSelector) []uint64 {
	values := make([]uint64, count)

	if count > 0 {
		values[0] = vs.GenerateInitialValue(maxValue)
	}

	for i := 1; i < count; i++ {
		values[i] = vs.GenerateInitialValue(maxValue)
	}

	return values
}

func slicesEqualUint64(a, b []uint64) bool {
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

func TestDeltaEncoding() {
	fmt.Println("Testing Delta Encoding with uint64...")

	testValues := [][]uint64{
		{1, 2, 3, 4, 5},
		{127, 128, 129, 255, 256},
		{65535, 65536, 16777215, 16777216},
		{0, 1, 127, 128, 255, 256, 65535, 65536},
	}

	for i, values := range testValues {
		fmt.Printf("\nTest case %d: %v\n", i+1, values)

		// Test Varint Delta
		varintBuffer := make([]byte, 1024)
		varintSize := encodeVarintDeltas(values, varintBuffer)
		decodedValues := decodeVarintDeltas(varintBuffer[:varintSize], len(values))

		fmt.Printf("Varint Delta - Size: %d bytes\n", varintSize)
		fmt.Printf("  Decoded: %v\n", decodedValues)

		// Test Prefixed Delta
		prefixedBuffer := make([]byte, 1024)
		prefixedSize, prefixedLastValue := encodePrefixedDeltas(values, prefixedBuffer)
		decodedValues2 := decodePrefixedDeltas(prefixedBuffer[:prefixedSize], len(values))

		fmt.Printf("Prefixed Delta - Size: %d bytes, Last: %d\n", prefixedSize, prefixedLastValue)
		fmt.Printf("  Decoded: %v\n", decodedValues2)

		// Verify correctness
		originalSorted := make([]uint64, len(values))
		copy(originalSorted, values)
		slices.Sort(originalSorted)

		varintCorrect := slicesEqualUint64(decodedValues, originalSorted)
		prefixedCorrect := slicesEqualUint64(decodedValues2, originalSorted)

		fmt.Printf("Varint correct: %v, Prefixed correct: %v\n", varintCorrect, prefixedCorrect)
	}
}

func main() {
	TestDeltaEncoding()

	fmt.Println("\n=== Performance Testing ===")

	// Generate test data
	testValues := generateTestValues(32, 1_000_000)
	N := 1_000_000

	// Test encoding performance
	fmt.Printf("\nEncoding performance (%d iterations):\n", N)

	// Varint encoding
	varintBuffer := make([]byte, 1024)
	varintSize := 0
	start := time.Now()
	for i := 0; i < N; i++ {
		varintSize += encodeVarintDeltas(testValues, varintBuffer)
	}
	varintTime := time.Since(start)

	deltas := make([]uint32, len(testValues))
	start = time.Now()
	for i := 0; i < N; i++ {
		decodeVarintDeltas(varintBuffer, len(testValues))
	}
	varintDecodeTime := time.Since(start)

	// Prefixed encoding
	prefixedBuffer := make([]byte, 1024)
	prefixedSize := 0
	start = time.Now()
	for i := 0; i < N; i++ {
		s, _ := encodePrefixedDeltas(testValues, prefixedBuffer)
		prefixedSize += s
	}
	prefixedTime := time.Since(start)

	start = time.Now()
	for i := 0; i < N; i++ {
		decodePrefixedDeltas(prefixedBuffer, len(deltas))
	}
	prefixedDecodeTime := time.Since(start)

	fmt.Printf("Varint Delta:   %v\n", varintTime)
	fmt.Printf("Varint Decode:   %v\n", varintDecodeTime)
	fmt.Printf("Prefixed Delta: %v\n", prefixedTime)
	fmt.Printf("Prefixed Decode: %v\n", prefixedDecodeTime)
	fmt.Printf("Compresion rate: %.2fx\n", float64(varintSize)/float64(prefixedSize))
	fmt.Printf("Speedup encode: %.2fx\n", float64(varintTime)/float64(prefixedTime))
	fmt.Printf("Speedup decode: %.2fx\n", float64(varintDecodeTime)/float64(prefixedDecodeTime))

	// Test append performance
	fmt.Printf("\nAppend performance testing:\n")

	appendN := 10000
	maxElements := 32
	initialBufferSize := 1024

	for _, startSize := range []int{0, 8, 16, 24} {
		if startSize >= maxElements {
			continue
		}

		fmt.Printf("\nGrowing from %d to %d elements:\n", startSize, maxElements)

		elementsToAdd := maxElements - startSize
		varintEllapsed := time.Duration(0)
		totalReallocationsVarint := 0
		prefixedEllapsed := time.Duration(0)
		totalReallocationsPrefixed := 0
		for i := 0; i < appendN; i++ {
			// Generate initial data
			vs := NewValueSelector()
			initialValues := generateTestValuesFromGenerator(startSize, 1_000_000, vs)

			// Prepare initial buffers
			var varintInitBuffer []byte
			if startSize > 0 {
				varintInitBuffer = make([]byte, initialBufferSize)
				size := encodeVarintDeltas(initialValues, varintInitBuffer)
				varintInitBuffer = varintInitBuffer[:size]
			} else {
				varintInitBuffer = make([]byte, 0, initialBufferSize)
			}

			// Generate values to append
			var prefixedInitBuffer []byte
			var prefixedLastValue uint64
			if startSize > 0 {
				prefixedInitBuffer = make([]byte, initialBufferSize)
				size, lastVal := encodePrefixedDeltas(initialValues, prefixedInitBuffer)
				prefixedInitBuffer = prefixedInitBuffer[:size]
				prefixedLastValue = lastVal
			} else {
				prefixedInitBuffer = make([]byte, 0, initialBufferSize)
			}

			// Generate values to append
			for j := 0; j < elementsToAdd; j++ {
				nextElement := vs.GetNextValue()
				var reallocated bool

				// Benchmark Varint append
				start = time.Now()
				reallocated, varintInitBuffer = appendVarintDelta(nextElement, varintInitBuffer, startSize+j)
				varintEllapsed += time.Since(start)
				if reallocated {
					totalReallocationsVarint++
				}

				// Benchmark Prefixed append
				start = time.Now()
				reallocated, prefixedInitBuffer, prefixedLastValue = appendPrefixedDelta(nextElement, prefixedInitBuffer, startSize+j, prefixedLastValue)
				prefixedEllapsed += time.Since(start)
				if reallocated {
					totalReallocationsPrefixed++
				}
			}
		}

		fmt.Printf("Varint Append:   %v, Reallocations: %d\n", varintEllapsed, totalReallocationsVarint)
		fmt.Printf("Prefixed Append: %v, Reallocations: %d\n", prefixedEllapsed, totalReallocationsPrefixed)

		if prefixedEllapsed > 0 {
			fmt.Printf("Speedup: %.2fx\n", float64(varintEllapsed)/float64(prefixedEllapsed))
		}
	}
}
