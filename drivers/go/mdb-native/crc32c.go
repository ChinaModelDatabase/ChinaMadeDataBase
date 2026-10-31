package mdb

// CRC32C (Castagnoli) lookup table and implementation.
// Polynomial: 0x1EDC6F41

var crc32cTable [256]uint32

func init() {
	for i := uint32(0); i < 256; i++ {
		crc := i
		for j := 0; j < 8; j++ {
			if crc&1 != 0 {
				crc = (crc >> 1) ^ 0x82F63B78
			} else {
				crc >>= 1
			}
		}
		crc32cTable[i] = crc
	}
}

func updateCRC32C(crc uint32, data []byte) uint32 {
	crc = ^crc
	for _, b := range data {
		crc = crc32cTable[(crc^uint32(b))&0xFF] ^ (crc >> 8)
	}
	return ^crc
}

// CRC32C computes the CRC32C (Castagnoli) checksum of data.
func CRC32C(data []byte) uint32 {
	return updateCRC32C(0, data)
}
