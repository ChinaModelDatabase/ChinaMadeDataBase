#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

static uint32_t crc32c_table[256];
static bool crc32c_table_init = false;

static void crc32c_init_table(void) {
    const uint32_t poly = 0x82F63B78;
    for (int i = 0; i < 256; i++) {
        uint32_t crc = (uint32_t)i;
        for (int j = 0; j < 8; j++) {
            if (crc & 1)
                crc = (crc >> 1) ^ poly;
            else
                crc >>= 1;
        }
        crc32c_table[i] = crc;
    }
    crc32c_table_init = true;
}

uint32_t mdb_crc32c(const uint8_t *data, size_t len) {
    if (!crc32c_table_init) crc32c_init_table();
    uint32_t crc = 0xFFFFFFFF;
    for (size_t i = 0; i < len; i++) {
        crc = crc32c_table[(crc ^ data[i]) & 0xFF] ^ (crc >> 8);
    }
    return crc ^ 0xFFFFFFFF;
}
