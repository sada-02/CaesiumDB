#include <cstdint>

#ifndef DECODE_H
#define DECODE_H

struct Coordinates {
    double latitude;
    double longitude;
};

Coordinates decode(uint64_t geo_code);

#endif 