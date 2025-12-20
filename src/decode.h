#include <cstdint>

#ifndef UTILS_H
#define UTILS_H

struct Coordinates {
    double latitude;
    double longitude;
};

Coordinates decode(uint64_t geo_code);

#endif 