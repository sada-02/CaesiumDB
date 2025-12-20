#include <iostream>
#include <cstdint>
#include <cmath>
#include <vector>
#include <string>

constexpr double MIN_LATITUDE = -85.05112878;
constexpr double MAX_LATITUDE = 85.05112878;
constexpr double MIN_LONGITUDE = -180.0;
constexpr double MAX_LONGITUDE = 180.0;

constexpr double LATITUDE_RANGE = MAX_LATITUDE - MIN_LATITUDE;
constexpr double LONGITUDE_RANGE = MAX_LONGITUDE - MIN_LONGITUDE;

struct Coordinates {
    double latitude;
    double longitude;
};

uint32_t compact_int64_to_int32(uint64_t v) {
    v = v & 0x5555555555555555ULL;
    v = (v | (v >> 1)) & 0x3333333333333333ULL;
    v = (v | (v >> 2)) & 0x0F0F0F0F0F0F0F0FULL;
    v = (v | (v >> 4)) & 0x00FF00FF00FF00FFULL;
    v = (v | (v >> 8)) & 0x0000FFFF0000FFFFULL;
    v = (v | (v >> 16)) & 0x00000000FFFFFFFFULL;
    return static_cast<uint32_t>(v);
}

Coordinates convert_grid_numbers_to_coordinates(uint32_t grid_latitude_number, uint32_t grid_longitude_number) {
    // Calculate the grid boundaries
    double grid_latitude_min = MIN_LATITUDE + LATITUDE_RANGE * (grid_latitude_number / pow(2, 26));
    double grid_latitude_max = MIN_LATITUDE + LATITUDE_RANGE * ((grid_latitude_number + 1) / pow(2, 26));
    double grid_longitude_min = MIN_LONGITUDE + LONGITUDE_RANGE * (grid_longitude_number / pow(2, 26));
    double grid_longitude_max = MIN_LONGITUDE + LONGITUDE_RANGE * ((grid_longitude_number + 1) / pow(2, 26));
    
    // Calculate the center point of the grid cell
    Coordinates result;
    result.latitude = (grid_latitude_min + grid_latitude_max) / 2;
    result.longitude = (grid_longitude_min + grid_longitude_max) / 2;
    
    return result;
}

Coordinates decode(uint64_t geo_code) {
    // Align bits of both latitude and longitude to take even-numbered position
    uint64_t y = geo_code >> 1;
    uint64_t x = geo_code;
    
    // Compact bits back to 32-bit ints
    uint32_t grid_latitude_number = compact_int64_to_int32(x);
    uint32_t grid_longitude_number = compact_int64_to_int32(y);
    
    return convert_grid_numbers_to_coordinates(grid_latitude_number, grid_longitude_number);
}

struct TestCase {
    std::string name;
    double expected_latitude;
    double expected_longitude;
    uint64_t score;
};