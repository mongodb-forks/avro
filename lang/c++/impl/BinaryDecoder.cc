/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "Decoder.hh"
#include "Exception.hh"
#include "Zigzag.hh"
#include <algorithm>
#include <limits>
#include <memory>

namespace avro {

using std::make_shared;

namespace {

// Lower bound for the item-count check: arrays/maps of zero-byte elements (e.g.
// "null") can legitimately declare more items than there are remaining bytes.
constexpr size_t kMinItemCount = 1 << 20;

}  // namespace

class BinaryDecoder : public Decoder {
    StreamReader in_;

    void init(InputStream &is) final;
    void decodeNull() final;
    bool decodeBool() final;
    int32_t decodeInt() final;
    int64_t decodeLong() final;
    float decodeFloat() final;
    double decodeDouble() final;
    void decodeString(std::string &value) final;
    void skipString() final;
    void decodeBytes(std::vector<uint8_t> &value) final;
    void skipBytes() final;
    void decodeFixed(size_t n, std::vector<uint8_t> &value) final;
    void skipFixed(size_t n) final;
    size_t decodeEnum() final;
    size_t arrayStart() final;
    size_t arrayNext() final;
    size_t skipArray() final;
    size_t mapStart() final;
    size_t mapNext() final;
    size_t skipMap() final;
    size_t decodeUnionIndex() final;

    int64_t doDecodeLong();
    size_t doDecodeItemCount();
    size_t doDecodeLength();
    void drain() final;
};

DecoderPtr binaryDecoder() {
    return make_shared<BinaryDecoder>();
}

void BinaryDecoder::init(InputStream &is) {
    in_.reset(is);
}

void BinaryDecoder::decodeNull() {
}

bool BinaryDecoder::decodeBool() {
    auto v = in_.read();
    if (v == 0) {
        return false;
    } else if (v == 1) {
        return true;
    }
    throw Exception("Invalid value for bool: {}", v);
}

int32_t BinaryDecoder::decodeInt() {
    auto val = doDecodeLong();
    if (val < INT32_MIN || val > INT32_MAX) {
        throw Exception("Value out of range for Avro int: {}", val);
    }
    return static_cast<int32_t>(val);
}

int64_t BinaryDecoder::decodeLong() {
    return doDecodeLong();
}

float BinaryDecoder::decodeFloat() {
    float result;
    in_.readBytes(reinterpret_cast<uint8_t *>(&result), sizeof(float));
    return result;
}

double BinaryDecoder::decodeDouble() {
    double result;
    in_.readBytes(reinterpret_cast<uint8_t *>(&result), sizeof(double));
    return result;
}

size_t BinaryDecoder::doDecodeLength() {
    ssize_t len = decodeInt();
    if (len < 0) {
        throw Exception("Cannot have negative length: {}", len);
    }
    // A string/bytes length cannot exceed the bytes left to read.
    if (static_cast<size_t>(len) > in_.remaining()) {
        throw Exception("Avro length {} exceeds remaining input size {}",
                        len,
                        in_.remaining());
    }
    return len;
}

void BinaryDecoder::drain() {
    in_.drain(false);
}

void BinaryDecoder::decodeString(std::string &value) {
    size_t len = doDecodeLength();
    value.resize(len);
    if (len > 0) {
        in_.readBytes(const_cast<uint8_t *>(
                          reinterpret_cast<const uint8_t *>(value.c_str())),
                      len);
    }
}

void BinaryDecoder::skipString() {
    size_t len = doDecodeLength();
    in_.skipBytes(len);
}

void BinaryDecoder::decodeBytes(std::vector<uint8_t> &value) {
    size_t len = doDecodeLength();
    value.resize(len);
    if (len > 0) {
        in_.readBytes(value.data(), len);
    }
}

void BinaryDecoder::skipBytes() {
    size_t len = doDecodeLength();
    in_.skipBytes(len);
}

void BinaryDecoder::decodeFixed(size_t n, std::vector<uint8_t> &value) {
    value.resize(n);
    if (n > 0) {
        in_.readBytes(value.data(), n);
    }
}

void BinaryDecoder::skipFixed(size_t n) {
    in_.skipBytes(n);
}

size_t BinaryDecoder::decodeEnum() {
    return static_cast<size_t>(doDecodeLong());
}

size_t BinaryDecoder::arrayStart() {
    return doDecodeItemCount();
}

size_t BinaryDecoder::doDecodeItemCount() {
    // Capture remaining before consuming the count bytes.
    const size_t remaining = in_.remaining();
    auto result = doDecodeLong();
    size_t count;
    if (result < 0) {
        // Negative count: abs(count) items preceded by a block size in bytes.
        if (result == std::numeric_limits<int64_t>::min()) {
            throw Exception("Invalid Avro item count: {}", result);
        }
        auto blockSize = doDecodeLong();
        if (blockSize < 0) {
            throw Exception("Cannot have negative block size: {}", blockSize);
        }
        count = static_cast<size_t>(-result);
    } else {
        count = static_cast<size_t>(result);
    }
    if (count > std::max(remaining, kMinItemCount)) {
        throw Exception("Avro item count {} exceeds remaining input size {}", count, remaining);
    }
    return count;
}

size_t BinaryDecoder::arrayNext() {
    // Bounds the count and handles negative blocks, unlike a raw doDecodeLong().
    return doDecodeItemCount();
}

size_t BinaryDecoder::skipArray() {
    for (;;) {
        auto r = doDecodeLong();
        if (r < 0) {
            auto n = static_cast<size_t>(doDecodeLong());
            in_.skipBytes(n);
        } else {
            return static_cast<size_t>(r);
        }
    }
}

size_t BinaryDecoder::mapStart() {
    return doDecodeItemCount();
}

size_t BinaryDecoder::mapNext() {
    return doDecodeItemCount();
}

size_t BinaryDecoder::skipMap() {
    return skipArray();
}

size_t BinaryDecoder::decodeUnionIndex() {
    return static_cast<size_t>(doDecodeLong());
}

int64_t BinaryDecoder::doDecodeLong() {
    uint64_t encoded = 0;
    int shift = 0;
    uint8_t u;
    do {
        if (shift >= 64) {
            throw Exception("Invalid Avro varint");
        }
        u = in_.read();
        encoded |= static_cast<uint64_t>(u & 0x7f) << shift;
        shift += 7;
    } while (u & 0x80);

    return decodeZigzag64(encoded);
}

} // namespace avro
