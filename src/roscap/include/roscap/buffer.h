#pragma once

#include <stdint.h>
#include "macros.h"

namespace roscap {

class Buffer
{
public:
    Buffer();
    ~Buffer();

    uint8_t* getData();
    uint32_t getCapacity() const;
    uint32_t getSize()     const;

    void setSize(uint32_t size);
    void swap(Buffer& other);

private:
    Buffer(const Buffer&);
    Buffer& operator=(const Buffer&);
    void ensureCapacity(uint32_t capacity);

private:
    uint8_t* buffer_;
    uint32_t capacity_;
    uint32_t size_;
};

inline void swap(Buffer& a, Buffer& b) {
    a.swap(b);
}

} // namespace roscap
