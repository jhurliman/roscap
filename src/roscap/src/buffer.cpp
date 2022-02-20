#include <stdlib.h>
#include <assert.h>
#include <utility>
#include <limits>

#include "roscap/buffer.h"

namespace roscap {

Buffer::Buffer() : buffer_(NULL), capacity_(0), size_(0) { }

Buffer::~Buffer() {
    free(buffer_);
}

uint8_t* Buffer::getData()           { return buffer_;   }
uint32_t Buffer::getCapacity() const { return capacity_; }
uint32_t Buffer::getSize()     const { return size_;     }

void Buffer::setSize(uint32_t size) {
    size_ = size;
    ensureCapacity(size);
}

void Buffer::ensureCapacity(uint32_t capacity) {
    if (capacity <= capacity_)
        return;

    if (capacity_ == 0)
        capacity_ = capacity;
    else {
        while (capacity_ < capacity)
        {
          if (static_cast<uint64_t>(capacity) * 2 > std::numeric_limits<uint32_t>::max())
            capacity_ = std::numeric_limits<uint32_t>::max();
          else
            capacity_ *= 2;
        }
    }

    buffer_ = (uint8_t*) realloc(buffer_, capacity_);
    assert(buffer_);
}

void Buffer::swap(Buffer& other) {
    using std::swap;
    swap(buffer_, other.buffer_);
    swap(capacity_, other.capacity_);
    swap(size_, other.size_);
}

} // namespace roscap
