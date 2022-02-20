#pragma once

#include <ros/exception.h>

namespace roscap {

//! Base class for roscap exceptions
class McapException : public ros::Exception
{
public:
    McapException(std::string const& msg) : ros::Exception(msg) { }
};

//! Exception thrown when on IO problems
class McapIOException : public McapException
{
public:
    McapIOException(std::string const& msg) : McapException(msg) { }
};

//! Exception thrown on problems reading the MCAP format
class McapFormatException : public McapException
{
public:
    McapFormatException(std::string const& msg) : McapException(msg) { }
};

//! Exception thrown on problems reading the MCAP index
class McapUnindexedException : public McapException
{
public:
    McapUnindexedException() : McapException("MCAP unindexed") { }
};

} // namespace roscap
