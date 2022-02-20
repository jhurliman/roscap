#include "roscap/recorder.h"
#include "roscap/exceptions.h"

#include "boost/program_options.hpp"
#include <signal.h>
#include <string>
#include <sstream>

namespace po = boost::program_options;

//! Parse the command-line arguments for recorder options
roscap::RecorderOptions parseOptions(int argc, char** argv) {
    roscap::RecorderOptions opts;

    po::options_description desc("Allowed options");

    desc.add_options()
      ("help,h", "produce help message")
      ("all,a", "record all topics")
      ("regex,e", "match topics using regular expressions")
      ("exclude,x", po::value<std::string>(), "exclude topics matching regular expressions")
      ("quiet,q", "suppress console output")
      ("publish,p", "Publish a msg when the record begin")
      ("output-prefix,o", po::value<std::string>(), "prepend PREFIX to beginning of MCAP name")
      ("output-name,O", po::value<std::string>(), "record file named NAME.mcap")
      ("buffsize,b", po::value<int>()->default_value(256), "Use an internal buffer of SIZE MB (Default: 256)")
      ("chunksize", po::value<int>()->default_value(768), "Set chunk size of message data, in KB (Default: 768. Advanced)")
      ("limit,l", po::value<int>()->default_value(0), "Only record NUM messages on each topic")
      ("min-space,L", po::value<std::string>()->default_value("1G"), "Minimum allowed space on recording device (use G,M,k multipliers)")
      ("zstd,z", "use ZSTD compression")
      ("lz4", "use LZ4 compression")
      ("split", po::value<int>()->implicit_value(0), "Split the MCAP file and continue recording when maximum size or maximum duration reached.")
      ("max-splits", po::value<int>(), "Keep a maximum of N MCAP files, when reaching the maximum erase the oldest one to keep a constant number of files.")
      ("topic", po::value< std::vector<std::string> >(), "topic to record")
      ("size", po::value<uint64_t>(), "The maximum size of the MCAP to record in MB.")
      ("duration", po::value<std::string>(), "Record a MCAP of maximum duration in seconds, unless 'm', or 'h' is appended.")
      ("node", po::value<std::string>(), "Record all topics subscribed to by a specific node.")
      ("tcpnodelay", "Use the TCP_NODELAY transport hint when subscribing to topics.")
      ("udp", "Use the UDP transport hint when subscribing to topics.")
      ("repeat-latched", "Repeat latched msgs at the start of each new MCAP file.");

  
    po::positional_options_description p;
    p.add("topic", -1);
    
    po::variables_map vm;
    
    try 
    {
      po::store(po::command_line_parser(argc, argv).options(desc).positional(p).run(), vm);
    } catch (const boost::program_options::invalid_command_line_syntax& e)
    {
      throw ros::Exception(e.what());
    } catch (const boost::program_options::unknown_option& e)
    {
      throw ros::Exception(e.what());
    }

    if (vm.count("help")) {
      std::cout << desc << std::endl;
      exit(0);
    }

    if (vm.count("all"))
      opts.record_all = true;
    if (vm.count("regex"))
      opts.regex = true;
    if (vm.count("exclude"))
    {
      opts.do_exclude = true;
      opts.exclude_regex = vm["exclude"].as<std::string>();
    }
    if (vm.count("quiet"))
      opts.quiet = true;
    if (vm.count("publish"))
      opts.publish = true;
    if (vm.count("repeat-latched"))
      opts.repeat_latched = true;
    if (vm.count("output-prefix"))
    {
      opts.prefix = vm["output-prefix"].as<std::string>();
      opts.append_date = true;
    }
    if (vm.count("output-name"))
    {
      opts.prefix = vm["output-name"].as<std::string>();
      opts.append_date = false;
    }
    if (vm.count("split"))
    {
      opts.split = true;

      int S = vm["split"].as<int>();
      if (S != 0)
      {
        ROS_WARN("Use of \"--split <MAX_SIZE>\" has been deprecated.  Please use --split --size <MAX_SIZE> or --split --duration <MAX_DURATION>");
        if (S < 0)
          throw ros::Exception("Split size must be 0 or positive");
        opts.max_size = 1048576 * static_cast<uint64_t>(S);
      }
    }
    if(vm.count("max-splits"))
    {
        if(!opts.split)
        {
            ROS_WARN("--max-splits is ignored without --split");
        }
        else
        {
            opts.max_splits = vm["max-splits"].as<int>();
        }
    }
    if (vm.count("buffsize"))
    {
      int m = vm["buffsize"].as<int>();
      if (m < 0)
        throw ros::Exception("Buffer size must be 0 or positive");
      opts.buffer_size = 1048576 * m;
    }
    if (vm.count("chunksize"))
    {
      int chnk_sz = vm["chunksize"].as<int>();
      if (chnk_sz < 0)
        throw ros::Exception("Chunk size must be 0 or positive");
      opts.chunk_size = 1024 * chnk_sz;
    }
    if (vm.count("limit"))
    {
      opts.limit = vm["limit"].as<int>();
    }
    if (vm.count("min-space"))
    {
        std::string ms = vm["min-space"].as<std::string>();
        long long int value = 1073741824ull;
        char mul = 0;
        // Sane default values, just in case
        opts.min_space_str = "1G";
        opts.min_space = value;
        if (sscanf(ms.c_str(), " %lld%c", &value, &mul) > 0) {
            opts.min_space_str = ms;
            switch (mul) {
                case 'G':
                case 'g':
                    opts.min_space = value * 1073741824ull;
                    break;
                case 'M':
                case 'm':
                    opts.min_space = value * 1048576ull;
                    break;
                case 'K':
                case 'k':
                    opts.min_space = value * 1024ull;
                    break;
                default:
                    opts.min_space = value;
                    break;
            }
        }
        ROS_DEBUG("roscap using minimum space of %lld bytes, or %s", opts.min_space, opts.min_space_str.c_str());
    }
    if (vm.count("bz2") && vm.count("lz4"))
    {
      throw ros::Exception("Can only use one type of compression");
    }
    if (vm.count("zstd"))
    {
      opts.compression = roscap::CompressionType::ZSTD;
    }
    if (vm.count("lz4"))
    {
      opts.compression = roscap::CompressionType::LZ4;
    }
    if (vm.count("duration"))
    {
      std::string duration_str = vm["duration"].as<std::string>();

      double duration;
      double multiplier = 1.0;
      std::string unit("");

      std::istringstream iss(duration_str);
      if ((iss >> duration).fail())
        throw ros::Exception("Duration must start with a floating point number.");

      if ( (!iss.eof() && ((iss >> unit).fail())) )
      {
        throw ros::Exception("Duration unit must be s, m, or h");
      }
      if (unit == std::string(""))
        multiplier = 1.0;
      else if (unit == std::string("s"))
        multiplier = 1.0;
      else if (unit == std::string("m"))
        multiplier = 60.0;
      else if (unit == std::string("h"))
        multiplier = 3600.0;
      else
        throw ros::Exception("Duration unit must be s, m, or h");

      
      opts.max_duration = ros::Duration(duration * multiplier);
      if (opts.max_duration <= ros::Duration(0))
        throw ros::Exception("Duration must be positive.");
    }
    if (vm.count("size"))
    {
      opts.max_size = vm["size"].as<uint64_t>() * 1048576;
      if (opts.max_size <= 0)
        throw ros::Exception("Split size must be 0 or positive");
    }
    if (vm.count("node"))
    {
      opts.node = vm["node"].as<std::string>();
      std::cout << "Recording from: " << opts.node << std::endl;
    }
    if (vm.count("tcpnodelay"))
    {
      opts.transport_hints.tcpNoDelay();
    }
    if (vm.count("udp"))
    {
      opts.transport_hints.udp();
    }

    // Every non-option argument is assumed to be a topic
    if (vm.count("topic"))
    {
      std::vector<std::string> caps = vm["topic"].as< std::vector<std::string> >();
      std::sort(caps.begin(), caps.end());
      caps.erase(std::unique(caps.begin(), caps.end()), caps.end());
      for (std::vector<std::string>::iterator i = caps.begin(); i != caps.end(); i++)
        opts.topics.push_back(*i);
    }


    // check that argument combinations make sense
    if(opts.exclude_regex.size() > 0 &&
            !(opts.record_all || opts.regex)) {
        fprintf(stderr, "Warning: Exclusion regex given, but no topics to subscribe to.\n"
                "Adding implicit 'record all'.");
        opts.record_all = true;
    }

    return opts;
}

/**
 * Handle SIGTERM to allow the recorder to cleanup by requesting a shutdown.
 * \param signal
 */
void signal_handler(int signal)
{
  (void) signal;
  ros::requestShutdown();
}

int main(int argc, char** argv) {
    ros::init(argc, argv, "record", ros::init_options::AnonymousName);

    // handle SIGTERM signals
    signal(SIGTERM, signal_handler);

    // Parse the command-line options
    roscap::RecorderOptions opts;
    try {
        opts = parseOptions(argc, argv);
    }
    catch (const ros::Exception& ex) {
        ROS_ERROR("Error reading options: %s", ex.what());
        return 1;
    }
    catch(const boost::regex_error& ex) {
        ROS_ERROR("Error reading options: %s\n", ex.what());
        return 1;
    }

    // Run the recorder
    roscap::Recorder recorder(opts);
    int result = recorder.run();
    
    return result;
}
