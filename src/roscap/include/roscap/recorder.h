#pragma once

#include <sys/stat.h>
#if !defined(_MSC_VER)
  #include <termios.h>
  #include <unistd.h>
#endif
#include <time.h>

#include <queue>
#include <string>
#include <vector>
#include <list>

#include <boost/thread/condition.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/regex.hpp>

#include <ros/ros.h>
#include <ros/time.h>

#include <std_msgs/Empty.h>
#include <std_msgs/String.h>
#include <topic_tools/shape_shifter.h>

#include "roscap/buffer.h"
#include "roscap/macros.h"
#include "mcap/writer.hpp"

namespace roscap {

enum struct CompressionType
{
    Uncompressed  = 0,
    ZSTD          = 1,
    LZ4           = 2,
};

class ROSCAP_DECL OutgoingMessage
{
public:
    OutgoingMessage(std::string const& _topic, topic_tools::ShapeShifter::ConstPtr _msg, boost::shared_ptr<ros::M_string> _connection_header, ros::Time _time);

    std::string                         topic;
    topic_tools::ShapeShifter::ConstPtr msg;
    boost::shared_ptr<ros::M_string>    connection_header;
    ros::Time                           time;
};

class ROSCAP_DECL OutgoingQueue
{
public:
    OutgoingQueue(std::string const& _filename, std::queue<OutgoingMessage>* _queue, ros::Time _time);

    std::string                  filename;
    std::queue<OutgoingMessage>* queue;
    ros::Time                    time;
};

class ROSCAP_DECL FileWriter final : public mcap::IWritable {
public:
    FileWriter() = default;
    ~FileWriter() override;

    void open(std::string_view filename);

    void handleWrite(const std::byte* data, uint64_t size) override;
    void end() override;
    uint64_t size() const override;

private:
    FILE* file_ = nullptr;
    uint64_t size_ = 0;
};

struct ROSCAP_DECL RecorderOptions
{
    RecorderOptions();

    bool            trigger;
    bool            record_all;
    bool            regex;
    bool            do_exclude;
    bool            quiet;
    bool            append_date;
    bool            snapshot;
    bool            verbose;
    bool            publish;
    bool            repeat_latched;
    CompressionType compression;
    std::string     prefix;
    std::string     name;
    boost::regex    exclude_regex;
    uint32_t        buffer_size;
    uint32_t        chunk_size;
    uint32_t        limit;
    bool            split;
    uint64_t        max_size;
    uint32_t        max_splits;
    ros::Duration   max_duration;
    std::string     node;
    unsigned long long min_space;
    std::string min_space_str;
    ros::TransportHints transport_hints;

    std::vector<std::string> topics;
};

class ROSCAP_DECL Recorder
{
public:
    Recorder(RecorderOptions const& options);

    void doTrigger();

    bool isSubscribed(std::string const& topic) const;

    boost::shared_ptr<ros::Subscriber> subscribe(std::string const& topic);

    int run();

private:
    void printUsage();

    void updateFilenames();
    void startWriting();
    void stopWriting();

    bool checkLogging();
    bool scheduledCheckDisk();
    bool checkDisk();

    void snapshotTrigger(std_msgs::Empty::ConstPtr trigger);
    //    void doQueue(topic_tools::ShapeShifter::ConstPtr msg, std::string const& topic, boost::shared_ptr<ros::Subscriber> subscriber, boost::shared_ptr<int> count);
    void doQueue(const ros::MessageEvent<topic_tools::ShapeShifter const>& msg_event, std::string const& topic, boost::shared_ptr<ros::Subscriber> subscriber, boost::shared_ptr<int> count);
    void doRecord();
    void checkNumSplits();
    bool checkSize();
    bool checkDuration(const ros::Time&);
    void doRecordSnapshotter();

    template<class T>
    bool mcapWrite(std::string const& topic, ros::MessageEvent<T> const& event);

    template<class T>
    bool mcapWrite(std::string const& topic, ros::Time const& time, T const& msg, boost::shared_ptr<ros::M_string> connection_header);

    template<class T>
    bool mcapWrite(std::string const& topic, ros::Time const& time, boost::shared_ptr<T const> const& msg, boost::shared_ptr<ros::M_string> connection_header);

    template<class T>
    bool mcapWrite(std::string const& topic, ros::Time const& time, boost::shared_ptr<T> const& msg, boost::shared_ptr<ros::M_string> connection_header);

    template<class T>
    bool doWrite(std::string const& topic, ros::Time const& time, T const& msg, boost::shared_ptr<ros::M_string> const& connection_header);

    template <class T>
    mcap::ChannelId createChannel(std::string const& topic, T const& msg);

    void doCheckMaster(ros::TimerEvent const& e, ros::NodeHandle& node_handle);

    bool shouldSubscribeToTopic(std::string const& topic, bool from_node = false);

    template<class T>
    static std::string timeToStr(T ros_t);

private:
    RecorderOptions               options_;
    mcap::McapWriterOptions       mcap_options_;

    FileWriter                    file_writer_;
    mcap::McapWriter              mcap_;
    std::unordered_map<std::string, mcap::ChannelId> topic_channel_ids_;
    std::map<ros::M_string, mcap::ChannelId> header_channel_ids_;
    std::unordered_map<std::string, mcap::SchemaId> md5sum_schema_ids_;
    mutable Buffer record_buffer_;                       //!< reusable buffer in which to assemble the record data before writing to file


    std::string                   target_filename_;
    std::string                   write_filename_;
    std::list<std::string>        current_files_;

    std::set<std::string>         currently_recording_;  //!< set of currently recording topics
    int                           num_subscribers_;      //!< used for book-keeping of our number of subscribers

    int                           exit_code_;            //!< eventual exit code

    std::map<std::pair<std::string, std::string>, OutgoingMessage> latched_msgs_;

    boost::condition_variable_any queue_condition_;      //!< conditional variable for queue
    boost::mutex                  queue_mutex_;          //!< mutex for queue
    std::queue<OutgoingMessage>*  queue_;                //!< queue for storing
    uint64_t                      queue_size_;           //!< queue size
    uint64_t                      max_queue_size_;       //!< max queue size

    uint64_t                      split_count_;          //!< split count

    std::queue<OutgoingQueue>     queue_queue_;          //!< queue of queues to be used by the snapshot recorders

    ros::Time                     last_buffer_warn_;

    ros::Time                     start_time_;

    bool                          writing_enabled_;
    boost::mutex                  check_disk_mutex_;
    ros::WallTime                 check_disk_next_;
    ros::WallTime                 warn_next_;

    ros::Publisher                pub_begin_write;
};

} // namespace roscap
