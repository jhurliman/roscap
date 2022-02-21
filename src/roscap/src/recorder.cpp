#include <sys/stat.h>
#include <boost/filesystem.hpp>
#include <time.h>

#include <queue>
#include <set>
#include <sstream>
#include <string>

#include <boost/lexical_cast.hpp>
#include <boost/regex.hpp>
#include <boost/thread.hpp>
#include <boost/thread/xtime.hpp>
#include <boost/date_time/local_time/local_time.hpp>

#include <ros/ros.h>
#include <topic_tools/shape_shifter.h>

#include "ros/network.h"
#include "ros/xmlrpc_manager.h"
#include "xmlrpcpp/XmlRpc.h"

#include "roscap/exceptions.h"
#include "roscap/recorder.h"

using std::cout;
using std::endl;
using std::set;
using std::string;
using std::vector;
using boost::shared_ptr;
using ros::Time;

namespace roscap {

mcap::McapWriterOptions RecorderOptionsToMcapWriterOptions(const RecorderOptions& options) {
    mcap::McapWriterOptions output{"ros1"};
    switch (options.compression) {
        case CompressionType::Uncompressed:
            output.compression = mcap::Compression::None;
            break;
        case CompressionType::ZSTD:
            output.compression = mcap::Compression::Zstd;
            break;
        case CompressionType::LZ4:
            output.compression = mcap::Compression::Lz4;
            break;
    }
    output.chunkSize = options.chunk_size;
    return output;
}

// OutgoingMessage

OutgoingMessage::OutgoingMessage(string const& _topic, topic_tools::ShapeShifter::ConstPtr _msg, boost::shared_ptr<ros::M_string> _connection_header, Time _time) :
    topic(_topic), msg(_msg), connection_header(_connection_header), time(_time)
{
}

// OutgoingQueue

OutgoingQueue::OutgoingQueue(string const& _filename, std::queue<OutgoingMessage>* _queue, Time _time) :
    filename(_filename), queue(_queue), time(_time)
{
}

// FileWriter

void FileWriter::open(std::string_view filename) {
    end();
    file_ = fopen(filename.data(), "wb");
    if (!file_) {
        throw McapIOException(mcap::StrFormat("Error opening file: {}", filename));
    }
}

FileWriter::~FileWriter() {
    end();
}

void FileWriter::handleWrite(const std::byte* data, uint64_t size) {
    size_t result = fwrite(data, 1, size, file_);
    if (result != size) {
        throw McapIOException(mcap::StrFormat("Error writing to file: writing {} bytes, wrote {} bytes", size, result));
    }
    size_ += size;
}

void FileWriter::end() {
    if (file_) {
        fclose(file_);
        file_ = nullptr;
    }
    size_ = 0;
}

uint64_t FileWriter::size() const {
    return size_;
}

// RecorderOptions

RecorderOptions::RecorderOptions() :
    trigger(false),
    record_all(false),
    regex(false),
    do_exclude(false),
    quiet(false),
    append_date(true),
    snapshot(false),
    verbose(false),
    publish(false),
    compression(CompressionType::Uncompressed),
    prefix(""),
    name(""),
    exclude_regex(),
    buffer_size(1048576 * 256),
    chunk_size(1024 * 768),
    limit(0),
    split(false),
    max_size(0),
    max_splits(0),
    max_duration(-1.0),
    node(""),
    min_space(1024 * 1024 * 1024),
    min_space_str("1G")
{
}

// Recorder

Recorder::Recorder(RecorderOptions const& options) :
    options_(options),
    mcap_options_("ros1"),
    num_subscribers_(0),
    exit_code_(0),
    queue_size_(0),
    split_count_(0),
    writing_enabled_(true)
{
}

int Recorder::run() {
    if (options_.trigger) {
        doTrigger();
        return 0;
    }

    if (options_.topics.size() == 0) {
        // Make sure limit is not specified with automatic topic subscription
        if (options_.limit > 0) {
            fprintf(stderr, "Specifying a count is not valid with automatic topic subscription.\n");
            return 1;
        }

        // Make sure topics are specified
        if (!options_.record_all && (options_.node == std::string(""))) {
            fprintf(stderr, "No topics specified.\n");
            return 1;
        }
    }

    ros::NodeHandle nh;
    if (!nh.ok())
        return 0;

    if (options_.publish)
    {
        pub_begin_write = nh.advertise<std_msgs::String>("begin_write", 1, true);
    }

    last_buffer_warn_ = Time();
    queue_ = new std::queue<OutgoingMessage>;

    // Subscribe to each topic
    if (!options_.regex) {
    	for (string const& topic : options_.topics)
            subscribe(topic);
    }

    if (!ros::Time::waitForValid(ros::WallDuration(2.0)))
      ROS_WARN("/use_sim_time set to true and no clock published.  Still waiting for valid time...");

    ros::Time::waitForValid();

    start_time_ = ros::Time::now();

    // Don't bother doing anything if we never got a valid time
    if (!nh.ok())
        return 0;

    ros::Subscriber trigger_sub;

    // Spin up a thread for writing to the file
    boost::thread record_thread;
    if (options_.snapshot)
    {
        record_thread = boost::thread([this]() {
          try
          {
            this->doRecordSnapshotter();
          }
          catch (const roscap::McapException& ex)
          {
            ROS_ERROR_STREAM(ex.what());
            exit_code_ = 1;
          }
          catch (const std::exception& ex)
          {
            ROS_ERROR_STREAM(ex.what());
            exit_code_ = 2;
          }
          catch (...)
          {
            ROS_ERROR_STREAM("Unknown exception thrown while recording MCAP, exiting.");
            exit_code_ = 3;
          }
        });

        // Subscribe to the snapshot trigger
        trigger_sub = nh.subscribe<std_msgs::Empty>("snapshot_trigger", 100, boost::bind(&Recorder::snapshotTrigger, this, boost::placeholders::_1));
    }
    else
    {
        record_thread = boost::thread([this]() {
          try
          {
            this->doRecord();
          }
          catch (const roscap::McapException& ex)
          {
            ROS_ERROR_STREAM(ex.what());
            exit_code_ = 1;
          }
          catch (const std::exception& ex)
          {
            ROS_ERROR_STREAM(ex.what());
            exit_code_ = 2;
          }
          catch (...)
          {
            ROS_ERROR_STREAM("Unknown exception thrown while recording MCAP, exiting.");
            exit_code_ = 3;
          }
        });
    }



    ros::Timer check_master_timer;
    if (options_.record_all || options_.regex || (options_.node != std::string("")))
    {
        // check for master first
        doCheckMaster(ros::TimerEvent(), nh);
        check_master_timer = nh.createTimer(ros::Duration(1.0), boost::bind(&Recorder::doCheckMaster, this, boost::placeholders::_1, boost::ref(nh)));
    }

    ros::AsyncSpinner s(10);
    s.start();

    record_thread.join();
    queue_condition_.notify_all();
    delete queue_;

    return exit_code_;
}

shared_ptr<ros::Subscriber> Recorder::subscribe(string const& topic) {
    ROS_INFO("Subscribing to %s", topic.c_str());

    ros::NodeHandle nh;
    shared_ptr<int> count(boost::make_shared<int>(options_.limit));
    shared_ptr<ros::Subscriber> sub(boost::make_shared<ros::Subscriber>());

    ros::SubscribeOptions ops;
    ops.topic = topic;
    ops.queue_size = 100;
    ops.md5sum = ros::message_traits::md5sum<topic_tools::ShapeShifter>();
    ops.datatype = ros::message_traits::datatype<topic_tools::ShapeShifter>();
    ops.helper = boost::make_shared<ros::SubscriptionCallbackHelperT<
        const ros::MessageEvent<topic_tools::ShapeShifter const> &> >(
            boost::bind(&Recorder::doQueue, this, boost::placeholders::_1, topic, sub, count));
    ops.transport_hints = options_.transport_hints;
    *sub = nh.subscribe(ops);

    currently_recording_.insert(topic);
    num_subscribers_++;

    return sub;
}

bool Recorder::isSubscribed(string const& topic) const {
    return currently_recording_.find(topic) != currently_recording_.end();
}

bool Recorder::shouldSubscribeToTopic(std::string const& topic, bool from_node) {
    // ignore already known topics
    if (isSubscribed(topic)) {
        return false;
    }

    // subtract exclusion regex, if any
    if(options_.do_exclude && boost::regex_match(topic, options_.exclude_regex)) {
        return false;
    }

    if(options_.record_all || from_node) {
        return true;
    }
    
    if (options_.regex) {
        // Treat the topics as regular expressions
	return std::any_of(
            std::begin(options_.topics), std::end(options_.topics),
            [&topic] (string const& regex_str){
                boost::regex e(regex_str);
                boost::smatch what;
                return boost::regex_match(topic, what, e, boost::match_extra);
            });
    }

    return std::find(std::begin(options_.topics), std::end(options_.topics), topic)
	    != std::end(options_.topics);
}

template<class T>
std::string Recorder::timeToStr(T ros_t)
{
    (void)ros_t;
    std::stringstream msg;
    const boost::posix_time::ptime now=
        boost::posix_time::second_clock::local_time();
    boost::posix_time::time_facet *const f=
        new boost::posix_time::time_facet("%Y-%m-%d-%H-%M-%S");
    msg.imbue(std::locale(msg.getloc(),f));
    msg << now;
    return msg.str();
}

//! Callback to be invoked to save messages into a queue
void Recorder::doQueue(const ros::MessageEvent<topic_tools::ShapeShifter const>& msg_event, string const& topic, shared_ptr<ros::Subscriber> subscriber, shared_ptr<int> count) {
    //void Recorder::doQueue(topic_tools::ShapeShifter::ConstPtr msg, string const& topic, shared_ptr<ros::Subscriber> subscriber, shared_ptr<int> count) {
    Time rectime = Time::now();
    
    if (options_.verbose)
        cout << "Received message on topic " << subscriber->getTopic() << endl;

    OutgoingMessage out(topic, msg_event.getMessage(), msg_event.getConnectionHeaderPtr(), rectime);
    
    {
        boost::mutex::scoped_lock lock(queue_mutex_);

        queue_->push(out);
        queue_size_ += out.msg->size();
        
        if (options_.repeat_latched)
        {
            ros::M_string::const_iterator it = out.connection_header->find("latching");
            if ((it != out.connection_header->end()) && (it->second == "1"))
            {
                ros::M_string::const_iterator it2 = out.connection_header->find("callerid");
                if (it2 != out.connection_header->end())
                {
                    latched_msgs_.insert({{subscriber->getTopic(), it2->second}, out});
                }
            }
        }

        // Check to see if buffer has been exceeded
        while (options_.buffer_size > 0 && queue_size_ > options_.buffer_size) {
            OutgoingMessage drop = queue_->front();
            queue_->pop();
            queue_size_ -= drop.msg->size();

            if (!options_.snapshot) {
                Time now = Time::now();
                if (now > last_buffer_warn_ + ros::Duration(5.0)) {
                    ROS_WARN("roscap record buffer exceeded.  Dropping oldest queued message.");
                    last_buffer_warn_ = now;
                }
            }
        }
    }
  
    if (!options_.snapshot)
        queue_condition_.notify_all();

    // If we are book-keeping count, decrement and possibly shutdown
    if ((*count) > 0) {
        (*count)--;
        if ((*count) == 0) {
            subscriber->shutdown();

            num_subscribers_--;

            if (num_subscribers_ == 0)
                ros::shutdown();
        }
    }
}

void Recorder::updateFilenames() {
    vector<string> parts;

    std::string prefix = options_.prefix;
    size_t ind = prefix.rfind(".mcap");

    if (ind != std::string::npos && ind == prefix.size() - 4)
    {
      prefix.erase(ind);
    }

    if (prefix.length() > 0)
        parts.push_back(prefix);
    if (options_.append_date)
        parts.push_back(timeToStr(ros::WallTime::now()));
    if (options_.split)
        parts.push_back(boost::lexical_cast<string>(split_count_));

    if (parts.size() == 0)
    {
      throw McapException("MCAP filename is empty (neither of these was specified: prefix, append_date, split)");
    }

    target_filename_ = parts[0];
    for (unsigned int i = 1; i < parts.size(); i++)
        target_filename_ += string("_") + parts[i];

    target_filename_ += string(".mcap");
    write_filename_ = target_filename_ + string(".active");
}

//! Callback to be invoked to actually do the recording
void Recorder::snapshotTrigger(std_msgs::Empty::ConstPtr trigger) {
    (void)trigger;
    updateFilenames();
    
    ROS_INFO("Triggered snapshot recording with name '%s'.", target_filename_.c_str());
    
    {
        boost::mutex::scoped_lock lock(queue_mutex_);
        queue_queue_.push(OutgoingQueue(target_filename_, queue_, Time::now()));
        queue_      = new std::queue<OutgoingMessage>;
        queue_size_ = 0;
    }

    queue_condition_.notify_all();
}

void Recorder::startWriting() {
    mcap_options_ = RecorderOptionsToMcapWriterOptions(options_);

    updateFilenames();

    try
    {
        file_writer_.open(write_filename_);
    }
    catch (const roscap::McapException& ex)
    {
        ROS_ERROR("Error writing: %s", ex.what());
        exit_code_ = 1;
        ros::shutdown();
    }

    mcap_.open(file_writer_, mcap_options_);
    ROS_INFO("Recording to '%s'.", target_filename_.c_str());

    if (options_.repeat_latched)
    {
        // Start each new MCAP file with copies of all latched messages.
        ros::Time now = ros::Time::now();
        // std::map<std::pair<std::string, std::string>, OutgoingMessage>
        for (auto const& [topicAndCallerId, messageEvent] : latched_msgs_)
        {
            // Overwrite the original receipt time, otherwise the new MCAP will
            // have a gap before the new messages start.
            mcapWrite(messageEvent.topic, now, *messageEvent.msg, nullptr);
        }
    }

    if (options_.publish)
    {
        std_msgs::String msg;
        msg.data = target_filename_.c_str();
        pub_begin_write.publish(msg);
    }
}

void Recorder::stopWriting() {
    ROS_INFO("Closing '%s'.", target_filename_.c_str());
    mcap_.close();
    rename(write_filename_.c_str(), target_filename_.c_str());
}

void Recorder::checkNumSplits()
{
    if(options_.max_splits>0)
    {
        current_files_.push_back(target_filename_);
        if(current_files_.size()>options_.max_splits)
        {
            int err = unlink(current_files_.front().c_str());
            if(err != 0)
            {
                ROS_ERROR("Unable to remove %s: %s", current_files_.front().c_str(), strerror(errno));
            }
            current_files_.pop_front();
        }
    }
}

bool Recorder::checkSize()
{
    if (options_.max_size > 0)
    {
        if (file_writer_.size() > options_.max_size)
        {
            if (options_.split)
            {
                stopWriting();
                split_count_++;
                checkNumSplits();
                startWriting();
            } else {
                ros::shutdown();
                return true;
            }
        }
    }
    return false;
}

bool Recorder::checkDuration(const ros::Time& t)
{
    if (options_.max_duration > ros::Duration(0))
    {
        if (t - start_time_ > options_.max_duration)
        {
            if (options_.split)
            {
                while (start_time_ + options_.max_duration < t)
                {
                    stopWriting();
                    split_count_++;
                    checkNumSplits();
                    start_time_ += options_.max_duration;
                    startWriting();
                }
            } else {
                ros::shutdown();
                return true;
            }
        }
    }
    return false;
}


//! Thread that actually does writing to file.
void Recorder::doRecord() {
    // Open MCAP file for writing
    startWriting();

    // Schedule the disk space check
    warn_next_ = ros::WallTime();

    try
    {
        checkDisk();
    }
    catch (const roscap::McapException& ex)
    {
        ROS_ERROR_STREAM(ex.what());
        exit_code_ = 1;
        stopWriting();
        return;
    }

    check_disk_next_ = ros::WallTime::now() + ros::WallDuration().fromSec(20.0);

    // Technically the queue_mutex_ should be locked while checking empty.
    // Except it should only get checked if the node is not ok, and thus
    // it shouldn't be in contention.
    ros::NodeHandle nh;
    while (nh.ok() || !queue_->empty()) {
        boost::unique_lock<boost::mutex> lock(queue_mutex_);

        bool finished = false;
        while (queue_->empty()) {
            if (!nh.ok()) {
                lock.release()->unlock();
                finished = true;
                break;
            }
            boost::xtime xt;
            boost::xtime_get(&xt, boost::TIME_UTC_);
            xt.nsec += 250000000;
            queue_condition_.timed_wait(lock, xt);
            if (checkDuration(ros::Time::now()))
            {
                finished = true;
                break;
            }
        }
        if (finished)
            break;

        OutgoingMessage out = queue_->front();
        queue_->pop();
        queue_size_ -= out.msg->size();
        
        lock.release()->unlock();
        
        if (checkSize())
            break;

        if (checkDuration(out.time))
            break;

        if (scheduledCheckDisk() && checkLogging()) {
            if (!mcapWrite(out.topic, out.time, *out.msg, out.connection_header)) {
                exit_code_ = 1;
                break;
            }
        }
    }

    stopWriting();
}

void Recorder::doRecordSnapshotter() {
    ros::NodeHandle nh;
  
    while (nh.ok() || !queue_queue_.empty()) {
        boost::unique_lock<boost::mutex> lock(queue_mutex_);
        while (queue_queue_.empty()) {
            if (!nh.ok())
                return;
            queue_condition_.wait(lock);
        }
        
        OutgoingQueue out_queue = queue_queue_.front();
        queue_queue_.pop();
        
        lock.release()->unlock();
        
        string target_filename = out_queue.filename;
        string write_filename  = target_filename + string(".active");

        try
        {
            file_writer_.open(write_filename);
        }
        catch (const roscap::McapException& ex)
        {
            ROS_ERROR("Error writing: %s", ex.what());
            return;
        }

        mcap_.open(file_writer_, mcap_options_);

        while (!out_queue.queue->empty()) {
            OutgoingMessage out = out_queue.queue->front();
            out_queue.queue->pop();

            if (!mcapWrite(out.topic, out.time, *out.msg, nullptr)) {
                break;
            }
        }

        stopWriting();
    }
}

template<class T>
bool Recorder::mcapWrite(std::string const& topic, ros::MessageEvent<T> const& event) {
    return doWrite(topic, event.getReceiptTime(), *event.getMessage(), event.getConnectionHeaderPtr());
}

template<class T>
bool Recorder::mcapWrite(std::string const& topic, ros::Time const& time, T const& msg, boost::shared_ptr<ros::M_string> connection_header) {
    return doWrite(topic, time, msg, connection_header);
}

template<class T>
bool Recorder::mcapWrite(std::string const& topic, ros::Time const& time, boost::shared_ptr<T const> const& msg, boost::shared_ptr<ros::M_string> connection_header) {
    return doWrite(topic, time, *msg, connection_header);
}

template<class T>
bool Recorder::mcapWrite(std::string const& topic, ros::Time const& time, boost::shared_ptr<T> const& msg, boost::shared_ptr<ros::M_string> connection_header) {
    return doWrite(topic, time, *msg, connection_header);
}

template<class T>
bool Recorder::doWrite(std::string const& topic, ros::Time const& time, T const& msg, boost::shared_ptr<ros::M_string> const& connection_header) {
    if (time < ros::TIME_MIN)
    {
        throw McapException("Tried to insert a message with time less than ros::TIME_MIN");
    }

    // Whenever we write we increment our revision
    // bag_revision_++;

    mcap::ChannelId channel_id = 0;
    if (!connection_header) {
        // No connection header: look up by topic

        auto topic_channel_ids_it = topic_channel_ids_.find(topic);
        if (topic_channel_ids_it == topic_channel_ids_.end()) {
            channel_id = createChannel(topic, msg);
            topic_channel_ids_[topic] = channel_id;
        } else {
            channel_id = topic_channel_ids_it->second;
        }
    } else {
        // Store the connection info by the address of the connection header

        // Add the topic name to the connection header, so that when we later search by 
        // connection header, we can disambiguate connections that differ only by topic name (i.e.,
        // same callerid, same message type), #3755.  This modified connection header is only used
        // for our bookkeeping, and will not appear in the resulting .mcap file.
        ros::M_string connection_header_copy(*connection_header);
        connection_header_copy["topic"] = topic;

        auto header_channel_ids_it = header_channel_ids_.find(connection_header_copy);
        if (header_channel_ids_it == header_channel_ids_.end()) {
            channel_id = createChannel(topic, msg);
            header_channel_ids_[connection_header_copy] = channel_id;
        } else {
            channel_id = header_channel_ids_it->second;
        }
    }

    // Write the message data
    uint32_t msg_ser_len = ros::serialization::serializationLength(msg);
    record_buffer_.setSize(msg_ser_len);
    
    ros::serialization::OStream s(record_buffer_.getData(), msg_ser_len);
    ros::serialization::serialize(s, msg);

    mcap::Message message;
    message.channelId = channel_id;
    message.sequence = 0;
    message.logTime = time.toNSec();
    message.publishTime = message.logTime;
    message.dataSize = msg_ser_len;
    message.data = reinterpret_cast<const std::byte*>(record_buffer_.getData());
    const auto status = mcap_.write(message);
    if (!status.ok()) {
        ROS_ERROR("Error writing: %s", status.message.c_str());
        return false;
    }
    return true;
}

template <class T>
mcap::ChannelId Recorder::createChannel(std::string const& topic, T const& msg) {
    mcap::SchemaId schema_id = 0;

    // Find or create the schema
    auto md5sum = std::string(ros::message_traits::md5sum(msg));
    auto md5sum_schema_ids_it = md5sum_schema_ids_.find(md5sum);
    if (md5sum_schema_ids_it == md5sum_schema_ids_.end()) {
        // No mcap::Schema for this md5sum: create one
        auto datatype = std::string(ros::message_traits::datatype(msg));
        auto msg_def  = std::string(ros::message_traits::definition(msg));
        mcap::Schema schema{datatype, "ros1msg", msg_def};
        mcap_.addSchema(schema);
        schema_id = schema.id;
        md5sum_schema_ids_[md5sum] = schema_id;
    } else {
        schema_id = md5sum_schema_ids_it->second;
    }

    // Create the channel
    mcap::KeyValueMap metadata = { {"md5sum", md5sum} };
    mcap::Channel channel{topic, "ros1", schema_id, metadata};
    mcap_.addChannel(channel);
    return channel.id;
}

void Recorder::doCheckMaster(ros::TimerEvent const& e, ros::NodeHandle& node_handle) {
    (void)e;
    (void)node_handle;
    ros::master::V_TopicInfo topics;
    if (ros::master::getTopics(topics)) {
	for (ros::master::TopicInfo const& t : topics) {
	    if (shouldSubscribeToTopic(t.name))
	        subscribe(t.name);
	}
    }
    
    if (options_.node != std::string(""))
    {

      XmlRpc::XmlRpcValue req;
      req[0] = ros::this_node::getName();
      req[1] = options_.node;
      XmlRpc::XmlRpcValue resp;
      XmlRpc::XmlRpcValue payload;

      if (ros::master::execute("lookupNode", req, resp, payload, true))
      {
        std::string peer_host;
        uint32_t peer_port;

        if (!ros::network::splitURI(static_cast<std::string>(resp[2]), peer_host, peer_port))
        {
          ROS_ERROR("Bad xml-rpc URI trying to inspect node at: [%s]", static_cast<std::string>(resp[2]).c_str());
        } else {

          XmlRpc::XmlRpcClient c(peer_host.c_str(), peer_port, "/");
          XmlRpc::XmlRpcValue req2;
          XmlRpc::XmlRpcValue resp2;
          req2[0] = ros::this_node::getName();
          c.execute("getSubscriptions", req2, resp2);
          
          if (!c.isFault() && resp2.valid() && resp2.size() > 0 && static_cast<int>(resp2[0]) == 1)
          {
            for(int i = 0; i < resp2[2].size(); i++)
            {
              if (shouldSubscribeToTopic(resp2[2][i][0], true))
                subscribe(resp2[2][i][0]);
            }
          } else {
            ROS_ERROR("Node at: [%s] failed to return subscriptions.", static_cast<std::string>(resp[2]).c_str());
          }
        }
      }
    }
}

void Recorder::doTrigger() {
    ros::NodeHandle nh;
    ros::Publisher pub = nh.advertise<std_msgs::Empty>("snapshot_trigger", 1, true);
    pub.publish(std_msgs::Empty());

    ros::Timer terminate_timer = nh.createTimer(ros::Duration(1.0), boost::bind(&ros::shutdown));
    ros::spin();
}

bool Recorder::scheduledCheckDisk() {
    boost::mutex::scoped_lock lock(check_disk_mutex_);

    if (ros::WallTime::now() < check_disk_next_)
        return true;

    check_disk_next_ += ros::WallDuration().fromSec(20.0);
    return checkDisk();
}

bool Recorder::checkDisk() {
    boost::filesystem::path p(boost::filesystem::system_complete(write_filename_.c_str()));
    p = p.parent_path();
    boost::filesystem::space_info info;
    try
    {
        info = boost::filesystem::space(p);
    }
    catch (const boost::filesystem::filesystem_error& e) 
    { 
        ROS_WARN("Failed to check filesystem stats [%s].", e.what());
        writing_enabled_ = false;   
        return false;
    }
    if ( info.available < options_.min_space)
    {
        writing_enabled_ = false;
        throw McapException("Less than " + options_.min_space_str + " of space free on disk with " + write_filename_ + ". Disabling recording.");
    }
    else if (info.available < 5 * options_.min_space)
    {
        ROS_WARN("Less than 5 x %s of space free on disk with '%s'.", options_.min_space_str.c_str(), write_filename_.c_str());
        writing_enabled_ = true;
    }
    else
    {
        writing_enabled_ = true;
    }
    return true;
}

bool Recorder::checkLogging() {
    if (writing_enabled_)
        return true;

    ros::WallTime now = ros::WallTime::now();
    if (now >= warn_next_) {
        warn_next_ = now + ros::WallDuration().fromSec(5.0);
        ROS_WARN("Not logging message because logging disabled. Most likely cause is a full disk.");
    }
    return false;
}

} // namespace roscap
