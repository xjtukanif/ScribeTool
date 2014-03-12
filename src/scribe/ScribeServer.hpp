/**
 *    \file   ScribeServer.hpp
 *    \brief  a wrap of scribe server
 *    \date   12/14/2011
 *    \author kanif (xjtukanif@gmail.com) 
 */

#ifndef SCRIBESERVER_HPP_
#define SCRIBESERVER_HPP_

#include <boost/shared_ptr.hpp>
#include <boost/bind.hpp>
#include <ace/Thread_Manager.h>
#include <ace/Task.h>
#include <ace/Synch.h>
#include <event.h>
#include <arpa/inet.h>

#include "debug.h"

#include "ScribeHandler.hpp"

//using namespace apache::thrift;
//use namespace apache::thrift::concurrency;
//use namespace apache::thrift::protocol;
//use namespace apache::thrift::transport;
//use namespace apache::thrift::server;
//use namespace facebook::fb303;
//use namespace facebook;

//use namespace scribe::thrift;

#define MAX_MESSAGE_LENGTH 1024*1024

struct ScribeServerEntry
{
    ScribeServerEntry(const std::string& input_key, const std::string &input_value):
	key(input_key),
	value(input_value)
    {}

    std::string key;
    std::string value;
};

/**
    scribe协议的一个封装，用户只需指定一个AceTask作为该类的后继即可
*/
class ScribeServer
{
    public:
	ScribeServer(int server_thread_num, unsigned short server_port, 
		int max_connect_num=10, size_t max_message_length = MAX_MESSAGE_LENGTH, size_t scribe_full_retry_times = 2, size_t scribe_full_sleep_time= 100, bool scribe_if_full_drop_msg = false, bool remove_key=false):
		    server_name_("Unkown_Name"),
		    server_thread_num_(server_thread_num),
		    server_port_(server_port),
		    max_connect_num_(max_connect_num),
		    max_message_length_(max_message_length),
		    scribe_full_retry_times_(scribe_full_retry_times),
		    scribe_full_sleep_time_(scribe_full_sleep_time),
		    scribe_if_full_drop_msg_(scribe_if_full_drop_msg),
		    next_(NULL),
		    scribe_remove_key(remove_key)
		    //is_available_(true)
	{
	    on_message_cb_ = boost::bind(&ScribeServer::handleInputMessage, this, _1);
	}

	virtual ~ScribeServer(){
	    //pthread_cond_destroy(&available_cond_); 
	    //pthread_mutex_destroy(&lock_);
	}
	
	///设置该server的名称，打印日志时用来区分不同的server
	void setServerName(const std::string &server_name)
	{
	    server_name_ = server_name;
	}
    
	void setMessageCallBack(boost::function<bool (const std::vector<scribe::thrift::LogEntry>&)> on_message_cb)
	{
	    on_message_cb_ = on_message_cb;
	}

	int open()
	{
	    try
	    {
		if (sem_init(&sem, 0, 1) == -1){
			LOG_ERROR("sem_init error: %s", strerror(errno));
			exit(1);
		}
		/*
		pthread_mutex_init(&lock_, NULL);
		pthread_cond_init(&available_cond_, NULL);
		*/
		handler_.reset(new scribeHandler(server_port_));
		handler_->initialize(on_message_cb_);
		boost::shared_ptr<apache::thrift::TProcessor> processor(new scribe::thrift::scribeProcessor(handler_));
		/* This factory is for binary compatibility. */
		boost::shared_ptr<apache::thrift::protocol::TProtocolFactory> protocol_factory(
		    new apache::thrift::protocol::TBinaryProtocolFactory(0, 0, false, false)
		);

		boost::shared_ptr<apache::thrift::concurrency::ThreadManager> thread_manager;

		int numThriftServerThreads = server_thread_num_;

		{
		    // create a ThreadManager to process incoming calls
		    thread_manager = apache::thrift::concurrency::ThreadManager::newSimpleThreadManager(numThriftServerThreads);

		    boost::shared_ptr<apache::thrift::concurrency::PosixThreadFactory> thread_factory(new apache::thrift::concurrency::PosixThreadFactory());
		    thread_manager->threadFactory(thread_factory);
		    thread_manager->start();
		}

		server_.reset(new apache::thrift::server::TNonblockingServer(
			    processor,
			    protocol_factory,
			    handler_->port,
			    thread_manager
			    ));
		// throttle concurrent connections
		int mconn = max_connect_num_;
		server_->setMaxConnections(mconn);
		server_->setOverloadAction(apache::thrift::server::T_OVERLOAD_NO_ACTION);

	    } catch(const std::exception &e){
		LOG_ERROR("Exception in scribe server: %s\n", e.what());
		return -1;
	    }

	    return 0;
	}
	
	int svc()
	{
	    LOG_INFO("Starting scribe server %s on port %lu\n", server_name_.c_str(), handler_->port);
	    server_->serve();
	    LOG_INFO("ScribeServer %s stops\n", server_name_.c_str());
	    return 0;
	}
	
	///This function never return until exit
	void activate()
	{
	    svc();
	}

	void stop()
	{
	    if(next_ != NULL){
		if(next_->putq(new ACE_Message_Block(0, ACE_Message_Block::MB_STOP)) == -1){
		    LOG_INFO("ScribeServer %s is stopping, Put MB_STOP to next FAILURE %d\n", server_name_.c_str(), errno);
		
		}else{
		    LOG_INFO("ScribeServer %s is stopping, Put MB_STOP to next SUCCESS\n", server_name_.c_str());
		}
	    }else{
		LOG_INFO("ScribeServer %s is stopping\n", server_name_.c_str());
	    }
	    StopScribeServer();
	}
	
	///设置流水线的下一个环节
	void next(ACE_Task<ACE_SYNCH> *nexttask)
	{
	    next_ = nexttask;
	}
	
	///返回流水线的下一个环节
	ACE_Task<ACE_MT_SYNCH> * next()
	{
	    return next_;
	}

	bool handleInputMessage(const std::vector<scribe::thrift::LogEntry>&  messages)
	{
	    size_t i, size = 0;
    	    for(i = 0; i < messages.size(); i++)
		size += messages[i].message.size();
	    if(size > max_message_length_){
		LOG_WARNING("Input message len: %lu too long\n", size);
		return false;
	    } 

	    if(next_ == NULL){
		LOG_INFO("Recv %lu message, has no next.\n", messages.size());
		return true;
	    }

	    //等待超时时间
	    time_t time_out_ms = scribe_full_retry_times_*scribe_full_sleep_time_;
	    struct timespec abstime;
    	    struct timeval now;
	    gettimeofday(&now, NULL);
	    time_t time_nsec = now.tv_usec*1000 + time_out_ms*1000*1000;
	    abstime.tv_sec = now.tv_sec + time_nsec/(1000*1000*1000);
	    abstime.tv_nsec= time_nsec % (1000*1000*1000);
	    
	    int ret = 0;
	    while((ret = sem_timedwait(&sem, &abstime)) == -1 && errno == EINTR); 
	    //超时
	    if(ret == -1){
		if(errno == ETIMEDOUT){
			LOG_WARNING("Message Queue is Full, message count: %lu.\n", next_->msg_queue()->message_count());
			return false;
		}
		LOG_ERROR("sem_timewait error: %s\n", strerror(errno));
		return false;	
	    }

	    for(i=0; i < messages.size(); i++) {
		if (messages[i].category.empty()) {
		    LOG_ERROR("Received Empty Category, Drop Message\n");
		    continue;
		}
 
		std::string proc_msg;
		ScribeServerEntry *entry;
		if(scribe_remove_key && RemoveKey(messages[i].message, proc_msg)){
		    if(i == 0) LOG_INFO("Removed message key\n");	//只打一行log
		    entry = new ScribeServerEntry(messages[i].category, proc_msg);
		} else
		    entry = new ScribeServerEntry(messages[i].category, messages[i].message);

		ACE_Message_Block *msg_block = new ACE_Message_Block((char*)entry, sizeof(ScribeServerEntry));
		if(next_->putq(msg_block, NULL) == -1){
		    if (errno == ESHUTDOWN){
			LOG_WARNING("DROP_INPUT_MSG %s ShutingDown\n", proc_msg.c_str());
		    }
		    delete entry;
		    delete msg_block;
	    	    sem_post(&sem);
		    return false;
		}
	    }

	    sem_post(&sem);
	    return true;
	}

    private:
	bool RemoveKey(const std::string& msg, std::string& proc_msg){
		size_t m_pos = msg.substr(0, 5).find(":");
		if(m_pos == std::string::npos)
			return false;
		int i=0;
		for(; i < m_pos && isdigit(msg[i]); i++);
		if(i == m_pos){
			proc_msg = msg.substr(m_pos+1); 
			return true;
		}
		return false;
	}

	void StopScribeServer() {
	    //如何停止scribeserver，可参考http://hi.baidu.com/kanif/item/d4d94caaf3c37bf315329b99
	    handler_->setStatus(facebook::fb303::STOPPING);
	    struct event_base* event_base_ = server_->getEventBase();
	    struct timeval timeout;
	    timeout.tv_sec = 1;
	    timeout.tv_usec = 0;
	    event_base_loopexit(event_base_, &timeout);
	    #if SS_THRIFT_VERSION >= 700
	    server_->stop();
	    #else
	    int socket_fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	    apache::thrift::server::TConnection* conn = server_->createConnection(socket_fd, 0);
	    if (!conn->notifyServer()) {
		LOG_ERROR("Can't notifyServer!\n");
		socket_fd = socket(AF_INET, SOCK_STREAM, 0);
		assert(socket_fd > 0);
		struct sockaddr_in addr;
		bzero(&addr,sizeof(addr));
		addr.sin_family = AF_INET;
		addr.sin_port=htons(server_port_);
		addr.sin_addr.s_addr = inet_addr("127.0.0.1"); 
		if (connect(socket_fd, (const sockaddr*)&addr, sizeof(addr)) < 0) {
		    LOG_ERROR("Can't connect to Server!\n");
		    return;
		}
		close(socket_fd);
	    }
	    #endif
	}

	boost::shared_ptr<scribeHandler> handler_;
	boost::shared_ptr<apache::thrift::server::TNonblockingServer> server_;
	boost::function<bool (const std::vector<scribe::thrift::LogEntry>&)> on_message_cb_;
	std::string server_name_;
	int server_thread_num_;
	unsigned short server_port_;
	int max_connect_num_;
	size_t max_message_length_;
	int scribe_full_retry_times_;   ///times to retry when the queue is full
	int scribe_full_sleep_time_;    ///time to sleep when the queue is full
	bool scribe_if_full_drop_msg_;  ///whether to drop message when the queue is full
	ACE_Task<ACE_MT_SYNCH> *next_;
	bool scribe_remove_key;		///whether to remove "i:" in front of message

	sem_t sem;
	/*
	pthread_mutex_t lock_;
	pthread_cond_t available_cond_;
	bool is_available_;
	*/
};

#endif
