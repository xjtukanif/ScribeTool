/**
 *    \file   ScribeClient.hpp
 *    \brief  
 *    \date   12/15/2011
 *    \author kanif (xjtukanif@gmail.com)
 */

#ifndef __SCRIBE_CLIENT_HPP
#define __SCRIBE_CLIENT_HPP

#include <unistd.h> 
#include <ace/Task.h>
#include <ace/Synch.h>

#include "debug.h"
#include "common.h"
#include "Configuration.hpp"

#define SCRIBECLIENT_STORAGE_START_CODE "STARTCODE" 
#define SCRIBECLIENT_STORAGE_START_LEN 9
#define BATCH_EACH_SEND_NUM 100

class SingleScribeClient
{
    public:
	SingleScribeClient(const std::string &remote_addr, int remote_port, int timeout, int retry_period=60, int max_retry=-1)
	{
	    remote_addr_ = remote_addr;
	    remote_port_ = remote_port;
	    send_timeout_ = timeout;
	    max_retry_ = max_retry;
	    retry_period_ = retry_period;
	}

	int open()
	{
	    try
	    {
		socket_ = boost::shared_ptr<apache::thrift::transport::TSocket>(new apache::thrift::transport::TSocket(remote_addr_, remote_port_));
		if (!socket_){
		    throw std::runtime_error("Failed to create socket");
		}
		
		int timeout = send_timeout_;
		socket_->setConnTimeout(timeout);
		socket_->setRecvTimeout(timeout);
		socket_->setSendTimeout(timeout);

		framedTransport_ = boost::shared_ptr<apache::thrift::transport::TFramedTransport>(new apache::thrift::transport::TFramedTransport(socket_));
		if (!framedTransport_) {
		    throw std::runtime_error("Failed to create framed transport");
		}
		protocol_ = boost::shared_ptr<apache::thrift::protocol::TBinaryProtocol>(new apache::thrift::protocol::TBinaryProtocol(framedTransport_));
		if (!protocol_) {
		    throw std::runtime_error("Failed to create protocol");
		}
		protocol_->setStrict(false, false);
		client_ = boost::shared_ptr<scribe::thrift::scribeClient>(new scribe::thrift::scribeClient(protocol_));
		if (!client_) {
		    throw std::runtime_error("Failed to create network client");
		}

		framedTransport_->open();
		return 0;
	    }catch(apache::thrift::transport::TTransportException& ttx){
			LOG_ERROR("ScribeClient open error TTransportException\n");
		return -1;
	    }catch(std::exception& stx){
		LOG_ERROR("ScribeClient open error std::exception\n");
		return -1;
	    }
	    return -1;
	}
	
	int reconf(const std::string &remote_addr, int remote_port, int timeout, int retry_period, int max_retry=-1)
	{
	    remote_addr_ = remote_addr;
	    remote_port_ = remote_port;
	    send_timeout_ = timeout;
	    max_retry_ = max_retry;
	    retry_period_ = retry_period;

	    return open();
	}

	void close()
	{
	    try {
		framedTransport_->close();
	    } catch (apache::thrift::transport::TTransportException& ttx) {
		LOG_INFO("error <%s> while closing connection to remote scribe server",
			    ttx.what());
	    }
	}
	
	int send(const std::string &key, const std::string& value)
	{
	    msgs_.clear();
	    scribe::thrift::LogEntry log_entry;
	    log_entry.category= key;
	    log_entry.message = value;
	    msgs_.push_back(log_entry);
	    return _send(msgs_, max_retry_);
	}

	int send(std::vector<std::string>& key, const std::string& value)
	{
	    msgs_.clear();
	    for (std::vector<std::string>::iterator iter = key.begin(); 
		    iter != key.end(); ++iter) {
		scribe::thrift::LogEntry log_entry;
		log_entry.category = *iter;
		log_entry.message = value;
		msgs_.push_back(log_entry);
	    }
	    return _send(msgs_, max_retry_);
	}
	
	int send(const scribe::thrift::LogEntry &log_entry)
	{
	    msgs_.clear();
	    msgs_.push_back(log_entry);
	    return _send(msgs_, max_retry_);
	}

	int send(const std::vector<scribe::thrift::LogEntry>& messages) {
	    return _send(messages, max_retry_);  
	}

	int _send(const std::vector<scribe::thrift::LogEntry>& messages, int max_retry)
	{
	    while(_send(messages) != 0){
		if(max_retry == 0){
		    return -1;
		}else if(max_retry > 0){
		    max_retry --;
		}else if(max_retry < 0 && max_retry_ >= 0){
		    //修改配置，无限重试变为有限重试
		    max_retry = max_retry_; 
		}
		sleep(retry_period_);
	    }
	    return 0;
	}

	int _send(const std::vector<scribe::thrift::LogEntry>& messages)
	{
	    ScribeResultCode result = ScribeResultCodeNamespace::TRY_LATER;
	    try {
		result = client_->Log(messages);
		if (result == ScribeResultCodeNamespace::OK) {
		    return 0;
		} else {
		    LOG_WARNING("ScribeClient_Failed Overload %d: %s %d\n", (int)result, remote_addr_.c_str(), remote_port_);
		    return -1; // Don't retry here. If this server is overloaded they probably all are.
		}
	    } catch (apache::thrift::transport::TTransportException& ttx) {
		    LOG_ERROR("ScribeClient_Failed Transport %d\n", (int)result);
	    } catch (...) {
		    LOG_ERROR("ScribeClient_Failed Unknown %d\n", (int)result);
	    }

	    // we only get here if the send threw an exception
	    close();
	    if(open() == 0) {
		LOG_INFO("[scribeClient]reopened connection to remote scribe server(%s)\n", remote_addr_.c_str());
	    }else{
		LOG_ERROR("[scribeClient]reopened connection to remote scribe server(%s) error!\n", remote_addr_.c_str());
	    }
	    return -1;
	}

    private:
	boost::shared_ptr<apache::thrift::transport::TSocket> socket_;
	boost::shared_ptr<apache::thrift::transport::TFramedTransport> framedTransport_;
	boost::shared_ptr<apache::thrift::protocol::TBinaryProtocol> protocol_;
	boost::shared_ptr<scribe::thrift::scribeClient> client_;

	std::string remote_addr_;
	int remote_port_;
	int send_timeout_;
	int max_retry_;
	int retry_period_; 
	std::vector<scribe::thrift::LogEntry> msgs_;
};

class ScribeClient:public ACE_Task<ACE_MT_SYNCH>
{
    public:
	ScribeClient():
	    thread_num_(1),
	    taskname_("ScribeClient"),
	    main_thread_known_(false),
	    count_stopped_client_num_(0)
	{}

	int activate()
	{
	    return ACE_Task<ACE_MT_SYNCH>::activate(THR_NEW_LWP|THR_JOINABLE|THR_INHERIT_SCHED, thread_num_);
	}

	void notify_reconfig(const basetool::ConfigurationSection &config)
	{
	    config_ = config;
	    reconfig_flag_ = true;
	}
	
	void stop()
	{
	    should_stop_ = true;
	    putq(new ACE_Message_Block(0, ACE_Message_Block::MB_STOP));
	    LOG_INFO("%s should stop\n", taskname_.c_str());
	}
	
	int open(const basetool::ConfigurationSection &config)
	{
	    stopping_ = reconfig_flag_ = should_stop_ = should_stopping_ = false;
	    config_ = config;
	    thread_num_ = config_.Value<int>("ThreadNum", 1);
	    retry_period_ = config_.Value<int >("RetryPeriod", 60); //seconds
	    log_drop_ = config_.Value<bool>("LogDrop", false);
	    msg_queue()->high_water_mark(config_.Value<int>("HighWaterMark", 16*1024));
	    unfinished_store_file_ = config_.Value<std::string>("UnfinisheStoreFile","");
	    if (unfinished_store_file_.empty()) {
		store_at_exit_ = false;
	    } else {
		store_at_exit_ = true;
	    }
	    return doConfig();
	}

	int doConfig(bool reconf=false)
	{
	    reconfig_flag_ = false;
	    server_address_ = config_.Value<std::string>("Address", std::string());
	    server_port_ = config_.Value<int >("Port");
	    send_timeout_ = config_.Value<int >("Timeout", 1000); //ms
	    max_retry_ = config_.Value<int >("MaxRetry", -1);
	    taskname_ = "ScribeClient_" + server_address_;
	    return 0;
	}

	std::string serverAddress(){
	    return server_address_;
	}
	
	int serverPort(){
	    return server_port_;
	}
	
	int open(const std::string &remote_addr, int remote_port, int timeout = 1000, int retry_period=60, int max_retry=-1) {
	    server_address_ = remote_addr;
	    server_port_ = remote_port;
	    send_timeout_ = timeout; //ms
	    retry_period_ = retry_period;
	    max_retry_ = max_retry;
	    return 0;
	}

	int send(const std::string &key, unsigned cycle_id, const std::string& value){
		char proc_value_buf[100];
		snprintf(proc_value_buf, 100, "%u:", cycle_id);
		std::string proc_value = proc_value_buf;
		proc_value += value;
		return send(key, proc_value);
	}
	
	int send(const std::string &key, const std::string& value)
	{
	    scribe::thrift::LogEntry *log_entry = new scribe::thrift::LogEntry;
	    log_entry->category= key;
	    log_entry->message = value;

	    ACE_Message_Block *msg_blk = new ACE_Message_Block((char*)log_entry, sizeof(scribe::thrift::LogEntry));
	    if(putq(msg_blk) == -1){
		LOG_WARNING("DROP_INPUT_MSG %s\n", log_entry->message.c_str());
		delete log_entry;
		delete msg_blk;
	    }
	    return 0;
	}
	
	int send(const std::string &key, unsigned cycle_id, const std::string& value, ACE_Time_Value* timeout){
	   	char proc_value_buf[100];
		snprintf(proc_value_buf, 100, "%u:", cycle_id);
		std::string proc_value = proc_value_buf;
		proc_value += value;
		return send(key, proc_value, timeout);
	}
    
	int send(const std::string &key, const std::string& value, ACE_Time_Value* timeout)
	{
	    scribe::thrift::LogEntry *log_entry = new scribe::thrift::LogEntry;
	    log_entry->category= key;
	    log_entry->message = value;

	    ACE_Message_Block *msg_blk = new ACE_Message_Block((char*)log_entry, sizeof(scribe::thrift::LogEntry));
	    if(putq(msg_blk, timeout) == -1){
		LOG_WARNING("DROP_INPUT_MSG %s\n", log_entry->message.substr(0,4000).c_str());
		delete log_entry;
		delete msg_blk;
		return -1;
	    }
	    return 0;
	}

	int saveUnfinishedMessage() {
	    std::string unfinished_store_file_temp = unfinished_store_file_ + ".temp";
	    const char* storage_name = unfinished_store_file_temp.c_str();

	    int r = 0;
	    FILE *fp = NULL;
	    ACE_Message_Block *msg_blk = NULL;
	    ACE_Time_Value timeout;
	    size_t wr = 0;
	    size_t name_len = strlen(storage_name);
	    int msg_count = 0;
	    long msg_count_pos = 0;
	    if (name_len == 0) {
		r = -1;
		goto exit_save;
	    }

	    fp = fopen(storage_name,"wb+");
	    if (fp == NULL) {
		r = -2;
		goto exit_save;
	    }

	    wr = fwrite(SCRIBECLIENT_STORAGE_START_CODE, 1, SCRIBECLIENT_STORAGE_START_LEN, fp);
	    if (wr != SCRIBECLIENT_STORAGE_START_LEN) {
		r = -3;
		goto exit_save;
	    }

	    msg_count_pos = ftell(fp);
	    if (msg_count_pos == -1) {
		r = -4;
		goto exit_save;
	    }
	    //msa_count len: 20 bytes
	    if (fseek(fp, 20, SEEK_CUR) == -1) {
		r = -5;
		goto exit_save;
	    }
	    while (1) {
		timeout = ACE_OS::gettimeofday();
		timeout += 1;
		if (getq(msg_blk, &timeout) < 0) {
		    break;
		}
		
		if(msg_blk->msg_type() != ACE_Message_Block::MB_STOP) {
		    msg_count++;

		    scribe::thrift::LogEntry *log_entry = (scribe::thrift::LogEntry *)(msg_blk->base());
		    std::string category = log_entry->category;
		    std::string message = log_entry->message;
		    delete log_entry;
		    
		    //category len: 3 bytes
		    char category_size[3+1];
		    memset(category_size, 0, 3+1);
		    snprintf(category_size, 3+1, "%3zu", category.size());
		    wr = fwrite(category_size, 1, 3, fp);
		    if (wr != 3) {
			r = -3;
			break;
		    }

		    wr = fwrite(category.c_str(), 1, category.size(), fp);
		    if (wr != category.size()) {
			r = -3;
			break;
		    }

		    //message len: 10 bytes
		    char message_size[10+1];
		    memset(message_size, 0, 10+1);
		    snprintf(message_size, 10+1, "%10zu", message.size());
		    wr = fwrite(message_size, 1, 10, fp);
		    if (wr != 10) {
			r = -3;
			break;
		    }

		    wr = fwrite(message.c_str(), 1, message.size(), fp);
		    if (wr != message.size()) {
			r = -3;
			break;
		    }
		}
	    }

	    if (r == 0) {
		fseek(fp, msg_count_pos, SEEK_SET);
		//write msg_count
		char msg_count_size[20+1];
		memset(msg_count_size, 0, 20+1);
		snprintf(msg_count_size, 20+1, "%20d", msg_count);
		wr = fwrite(msg_count_size, 1, 20, fp);
		if (wr != 20) {
		    r = -3;
		    goto exit_save;
		}

	    }

exit_save:
	    if (fp) {
		fclose(fp);
	    }

	    if (r == 0) {
		if (rename(unfinished_store_file_temp.c_str(), unfinished_store_file_.c_str()) != 0) {
		    r = -6;
		}
	    }

	    if (r != 0) {
		unlink(unfinished_store_file_.c_str());
	    }
	    return r;
	}

	int loadUnfinishedMessage() {
	    const char* storage_name = unfinished_store_file_.c_str();
	    int r = 0;
	    FILE *fp = NULL;
	    size_t rd = 0;
	    size_t name_len = strlen(storage_name);
	    int msg_count;
	    if (name_len == 0)
	    {
		r = -1;
		goto exit_load;
	    }
	    fp = fopen(storage_name,"rb");
	    if (fp == NULL) {
		r = -2;
		goto exit_load;
	    }
	    //read start code
	    char start_code[SCRIBECLIENT_STORAGE_START_LEN + 1];
	    memset(start_code, 0, SCRIBECLIENT_STORAGE_START_LEN + 1);
	    rd = fread(start_code, 1, SCRIBECLIENT_STORAGE_START_LEN, fp);
	    if (rd != SCRIBECLIENT_STORAGE_START_LEN) {
		r = -3;
		goto exit_load;
	    }

	    char msg_count_array[20+1];
	    memset(msg_count_array, 0, 20+1);
	    rd = fread(msg_count_array, 1, 20, fp);
	    if (rd != 20) {
		r = -4;
		goto exit_load;
	    }
	    msg_count = atoi(msg_count_array);
	    
	    while (1) {
		if (msg_count <= 0) {
		    break;
		}
		msg_count--;
		//category len: 3 bytes
		char category_size_array[3+1];
		memset(category_size_array, 0, 3+1);
		rd = fread(category_size_array, 1, 3, fp);
		if (rd != 3) {
		    r = -3;
		    break;
		}

		int category_size = atoi(category_size_array);
		char* category = (char*)malloc(category_size+1);
		if (category == NULL) {
		    r = -4;
		    break;
		}
		category[category_size] = '\0';
		rd = fread(category, 1, category_size, fp);
		if (rd != category_size) {
		    r = -3;
		    break;
		}

		//message len: 10 bytes
		char message_size_array[10+1];
		memset(message_size_array, 0, 10+1);
		rd = fread(message_size_array, 1, 10, fp);
		if (rd != 10) {
		    r = -3;
		    break;
		}

		int message_size = atoi(message_size_array);
		char* message = (char*)malloc(message_size+1);
		if (message == NULL) {
		    r = -4;
		    free(category);
		    break;
		}
		message[message_size] = '\0';
		rd = fread(message, 1, message_size, fp);
		if (rd != message_size) {
		    r = -3;
		    break;
		}

		send(std::string(category, category_size), std::string(message, message_size));
		free(category);
		free(message);
	    }
	    
exit_load:
	    if (fp) {
		fclose(fp);
	    }

	    if (r == 0) {
		unlink(unfinished_store_file_.c_str());
	    }
	    return r;
	}

	static void* loadUnfinishedMessageThread(void *args) {
	    ScribeClient* client = (ScribeClient*)args;
	    
	    //load unfinished task
	    int r = 0;
	    if ((r = client->loadUnfinishedMessage()) == 0) {
		LOG_INFO("%s loadUnfinishedMessage Success\n", client->getTaskName().c_str());
	    } else {
		LOG_INFO("%s loadUnfinishedMessage Failed, ret %d\n", client->getTaskName().c_str(), r);
	    }

	    return NULL;
	}

	int svc()
	{
	    LOG_INFO("%s start\n", taskname_.c_str());
	    ACE_Message_Block *msg_blk = NULL;
	    ACE_Time_Value timeout;

	    bool current_thead_main = false;
	    if (!main_thread_known_ && !__sync_fetch_and_or(&main_thread_known_, true)) {
		current_thead_main = true;
		//unfinished message size may exceed the current thread highwater,
		//using another thread to load unfinished message to avoid deadlock
		if (isStoreAtExit()) {
		    ACE_Thread_Manager::instance()->spawn(loadUnfinishedMessageThread, this);
		}
	    }

	    boost::shared_ptr<SingleScribeClient> client;
	    client.reset(new SingleScribeClient(server_address_, server_port_, send_timeout_, retry_period_, 0));
	    client->open();
	    int max_retry;
	    try{
		while(!stopping_){
		    std::vector<scribe::thrift::LogEntry> messages;
		    timeout = ACE_OS::gettimeofday();
		    timeout += 1;
		    int cur_get_num = 0;
		    while(cur_get_num < BATCH_EACH_SEND_NUM && getq(msg_blk, &timeout) != -1){
			cur_get_num++;
			if(msg_blk->msg_type() == ACE_Message_Block::MB_STOP){
			    stopping_ = true;
			    //msg_queue()->deactivate();
			    LOG_INFO("%s STOP on MB_STOP msg\n", taskname_.c_str());
			    if(next()){
				next()->putq(msg_blk);
			    }else{
				delete msg_blk;
			    }

			    if(!store_at_exit_)
			    	LOG_INFO("%s stopping. Drop %lu message\n", taskname_.c_str(), messages.size());
			    else {
				for(int i=0; i<messages.size(); i++){
				    scribe::thrift::LogEntry *tmp_entry = new scribe::thrift::LogEntry(messages[i]);
	    			    msg_blk = new ACE_Message_Block((char*)tmp_entry, sizeof(scribe::thrift::LogEntry));
				    if(putq(msg_blk, &timeout) == -1){
					delete msg_blk; 
					goto out;
				    }
				}
			    }
			    goto out;
			}
		    	scribe::thrift::LogEntry *log_entry = (scribe::thrift::LogEntry *)(msg_blk->base());
			messages.push_back(*log_entry);
			delete log_entry;
			delete msg_blk;
		    }
		    //没有取到数据
		    if(messages.size() == 0)	
		    	continue;

		    if(reconfig_flag_){
			bool reconf = true;
			doConfig(reconf);
		    }
		    
		    max_retry = max_retry_;

		    if(should_stopping_){
			if (store_at_exit_) {
			    timeout = ACE_OS::gettimeofday();
			    timeout += 1;
			    for(int i=0; i<messages.size(); i++){
				scribe::thrift::LogEntry *tmp_entry = new scribe::thrift::LogEntry(messages[i]);
	    			msg_blk = new ACE_Message_Block((char*)&tmp_entry, sizeof(scribe::thrift::LogEntry));
				if (putq(msg_blk, &timeout) == -1){
				    LOG_INFO("%s stopping. Drop %lu message\n", taskname_.c_str(), messages.size()-i);
				    delete msg_blk;
				    goto out;
				}
			    }
			}
			//recv exit signal and send to server faild. drop all message
			LOG_INFO("%s stopping. Drop %lu message\n", taskname_.c_str(), messages.size());
			if (store_at_exit_) {
			    break;
			} else {
			    continue;
			}
		    }

		    //恶心的重试过程，主要是防止sever端故障时，client无法正常退出
		    while(client->send(messages) != 0){
			if(should_stop_){
			    LOG_INFO("%s SHOULD STOP on SEND_FAILD_AND_SHOULD_STOP\n", taskname_.c_str());
			    should_stopping_ = true;
			    break;
			}

			if(max_retry == 0){
			    if (log_drop_) 
				LOG_INFO("ScribeClient[%s] DropMsg: %lu msgs.\n", taskname_.c_str(), messages.size());	
			    break;
			}else if(max_retry > 0){
			    max_retry --;
			}else if(max_retry_ >= 0 && max_retry < 0){
			    max_retry = max_retry_; 
			}
			sleep(retry_period_);
		    }

		    if (should_stopping_ && store_at_exit_) {
			timeout = ACE_OS::gettimeofday();
			timeout += 1;
			for(int i=0; i<messages.size(); i++) {
			    scribe::thrift::LogEntry *tmp_entry = new scribe::thrift::LogEntry(messages[i]);
	    		    msg_blk = new ACE_Message_Block((char*)tmp_entry, sizeof(scribe::thrift::LogEntry));
			    if (putq(msg_blk, &timeout) == -1) {
				LOG_INFO("%s stopping. Drop %lu message\n", taskname_.c_str(), messages.size()-i);
				delete msg_blk;
				goto out;
			    }
			}
		    }
		}
out:
		if(1);	
	    }catch (std::runtime_error &e){
		LOG_ERROR("%s\n", e.what());
	    }catch (...){
		LOG_ERROR("Unknow exception\n");
	    }
	    client->close();

	    int stopped_client_num = __sync_add_and_fetch(&count_stopped_client_num_, 1);
	    if (thread_num_ == stopped_client_num && isStoreAtExit()) {
		//save unfinished task
		int r = 0;
		if ((r = saveUnfinishedMessage()) == 0) {
		    LOG_INFO("%s saveUnfinishedMessage Success\n", getTaskName().c_str());
		} else {
		    LOG_INFO("%s saveUnfinishedMessage Failed, ret %d\n", getTaskName().c_str(), r);
		}
	    }

	    LOG_INFO("%s stops\n", taskname_.c_str());
	    return 0;
	}
    
	void setTaskName(const std::string& name)
        {
	    taskname_ = name;
	}

	std::string getTaskName()
	{
	    return taskname_;
	}

	bool isStoreAtExit() {
	    return store_at_exit_;
	}

	const std::string& getServerAddress() const
	{
	    return server_address_;
	}

    private:				
	bool reconfig_flag_; //flag to active reconfig
	bool stopping_;
	bool should_stop_;
	bool should_stopping_; //recv exit signal and send to server failed
	int thread_num_;
	basetool::ConfigurationSection config_;  //ConfigurationSection for reconfig
	std::string taskname_;
	std::string server_address_;
	int server_port_;
	int send_timeout_;
	int max_retry_;
	int retry_period_;
	bool log_drop_;

	bool store_at_exit_;
	bool main_thread_known_;
	std::string unfinished_store_file_;
	int count_stopped_client_num_;
};

#endif
