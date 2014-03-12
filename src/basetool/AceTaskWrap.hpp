/**
 *    \file   AceTaskWrap.hpp
 *    \brief  A wrap of ACE_Task<ACE_MT_SYNCH>
 *    \date   12/15/2011
 *    \author kanif (xjtukanif@gmail.com)
 */

#ifndef __SPIDERUTILS_ACETASKWRAP_H
#define __SPIDERUTILS_ACETASKWRAP_H

#include <ace/Task.h>
#include <ace/Synch.h>

#include "Configuration.hpp"
#include "debug.h"

namespace basetool{ class ConfigurationSection; }
#include <iostream>

class AceTaskWrap:public ACE_Task<ACE_MT_SYNCH>
{
    public:
	AceTaskWrap():
	    thread_num_(1)
	{}

	~AceTaskWrap(){}

	virtual void notify_reconfig(const basetool::ConfigurationSection &config)
	{
	    config_ = config;
	    reconfig_flag_ = true;
	}
	
	virtual int open(const basetool::ConfigurationSection &config)
	{
	    stopping_ = reconfig_flag_ = false;
	    config_ = config;
	    thread_num_ = config_.Value<int>("ThreadNum", 1);
	    size_t input_queue_max = 0;
	    if (config_.GetValue("InputQueueMax",input_queue_max, 0)) {
		msg_queue()->high_water_mark(input_queue_max);
	    }
	    return doConfig();
	}

	virtual int doConfig(bool reconf=false)
	{
	    reconfig_flag_ = false;
	    return 0;
	}
	
	virtual int handleMessage(ACE_Message_Block *msg_blk) = 0;

	virtual int svc()
	{
	    LOG_INFO("%s start\n", taskname_.c_str());
	    ACE_Message_Block *msg_blk = NULL;
	    ACE_Time_Value timeout;
	    try{
		while(!stopping_){
		    //Avoid multi threads process reconfig
		    if(reconfig_flag_ && __sync_fetch_and_and(&reconfig_flag_, false)){
			bool reconf = true;
			doConfig(reconf);
		    }

		    timeout = ACE_OS::gettimeofday();
		    timeout += 1;
		    if(getq(msg_blk, &timeout) < 0)
			continue;
	    
		    if(msg_blk->msg_type() == ACE_Message_Block::MB_STOP){
			stopping_ = true;
			LOG_INFO("%s STOP on MB_STOP msg\n", taskname_.c_str());
			if(next()){
			    next()->putq(msg_blk);
			}else{
			    delete msg_blk;
			}
			break;
		    }

		    handleMessage(msg_blk);
		}
	    }catch (std::runtime_error &e){
		LOG_ERROR("%s\n", e.what());
	    }catch (...){
		LOG_ERROR("Unknow exception\n");
	    }
	    LOG_INFO("%s stops\n", taskname_.c_str());
	    return 0;
	}

	int ThreadNum()
	{
	    return thread_num_;
	}
	
	int putNext(ACE_Message_Block *msg_block)
	{
	    //FIXME: memory leak if put failed
	    return next()->putq(msg_block);
	}

	void setTaskName(const std::string& name)
	{
	    taskname_ = name;
	}
	
	const std::string &getTaskName() const
	{
	    return taskname_;
	}

	void setThreadNum(int thread_num)
	{
	    thread_num_ = thread_num;
	}

	int activate()
	{
	    return ACE_Task<ACE_MT_SYNCH>::activate(THR_NEW_LWP|THR_JOINABLE|THR_INHERIT_SCHED, thread_num_);
	}

    protected:
	bool reconfig_flag_; ///flag to active reconfig
	bool stopping_;
	int thread_num_;
	basetool::ConfigurationSection config_;  ///ConfigurationSection for reconfig   
    private:
	std::string taskname_;
};

#endif
