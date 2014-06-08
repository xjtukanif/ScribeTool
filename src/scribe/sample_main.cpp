/**
 *    \file   ScribeServer.hpp
 *    \brief  a wrap of scribe server
 *    \date   12/14/2011
 *    \author kanif (xjtukanif@gmail.com) 
 */


#include <boost/shared_ptr.hpp>

#include "CommServerSignalHandler.hpp"
#include "Configuration.hpp"
#include "debug.h"

#include "ScribeServer.hpp"

class ServerWrapper
{
    public:
	ServerWrapper(){}

	void stop()
	{
	    server_->stop();
	}

	int open(const char *config_file)
	{
	    try{
		cfg_file_.assign(config_file);
		basetool::ConfigurationFile config(config_file);

		basetool::ConfigurationSection sample_cfg = config.Section("Sample");
		bool debug = sample_cfg.Value<bool>("Debug", false);
		if(!debug){
		    LOG_INFO("Close debug log\n");
		    set_min_log_level(LOG_LEVEL_INFO);
		}else{
		    LOG_INFO("Open debug log\n");
		    set_min_log_level(LOG_LEVEL_DEBUG);
		}
		
		//configure of scribe server
		basetool::ConfigurationSection scribe_cfg = config.Section("Sample/ScribeReceiver");
		unsigned short server_port = scribe_cfg.Value<unsigned short>("ListenPort");
		int server_thread_num = scribe_cfg.Value<int>("ThriftServerThreads", 100);
		int max_connect_num = scribe_cfg.Value<int>("MaxConnectionNumber", 10);
		size_t max_message_length = scribe_cfg.Value<size_t>("MaxMessageLegth", MAX_MESSAGE_LENGTH);
		size_t scribe_full_retry_times = scribe_cfg.Value<size_t>("ScribeFullRetryTimes", 2);
		size_t scribe_full_sleep_time = scribe_cfg.Value<size_t>("ScribeFullSleepTime", 100);
		bool scribe_if_full_drop_msg = scribe_cfg.Value<bool>("IfQueueFullDropMsg", false);
		server_.reset(new ScribeServer(server_thread_num, server_port, max_connect_num, max_message_length, scribe_full_retry_times, scribe_full_sleep_time, scribe_if_full_drop_msg ));
		server_->setServerName("SampleScribeServer");
		
		if(server_->open() != 0){
		    LOG_ERROR("Open ScribeServer failed\n");
		    return -1;
		}

		if(parser_thread_.open(config.Section("Sample/Parser")) != 0){
		    LOG_ERROR("Open ParserThread failed\n");
		    return -1;
		}

	    }catch (basetool::ConfigurationError &e){
		LOG_ERROR("%s\n", e.what());
		return 1;
	    }catch (std::runtime_error &e){
		LOG_ERROR("%s\n", e.what());
		return 1;
	    }

	    server_->next(&parser_thread_);

	    return 0;
	}

	void notify_reconfig()
	{
	    try{
		LOG_INFO("reconfig signal set\n");
		basetool::ConfigurationFile config(cfg_file_.c_str());
		parser_thread_.notify_reconfig(config.Section("Sample/Parser"));
	    }catch (basetool::ConfigurationError &e){
		LOG_ERROR("%s\n", e.what());
		return;
	    }catch (std::runtime_error &e){
		LOG_ERROR("%s\n", e.what());
		return;
	    }
	}

	void activate()
	{
	    parser_thread_.activate();
	    //very important,  this never will not return until server exit
	    server_->activate();
	}

    private:
	std::string cfg_file_;
	boost::shared_ptr<ScribeServer> server_;
	//Real Thread to process scribe message. It's just a sample.the class We do not give
	InputParser parser_thread_;
};

int main(int argc, char *argv[])
{
    if(argc < 2){
	std::cout << "usage " << argv[0] << " confile" << std::endl;
	return 1;
    }
    
    ServerWrapper server;
    CommServerSignalHandler<ServerWrapper> comm_sighandler(&server);

    if(server.open(argv[1]) != 0)
	return 1;

    comm_sighandler.activate();
    server.activate();

    ACE_Thread_Manager::instance()->wait();
    return 0;
}

