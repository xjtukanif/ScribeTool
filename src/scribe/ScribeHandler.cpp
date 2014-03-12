//  Copyright (c) 2007-2008 Facebook
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//
// See accompanying file LICENSE or visit the Scribe site at:
// http://developers.facebook.com/scribe/
//
// @author Bobby Johnson
// @author James Wang
// @author Jason Sobel
// @author Avinash Lakshman
// @author Anthony Giardullo
#include "ScribeHandler.hpp"
#include "debug.h"

using namespace apache::thrift;
using namespace apache::thrift::concurrency;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::server;

using namespace facebook::fb303;
using namespace facebook;

using namespace scribe::thrift;
using namespace std;

using boost::shared_ptr;

scribeHandler::scribeHandler(unsigned long int server_port)
  : FacebookBase("Scribe"),
    port(server_port),
    status(STARTING),
    statusDetails("initial state") {
}

scribeHandler::~scribeHandler() {
}

// Returns the handler status, but overwrites it with WARNING if it's
// ALIVE and at least one store has a nonempty status.
fb_status scribeHandler::getStatus() {
  return ALIVE;
}

void scribeHandler::setStatus(fb_status new_status) {
  LOG_DEBUG("STATUS: %s\n", statusAsString(new_status));
  Guard status_monitor(statusLock);
  status = new_status;
}

// Returns the handler status details if non-empty,
// otherwise the first non-empty store status found
void scribeHandler::getStatusDetails(std::string& _return) {
  Guard status_monitor(statusLock);

  _return = statusDetails;
  return;
}

void scribeHandler::setStatusDetails(const string& new_status_details) {
  LOG_DEBUG("STATUS: %s\n", new_status_details.c_str());
  Guard status_monitor(statusLock);
  statusDetails = new_status_details;
}

const char* scribeHandler::statusAsString(fb_status status) {
  switch (status) {
  case DEAD:
    return "DEAD";
  case STARTING:
    return "STARTING";
  case ALIVE:
    return "ALIVE";
  case STOPPING:
    return "STOPPING";
  case STOPPED:
    return "STOPPED";
  case WARNING:
    return "WARNING";
  default:
    return "unknown status code";
  }
}


ScribeResultCode scribeHandler::Log(const std::vector<scribe::thrift::LogEntry>&  messages) {
    if(status == STOPPING || !on_log_cb_(messages)) 
	return ScribeResultCodeNamespace::TRY_LATER;

    return ScribeResultCodeNamespace::OK;
}

void scribeHandler::shutdown() {
  // calling stop to allow thrift to clean up client states and exit
  LOG_INFO("ScribeHandler stop\n");
  exit(0);
}
/*
void scribeHandler::reinitialize() {

  // reinitialize() will re-read the config file and re-configure the stores.
  // This is done without shutting down the Thrift server, so this will not
  // reconfigure any server settings such as port number.
  LOG_DEBUG("reinitializing");
  initialize();
}
*/

void scribeHandler::initialize(boost::function<bool (const std::vector<scribe::thrift::LogEntry>&)> on_log_cb) 
{
    //register call back function
    on_log_cb_ = on_log_cb;

    // This clears out the error state, grep for setStatus below for details
    setStatus(STARTING);
    setStatusDetails("");
    setStatus(ALIVE);
}


