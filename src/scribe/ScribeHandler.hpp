//  Copyright (c) 2007-2009 Facebook
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

#ifndef SCRIBE_SERVER_H
#define SCRIBE_SERVER_H

#include "common.h"
#include <boost/function.hpp>
#include <vector>

class scribeHandler : virtual public scribe::thrift::scribeIf,
                              public facebook::fb303::FacebookBase {

 public:
  scribeHandler(unsigned long int port);
  ~scribeHandler();

  void shutdown();
  void initialize(boost::function<bool (const std::vector<scribe::thrift::LogEntry>&)> on_log_cb);
  //void reinitialize();

  ScribeResultCode Log(const std::vector<scribe::thrift::LogEntry>& messages);

  void getVersion(std::string& _return) {_return = "2.2";}
  facebook::fb303::fb_status getStatus();
  void getStatusDetails(std::string& _return);
  void setStatus(facebook::fb303::fb_status new_status);
  void setStatusDetails(const std::string& new_status_details);

  unsigned long int port; // it's long because that's all I implemented in the conf class

 private:
  facebook::fb303::fb_status status;
  std::string statusDetails;
  apache::thrift::concurrency::Mutex statusLock;

  /* mutex to syncronize access to scribeHandler.
   * A single mutex is fine since it only needs to be locked in write mode
   * during start/stop/reinitialize or when we need to create a new category.
   */

  // disallow empty construction, copy, and assignment
  scribeHandler();
  scribeHandler(const scribeHandler& rhs);
  const scribeHandler& operator=(const scribeHandler& rhs);
  boost::function<bool (const std::vector<scribe::thrift::LogEntry>&)> on_log_cb_;

 protected:
  const char* statusAsString(facebook::fb303::fb_status status);
};
extern boost::shared_ptr<scribeHandler> g_Handler;
#endif // SCRIBE_SERVER_H
