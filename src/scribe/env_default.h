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

#ifndef SCRIBE_ENV
#define SCRIBE_ENV

#include "thrift/protocol/TBinaryProtocol.h"
#include "thrift/server/TNonblockingServer.h"
#include "thrift/concurrency/ThreadManager.h"
#include "thrift/concurrency/PosixThreadFactory.h"
#include "thrift/concurrency/Mutex.h"
#include "thrift/transport/TSocket.h"
#include "thrift/transport/TSocketPool.h"
#include "thrift/transport/TServerSocket.h"
#include "thrift/transport/TTransportUtils.h"
#include "thrift/transport/THttpClient.h"
#include "thrift/transport/TFileTransport.h"
#include "thrift/transport/TBufferTransports.h"
#include "thrift/transport/TSimpleFileTransport.h"

#include "fb303/FacebookBase.h"
#include "gen-cpp/scribe.h"
#include "gen-cpp/scribe_types.h"

#if SS_THRIFT_VERSION >= 700
#define ScribeResultCode scribe::thrift::ResultCode::type
#define ScribeResultCodeNamespace scribe::thrift::ResultCode
#else
#define ScribeResultCode scribe::thrift::ResultCode
#define ScribeResultCodeNamespace scribe::thrift
#endif


/*
 * Debug logging
 */
#define LOG_OPER(format_string,...)                                     \
  {                                                                     \
    time_t now;                                                         \
    char dbgtime[26] ;                                                  \
    time(&now);                                                         \
    ctime_r(&now, dbgtime);                                             \
    dbgtime[24] = '\0';                                                 \
    fprintf(stderr,"[%s] " #format_string " \n", dbgtime,##__VA_ARGS__); \
  }

#endif // SCRIBE_ENV
