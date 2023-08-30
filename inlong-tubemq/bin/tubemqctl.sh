#!/bin/bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# project directory
if [ -z "$BASE_DIR" ] ; then
  PRG="$0"

  # need this for relative symlinks
  while [ -h "$PRG" ] ; do
    ls=`ls -ld "$PRG"`
    link=`expr "$ls" : '.*-> \(.*\)$'`
    if expr "$link" : '/.*' > /dev/null; then
      PRG="$link"
    else
      PRG="`dirname "$PRG"`/$link"
    fi
  done
  BASE_DIR=`dirname "$PRG"`/..

  # make it fully qualified
  BASE_DIR=`cd "$BASE_DIR" && pwd`
fi
# load environmental variables
source $BASE_DIR/bin/env.sh


# display usage
function help() {
  echo "Usage: tubemqctl {producer/consumer/topic}" >&2
  echo "       producer:      --master-servers [YOUR_MASTER_IP:port] --topicName [topic name]"
  echo "       consumer:      --master-servers [YOUR_MASTER_IP:port] --topicName [topic name]"
  echo "       topic:         create --master-servers [YOUR_MASTER_IP:port] --topicName [topic name] --brokerId [broker id] --confModAuthToken [auth token] --createUser [user name]
                                     --deleteWhen --deletePolicy --numPartitions --unflushThreshold --numTopicStores --unflushInterval --memCacheMsgCntInK --memCacheMsgSizeInMB --memCacheFlushIntvl --brokerTLSPort --acceptPublish --acceptSubscribe
                              list   --master-servers [YOUR_MASTER_IP:port]
                              modify --master-servers [YOUR_MASTER_IP:port] --topicName [topic name] --brokerId [broker id] --confModAuthToken [auth token] --modifyUser [user name]
                                     --deleteWhen --deletePolicy --numPartitions --unflushThreshold --numTopicStores --unflushInterval --memCacheMsgCntInK --memCacheMsgSizeInMB --memCacheFlushIntvl --brokerTLSPort --acceptPublish --acceptSubscribe
                              delete --master-servers [YOUR_MASTER_IP:port] --topicName [topic name] --brokerId [broker id] --confModAuthToken [auth token] --modifyUser [user name]"
  echo "       -h,--help"
}

# if less than two arguments supplied
if [ $# -lt 2 ]; then
  help;
  exit 1;
fi

SERVICE=$1

opt=""
case $SERVICE in
  topic)
    SERVICE_CLASS="org.apache.inlong.tubemq.server.tools.cli.CliTopic"
    shift 1
    echo $*
    case $1 in
      list)
        opt="admin_query_topic_info "
        ;;
      create)
        opt="admin_add_new_topic_record "
        ;;
      delete)
        opt="admin_delete_topic_info "
        ;;
      modify)
        opt="admin_modify_topic_info "
        ;;
      *)
        help;
        exit 1;
        ;;
    esac
    ;;
  producer)
    SERVICE_CLASS="org.apache.inlong.tubemq.server.tools.cli.CliProducer"
    ;;
  consumer)
    SERVICE_CLASS="org.apache.inlong.tubemq.server.tools.cli.CliConsumer"
    ;;
  *)
    help;
    exit 1;
    ;;
esac

shift 1
opt="$opt $*"

$JAVA $TOOLS_ARGS $SERVICE_CLASS $opt
