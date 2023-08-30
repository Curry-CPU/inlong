/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.tubemq.server.tools.cli;

import org.junit.Test;

public class CliTopicTest {

    @Test
    public void testTopic() {
        CliTopic.main(new String[]{"admin_add_new_topic_record", "--master-servers", "127.0.0.1:8080", "--topicName",
                "test", "--brokerId", "1", "--createUser", "webapi", "--confModAuthToken", "abc"});
        CliTopic.main(new String[]{"admin_query_topic_info", "--master-servers", "127.0.0.1:8080"});
        CliTopic.main(new String[]{"admin_modify_topic_info", "--master-servers", "127.0.0.1:8080", "--topicName",
                "test", "--brokerId", "1", "--acceptPublish", "false", "--acceptSubscribe", "false", "--modifyUser",
                "webapi", "--confModAuthToken", "abc"});
        CliTopic.main(new String[]{"admin_delete_topic_info", "--master-servers", "127.0.0.1:8080", "--topicName",
                "test", "--brokerId", "1", "--modifyUser", "webapi", "--confModAuthToken", "abc"});
    }
}
