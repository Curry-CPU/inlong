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

import org.apache.inlong.tubemq.corebase.utils.TStringUtils;
import org.apache.inlong.tubemq.server.common.fielddef.CliArgDef;
import org.apache.inlong.tubemq.server.common.utils.HttpUtils;

import com.google.gson.JsonObject;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class CliTopic extends CliAbstractBase {

    private static final Logger logger = LoggerFactory.getLogger(CliTopic.class);
    private String masterServers = null;
    private static final Set<String> op_modify = new HashSet<String>() {

        {
            add("admin_add_new_topic_record");
            add("admin_delete_topic_info");
            add("admin_modify_topic_info");
        }
    };
    private static final Set<String> op_query = new HashSet<String>() {

        {
            add("admin_query_topic_info");
        }
    };

    public CliTopic() {
        super("tubemqctl.sh");
        initCommandOptions();
    }

    /**
     * Init command options
     */
    @Override
    protected void initCommandOptions() {
        // add the cli required parameters
        addCommandOption(CliArgDef.MASTERSERVER);
        addCommandOption(CliArgDef.TOPICNAME);
        addCommandOption(CliArgDef.BROKERID);
        addCommandOption(CliArgDef.DELETEWHEN);
        addCommandOption(CliArgDef.DELETEPOLICY);
        addCommandOption(CliArgDef.NUMPARTITIONS);
        addCommandOption(CliArgDef.UNFLUSHTHRESHOLD);
        addCommandOption(CliArgDef.NUMTOPICSTORES);
        addCommandOption(CliArgDef.UNFLUSHINTERVAL);
        addCommandOption(CliArgDef.MEMCACHEMSGCNTINK);
        addCommandOption(CliArgDef.MEMCACHEMSGSIZEINMB);
        addCommandOption(CliArgDef.MEMCACHEFLUSHINTVL);
        addCommandOption(CliArgDef.BROKERTLSPORT);
        addCommandOption(CliArgDef.ACCEPTPUBLISH);
        addCommandOption(CliArgDef.ACCEPTSUBSCRIBE);
        addCommandOption(CliArgDef.CONFMODAUTHTOKEN);
        addCommandOption(CliArgDef.CREATEUSER);
        addCommandOption(CliArgDef.MODIFYUSER);
    }

    @Override
    public boolean processParams(String[] args) throws Exception {
        CommandLine cli = parser.parse(options, args);
        if (cli == null) {
            throw new ParseException("Parse args failure");
        }
        if (cli.hasOption(CliArgDef.VERSION.longOpt)) {
            version();
        }
        if (cli.hasOption(CliArgDef.HELP.longOpt)) {
            help();
        }
        if (!cli.hasOption(CliArgDef.MASTERSERVER.longOpt)) {
            throw new Exception(CliArgDef.MASTERSERVER.longOpt + " is required!");
        }
        masterServers = cli.getOptionValue(CliArgDef.MASTERSERVER.longOpt);
        if (TStringUtils.isBlank(masterServers)) {
            throw new Exception(CliArgDef.MASTERSERVER.longOpt + " is not allowed blank!");
        }
        Map<String, String> inParamMap = new HashMap<>();
        JsonObject result = null;
        String masterUrl = "http://" + masterServers + "/webapi.htm";
        if (op_modify.contains(args[0])) {
            inParamMap.put("type", "op_modify");
            if (!cli.hasOption(CliArgDef.TOPICNAME.longOpt)) {
                throw new Exception(CliArgDef.TOPICNAME.longOpt + " is required!");
            }
            String topicName = cli.getOptionValue(CliArgDef.TOPICNAME.longOpt);
            if (TStringUtils.isBlank(topicName)) {
                throw new Exception(CliArgDef.TOPICNAME.longOpt + " is not allowed blank!");
            }
            inParamMap.put(CliArgDef.TOPICNAME.longOpt, topicName);
            if (!cli.hasOption(CliArgDef.BROKERID.longOpt)) {
                throw new Exception(CliArgDef.BROKERID.longOpt + " is required!");
            }
            String brokerId = cli.getOptionValue(CliArgDef.BROKERID.longOpt);
            if (TStringUtils.isBlank(brokerId)) {
                throw new Exception(CliArgDef.BROKERID.longOpt + " is not allowed blank!");
            }
            inParamMap.put(CliArgDef.BROKERID.longOpt, brokerId);
            if (!args[0].equals("admin_delete_topic_info")) {
                checkParam(cli, CliArgDef.DELETEWHEN.longOpt, inParamMap);
                checkParam(cli, CliArgDef.DELETEPOLICY.longOpt, inParamMap);
                checkParam(cli, CliArgDef.NUMPARTITIONS.longOpt, inParamMap);
                checkParam(cli, CliArgDef.UNFLUSHTHRESHOLD.longOpt, inParamMap);
                checkParam(cli, CliArgDef.NUMTOPICSTORES.longOpt, inParamMap);
                checkParam(cli, CliArgDef.UNFLUSHINTERVAL.longOpt, inParamMap);
                checkParam(cli, CliArgDef.MEMCACHEMSGCNTINK.longOpt, inParamMap);
                checkParam(cli, CliArgDef.MEMCACHEMSGSIZEINMB.longOpt, inParamMap);
                checkParam(cli, CliArgDef.MEMCACHEFLUSHINTVL.longOpt, inParamMap);
                checkParam(cli, CliArgDef.BROKERTLSPORT.longOpt, inParamMap);
                checkParam(cli, CliArgDef.ACCEPTPUBLISH.longOpt, inParamMap);
                checkParam(cli, CliArgDef.ACCEPTSUBSCRIBE.longOpt, inParamMap);
            }
            if (args[0].equals("admin_add_new_topic_record")) {
                checkParam(cli, CliArgDef.CREATEUSER.longOpt, inParamMap);
            } else {
                checkParam(cli, CliArgDef.MODIFYUSER.longOpt, inParamMap);
            }

            checkParam(cli, CliArgDef.CONFMODAUTHTOKEN.longOpt, inParamMap);
        } else {
            inParamMap.put("type", "op_query");
        }
        inParamMap.put("method", args[0]);
        result = HttpUtils.requestWebService(masterUrl, inParamMap);
        if (args[0].equals("admin_modify_topic_info")) {
            Map<String, String> paramMap = new HashMap<>();
            paramMap.put("type", "op_modify");
            paramMap.put("method", "admin_reload_broker_configure");
            paramMap.put(CliArgDef.BROKERID.longOpt, inParamMap.get(CliArgDef.BROKERID.longOpt));
            paramMap.put(CliArgDef.MODIFYUSER.longOpt, inParamMap.get(CliArgDef.MODIFYUSER.longOpt));
            paramMap.put(CliArgDef.CONFMODAUTHTOKEN.longOpt, inParamMap.get(CliArgDef.CONFMODAUTHTOKEN.longOpt));
            HttpUtils.requestWebService(masterUrl, paramMap);
        } else if (args[0].equals("admin_delete_topic_info")) {
            inParamMap.put("method", "admin_remove_topic_info");
            HttpUtils.requestWebService(masterUrl, inParamMap);
        }
        System.out.println(result.toString());
        // System.exit(0);
        return true;
    }

    public void checkParam(CommandLine cli, String opt, Map<String, String> inParamMap) throws Exception {
        if (cli.hasOption(opt)) {
            String param = cli.getOptionValue(opt);
            if (TStringUtils.isBlank(param)) {
                throw new Exception(opt + " is not allowed blank!");
            }
            inParamMap.put(opt, param);
        }
    }

    public static void main(String[] args) {
        CliTopic cliTopic = new CliTopic();
        try {
            cliTopic.processParams(args);
        } catch (Throwable ex) {
            ex.printStackTrace();
            logger.error(ex.getMessage());
            cliTopic.help();
        }
    }
}
