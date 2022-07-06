package org.example;


import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.interceptor.pingreq.PingReqInboundInterceptor;
import com.hivemq.extension.sdk.api.interceptor.pingreq.parameter.PingReqInboundInput;
import com.hivemq.extension.sdk.api.interceptor.pingreq.parameter.PingReqInboundOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class PingReqInterceptor implements PingReqInboundInterceptor {
    private static final Logger log = LoggerFactory.getLogger(TopicTreeMain.class);
    TopicTreeMain main = new TopicTreeMain();
    public static Charset charset = StandardCharsets.UTF_8;
    //boolean table_complete = false;




    public PingReqInterceptor(TopicTreeMain main) {
        this.main = main;
    }

    @Override
    public void onInboundPingReq(@NotNull PingReqInboundInput pingReqInboundInput, @NotNull PingReqInboundOutput pingReqInboundOutput) {
        //log.info("pingreq int...  num of topic: " + main.interestedTopic.size());
        if(main.interestedTopic.isEmpty())
            main.sendPublishToBroker("$PING/", computeMyRttList());
        else {
            if(!main.stopPing)
                for (String topic : main.interestedTopic) {
                    int numofBrokers = main.topicOverlays.get(topic).getTopicTable().size();
                    String ping_payload = numofBrokers + ";" + computeMyRttList();
                    //log.info("final payload: " + pingPayload);
                    main.sendPublishToBroker("$PINGTOPIC/" + topic, ping_payload);
                }
        }



    }

    public String computeMyRttList(){
        String rttList = null;
        for(String key : main.myRttTable.keySet()) {
            //log.info("rttList...: " + rttList);
            String newRtt = key + "-" + main.myRttTable.get(key) + "_";
            if(rttList == null)
                rttList = newRtt;
            else
                rttList = rttList.concat(newRtt);
        }
        //log.info("final rtt list: " + rttList);
        return rttList;
    }


}
