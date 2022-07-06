package org.example;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.interceptor.publish.PublishOutboundInterceptor;
import com.hivemq.extension.sdk.api.interceptor.publish.parameter.PublishOutboundInput;
import com.hivemq.extension.sdk.api.interceptor.publish.parameter.PublishOutboundOutput;
import com.hivemq.extension.sdk.api.packets.publish.ModifiablePublishPacket;
import com.hivemq.extension.sdk.api.packets.publish.PublishPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;


public class PublishOutInterceptor implements PublishOutboundInterceptor {
    private static final Logger log = LoggerFactory.getLogger(TopicTreeMain.class);
    TopicTreeMain main = new TopicTreeMain();
    public static Charset charset = StandardCharsets.UTF_8;

    public PublishOutInterceptor(TopicTreeMain main) {
        this.main = main;
    }
    @Override
    public void onOutboundPublish(@NotNull PublishOutboundInput publishOutboundInput, @NotNull PublishOutboundOutput publishOutboundOutput) {

        log.info("outbound publish working");
        //publishOutboundOutput.preventPublishDelivery();
        /*final PublishPacket publishPacket = publishOutboundInput.getPublishPacket();
        String topic_received = publishPacket.getTopic();
        String[] parts = null;
        if(topic_received.contains("SUBADV/")){
            log.info(("inviata una adv"));
            parts = topic_received.split("/");
            String topic = parts[1];
            main.topicOverlays.get(topic).incMsgExpected();
            log.info("msg expected: " +  main.topicOverlays.get(topic).getMsgExpected());

        }*/






    }
}
