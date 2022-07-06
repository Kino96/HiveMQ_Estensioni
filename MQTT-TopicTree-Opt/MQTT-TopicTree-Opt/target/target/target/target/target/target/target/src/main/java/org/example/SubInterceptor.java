package org.example;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.interceptor.subscribe.SubscribeInboundInterceptor;
import com.hivemq.extension.sdk.api.interceptor.subscribe.parameter.SubscribeInboundInput;
import com.hivemq.extension.sdk.api.interceptor.subscribe.parameter.SubscribeInboundOutput;
import com.hivemq.extension.sdk.api.packets.subscribe.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.hivemq.extension.sdk.api.services.Services;
import com.hivemq.extension.sdk.api.services.builder.Builders;
import com.hivemq.extension.sdk.api.services.builder.PublishBuilder;
import com.hivemq.extension.sdk.api.services.publish.Publish;

import java.net.InetAddress;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.nio.ByteBuffer;

import static java.net.InetAddress.getLocalHost;

public class SubInterceptor implements SubscribeInboundInterceptor {
    private static final Logger log = LoggerFactory.getLogger(TopicTreeMain.class);
    TopicTreeMain main = new TopicTreeMain();
    public static Charset charset = StandardCharsets.UTF_8;

    public SubInterceptor(TopicTreeMain main) {
        this.main = main;
    }

    @Override
    public void onInboundSubscribe(@NotNull SubscribeInboundInput subscribeInboundInput, @NotNull SubscribeInboundOutput subscribeInboundOutput) {
        log.info("subcription received...");
        main.stopPing = false;
        String clientId = subscribeInboundInput.getClientInformation().getClientId();
        @NotNull List<Subscription> subscriptions = subscribeInboundInput.getSubscribePacket().getSubscriptions();
        //log.info("subcriptions list size: " + subscriptions.size());

        for(int i = 0; i < subscriptions.size(); i++){
            String topic = subscriptions.get(i).getTopicFilter();
            log.info("sub to topic: " + topic);
            boolean isNewTopic = main.isNewTopicSub(topic);
            //log.info("è un topic nuovo? " + isNewTopic);
            //se ricevo una sottoiscrizione a un topic per la prima volta, avvisa altri broker
            if(isNewTopic) {
                //log.info("sub a nuovo topic, creo topic table e invio subAdv...");
                log.info("sub a un nuovo topic; invio adv agli altri broker");
                main.interestedTopic.add(topic);

                //creo topic table
                CellTable newSelfTopicTable = new CellTable();
                newSelfTopicTable.setReceived(true);
                newSelfTopicTable.setIp_address(main.own_address);
                main.topicOverlays.put(topic, new TopicTable(topic));
                main.topicOverlays.get(topic).addBroker(newSelfTopicTable);

                //invio subAdv
                main.sendPublishToBroker("$SUBADV/" + topic, "");
            }
            //update list of subscriptions
            SubTable subTable = new SubTable();
            subTable.setClientId(clientId);
            subTable.setTopic(topic);
            main.subscriptionTable.add(subTable);

        }
        /*log.info("subcriptionTable: ");
        for(int i = 0; i<main.subscriptionTable.size(); i++){
            log.info("clientId: " + main.subscriptionTable.get(i).getClientId());
            log.info("topic: " + main.subscriptionTable.get(i).getTopic());
        }*/



    }
}
