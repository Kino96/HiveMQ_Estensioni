
/*
 * Copyright 2018-present HiveMQ GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.example;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.interceptor.publish.PublishInboundInterceptor;
import com.hivemq.extension.sdk.api.interceptor.publish.parameter.PublishInboundInput;
import com.hivemq.extension.sdk.api.interceptor.publish.parameter.PublishInboundOutput;
import com.hivemq.extension.sdk.api.packets.publish.ModifiablePublishPacket;
import com.hivemq.extension.sdk.api.services.Services;
import com.hivemq.extension.sdk.api.services.builder.Builders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * This is a very simple {@link PublishInboundInterceptor},
 * it changes the payload of every incoming PUBLISH with the topic 'hello/world' to 'Hello World!'.
 *
 * @author Yannick Weber
 * @since 4.3.1
 */
public class PublishInterceptor implements PublishInboundInterceptor {
    TopicTreeMain main = new TopicTreeMain();
    private static final Logger log = LoggerFactory.getLogger(TopicTreeMain.class);
    public static Charset charset = StandardCharsets.UTF_8;

    public PublishInterceptor(TopicTreeMain main) {
        this.main = main;
    }

    @Override
    public void onInboundPublish(final @NotNull PublishInboundInput publishInboundInput, final @NotNull PublishInboundOutput publishInboundOutput) {
        final ModifiablePublishPacket publishPacket = publishInboundOutput.getPublishPacket();
        String broker_incoming_address = String.valueOf(publishInboundInput.getConnectionInformation().getInetAddress().orElse(null));
        broker_incoming_address = broker_incoming_address.substring(1);
        String  topic_received = null;
        String topic_received_part1 = null;
        String topic_received_part2 = null;
        String topic_received_part3 = null;
        String topic_received_part4 = null;
        topic_received = publishPacket.getTopic();

        ByteBuffer payload_bb;
        ByteBuffer payload;
        publishInboundOutput.preventPublishDelivery();
        String[] parts = null;
        if(topic_received.contains("/")) {
            parts = topic_received.split("/");
            topic_received_part1 = parts[0];

        }else{
            topic_received_part1 = topic_received;
        }
        switch(topic_received_part1){
            case "$PING" :
                payload_bb = publishInboundInput.getPublishPacket().getPayload().orElse(null);
                String rttPayload = bb_to_str(payload_bb, charset);
                //updateRttTable(rttPayload);
                break;
            case "$PINGTOPIC":
                topic_received_part2 = parts[1];
                payload_bb = publishInboundInput.getPublishPacket().getPayload().orElse(null);
                String pingPayload = bb_to_str(payload_bb, charset);
                //log.info("ping payload received: " + pingPayload);
                String[] payload_parts = pingPayload.split(";");
                String numofb_str = payload_parts[0];
                //String numofb_str = bb_to_str(payload_bb, charset);
                int numOfbrokers = Integer.parseInt(numofb_str);
                /*if(main.interestedTopic.contains(topic_received_part2)){
                    log.info("topic table size: " + main.topicOverlays.get(topic_received_part2).getTopicTable().size());
                    log.info("num of brokers: " + numOfbrokers);
                    log.info("get numofbrokers: " + main.topicOverlays.get(topic_received_part2).getNumBrokers());
                }*/
                //updateRttTable(payload_parts[1]);
                if(main.interestedTopic.contains(topic_received_part2) && numOfbrokers == main.topicOverlays.get(topic_received_part2).getTopicTable().size() ) {
                    if (main.topicOverlays.get(topic_received_part2).getTopicTable().size() != main.topicOverlays.get(topic_received_part2).getNumBrokers()) {
                            resetTopicTree(topic_received_part2);
                            log.info("ho ricevuto nuovi broker per topic " + topic_received_part2 + ", posso costruire nuovo tree con n broker: " + main.topicOverlays.get(topic_received_part2).getTopicTable().size());
                            log.info("choose root...");
                            main.topicOverlays.get(topic_received_part2).setNumBrokers(main.topicOverlays.get(topic_received_part2).getTopicTable().size());
                            //chooseRootOptTot(topic_received_part2);
                            chooseRootOptMax(topic_received_part2);
                            log.info("Starting tree building...");
                            createSpanningTree(topic_received_part2);

                    }
                }
                break;
            case "$SUBADV":
                main.stopPing = false;
                //log.info("SUBADV receveid from " + broker_incoming_address);
                topic_received_part2 = parts[1];
                if(!main.isNewTopicSub(topic_received_part2)){
                    log.info("anche io ho la stessa sub, mi salvo il broker se non l'ho gia salvato");
                    if(!checkBrokerInTopicTable(topic_received_part2, broker_incoming_address)){
                        log.info("broker " + broker_incoming_address + " nuovo, lo salvo");
                        CellTable cb = new CellTable();
                        cb.setIp_address(broker_incoming_address);
                        main.topicOverlays.get(topic_received_part2).addBroker(cb);
                        main.topicOverlays.get(topic_received_part2).setCompleted(false);
                        //main.msg_count = 0;

                    }
                    //log.info("Rispondo al broker del mio stesso interesse");
                    //invio subresp
                    //log.info("$SUBRESP/" + broker_incoming_address + "/" + topic_received_part2);

                    main.sendPublishToBroker("$SUBRESP/" + broker_incoming_address + "/" + topic_received_part2, "");

                }
            break;

            case "$SUBRESP":

                topic_received_part3 = parts[2]; //topic
                //log.info("ricevuta SUBRESP per topic " + topic_received_part3 + "da " + broker_incoming_address);
                if(!checkBrokerInTopicTable(topic_received_part3, broker_incoming_address)) {
                    log.info("broker " + broker_incoming_address + " nuovo, lo salvo");
                    CellTable cb = new CellTable();
                    cb.setIp_address(broker_incoming_address);
                    main.topicOverlays.get(topic_received_part3).addBroker(cb);
                    //main.msg_count = 0;

                }

            break;
            case "$PUBADV":
                topic_received_part2 = parts[1];
                //log.info("PUBADV da " + broker_incoming_address + " per topic " + topic_received_part2);
                if(main.interestedTopic.contains(topic_received_part2)) {
                    payload_bb = publishInboundInput.getPublishPacket().getPayload().orElse(null);

                    if (payload_bb != null) {

                        main.sendPublishToBroker("$PUBRESP/" + broker_incoming_address + "/" + topic_received_part2, payload_bb);

                    }


                }
                break;
            case "$PUBRESP":
                String topic = parts[2];
                payload_bb = publishInboundInput.getPublishPacket().getPayload().orElse(null);
                //log.info("PUBRESP da " + broker_incoming_address + " per topic " + topic);
                long pubId = 0;
                if (payload_bb != null) {
                    byte[] arr = new byte[payload_bb.remaining()];
                    payload_bb.get(arr);
                    pubId = bytesToLong(arr);
                    //log.info("pubId:" + pubId);
                }
                String cId = main.pubBuffer.get(pubId).getClientId();
                if(!main.pubBuffer.get(pubId).getSent()) {
                    //log.info("setto next broker per client " + cId);
                    main.clientBuf.put(cId, broker_incoming_address);
                    main.pubBuffer.get(pubId).setsent(true);
                    //log.info("new next broker: " + main.clientBuf.get(cId));
                    String new_topic = "$TOPIC/" + topic + "/" + broker_incoming_address
                            + "/FORWARD";
                    //log.info("invio al broker più veloce");
                    main.sendPublishToBroker(new_topic, main.pubBuffer.get(pubId).getPayload());

                    //log.info("pubresp inviata a " + broker_incoming_address);
                }



                break;
            //qui avviene tutto all'interno dello stesso topictree
            case "$TOPIC":
                topic_received_part2 = parts[1]; //topic
                //topic_received_part3 = parts[2]; //own_addr
                topic_received_part3 = parts[3]; //MSG
                //log.info("ricevuta $TOPIC per topic " + topic_received_part3);
                switch(topic_received_part3) {
                    case "FORWARD":
                        main.stopPing = true;
                        //log.info("pub forward da " + broker_incoming_address);
                        String id = publishInboundInput.getClientInformation().getClientId();
                        ByteBuffer payload_client = publishInboundInput.getPublishPacket().getPayload().get();
                        //per prima cosa invio pub ai miei client
                        for (int i = 0; i < main.subscriptionTable.size(); i++) {
                            if (main.subscriptionTable.get(i).getTopic().equals(topic_received_part2)) {
                                String clientId = main.subscriptionTable.get(i).getClientId();
                                main.sendPublishToClient(topic_received_part2, payload_client, clientId);
                            }
                        }

                        //ora invio pub ad altri broker nell'albero
                        forwardPub(topic_received_part2, payload_client, broker_incoming_address, 0, id);
                        break;

                    case "REMOVEBROKER":
                        if (main.interestedTopic.contains(topic_received_part2)){
                            log.info("remove broker " + broker_incoming_address  + " from tree of topic " + topic_received_part2);
                            main.topicOverlays.get(topic_received_part2).removeBroker(broker_incoming_address);
                            resetTopicTree(topic_received_part2);
                            log.info("Update tree con n broker: " + main.topicOverlays.get(topic_received_part2).getTopicTable().size());
                            main.topicOverlays.get(topic_received_part2).setNumBrokers(main.topicOverlays.get(topic_received_part2).getTopicTable().size());
                            if(main.topicOverlays.get(topic_received_part2).getNumBrokers()>0) {
                                log.info("choose root...");
                                //chooseRootOptTot(topic_received_part2);
                                chooseRootOptMax(topic_received_part2);
                                log.info("Starting tree building...");
                                createSpanningTree(topic_received_part2);
                            }
                        }
                        break;

                }


                break;
            //questo serve solo a controllare le tabelle dei broker quando voglio
            case "topictable":

                for(int i=0; i<main.interestedTopic.size(); i++){
                    String s = main.interestedTopic.get(i);
                    log.info("Topic Overlay of topic " + s);
                    log.info("Num of brokers: "+ main.topicOverlays.get(s).getTopicTable().size());
                    log.info("Root: " + main.topicOverlays.get(s).getRoot_address());
                    for(int k=0; k<main.topicOverlays.get(s).getTopicTable().size(); k++){
                    //for (CellTable c : main.topicOverlays.get(s.getTopic()).getTopicTable()) {
                        //log.info("ip addr: " + c.getIp_address() + "  rtt: " + c.getRTT());
                        if(main.topicOverlays.get(s).getTopicTable().get(k).isNext_hop()){
                            log.info("next_hop: {}",main.topicOverlays.get(s).getTopicTable().get(k).getIp_address());
                        }
                        if(main.topicOverlays.get(s).getTopicTable().get(k).isPrev_hop()){
                            if(!main.topicOverlays.get(s).getTopicTable().get(k).getIp_address().equals(main.own_address))
                            log.info("prev_hop: {}",main.topicOverlays.get(s).getTopicTable().get(k).getIp_address());
                        }
                    }

                }
            break;
            case "rtt" :
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(System.in));

                // Reading data using readLine
                String name = null;
                try {
                    name = reader.readLine();
                } catch (IOException e) {
                    e.printStackTrace();
                }

            break;
            //messaggi ricevuti dai client
            default:
                main.stopPing = true;
                log.info("pub da un client");
                String id = publishInboundInput.getClientInformation().getClientId();
                if(!main.clientBuf.containsKey(id))
                    main.clientBuf.put(id, null);
                String topic_client = publishInboundInput.getPublishPacket().getTopic();
                //int pub_id = publishInboundInput.getPublishPacket().getPacketId();
                long tmpPubId = publishInboundInput.getPublishPacket().getTimestamp();
                //log.info("pub id: " + tmpPubId);
                ByteBuffer payload_client = publishInboundInput.getPublishPacket().getPayload().get();
                //per prima cosa invio pub ai miei client
                for (int i = 0; i < main.subscriptionTable.size(); i++) {
                    if (main.subscriptionTable.get(i).getTopic().equals(topic_client)) {
                        String clientId = main.subscriptionTable.get(i).getClientId();
                        main.sendPublishToClient(topic_client, payload_client, clientId);
                    }
                }
                forwardPub(topic_client, payload_client, broker_incoming_address, tmpPubId, id);
                break;
        }

    }



    public void resetTopicTree(String topic){
        //resetta le tabelle per un topic. Da chiamare quando un topicTree deve
        //essere ricostruito
        //log.info("reset topic tree");

        main.topicOverlays.get(topic).setRoot_address(null);
        main.topicOverlays.get(topic).setCompleted(false);
        main.topicOverlays.get(topic).setOnBuilding(false);
        for(CellTable cb : main.topicOverlays.get(topic).getTopicTable()) {
            cb.setNext_hop(false);
            cb.setPrev_hop(false);
            cb.setTotRtt(0);
        }


    }



    public void forwardPub(String topic, ByteBuffer payload, String broker_incoming_address, long pubId, String id){
        //inviare pub a tutti i broker all'interno del topicTree
        //log.info("in forward pub");
        //Caso 1: broker fa parte del topicTree
        if(!main.isNewTopicSub(topic)){
            //log.info("broker gia nel topic tree");


            //ora invio pub agli altri broker nell'albero
            String next_hop = findMyNextHop(topic);
            //log.info("next hop: " + next_hop);
            List<String> prev_hops = findMyPrevHops(topic);
            if(!prev_hops.isEmpty()){
                for (String prev_hop : prev_hops) {
                    //log.info("prev hop: " + prev_hop);
                    if(!prev_hop.equals(broker_incoming_address)){
                        String new_topic = "$TOPIC/" + topic + "/" + prev_hop
                                + "/FORWARD";

                        //log.info("mando a prev hop");
                        main.sendPublishToBroker(new_topic, payload);
                    }
                }
            }

            if(next_hop != null && !next_hop.equals(broker_incoming_address)){
                String new_topic = "$TOPIC/" + topic + "/" + next_hop
                        + "/FORWARD";

                //log.info("mando a next hop");
                main.sendPublishToBroker(new_topic, payload);
            }
        }else{
            log.info("broker fuori dal topic tree per client " + id);
            //log.info("addr for this id: " + main.clientBuf.get(id));
            if(main.clientBuf.get(id) == null) {
                //log.info("prima pub, chiedo a chi inviarla");
                PubBuffer pBuf = new PubBuffer();
                pBuf.setPayload(payload);
                pBuf.setClientId(id);
                main.pubBuffer.put(pubId, pBuf);
                byte[] milli_byte = longToBytes(pubId);
                payload = ByteBuffer.wrap(milli_byte);
                main.sendPublishToBroker("$PUBADV/" + topic, payload);
            } else {
                //log.info("non prima pub, so gia a chi inviare");
                String new_topic = "$TOPIC/" + topic + "/" + main.clientBuf.get(id)
                        + "/FORWARD";
                main.sendPublishToBroker(new_topic, payload);
            }
        }
        log.info("");

    }


    public void chooseRootOptTot(String topic){
        // costruisco ST provando ogni broker come radice
        // e mi salvo l'rtt totale
        for(CellTable c : main.topicOverlays.get(topic).getTopicTable()) {
            main.topicOverlays.get(topic).setRoot_address(c.getIp_address());
            computeTotalSTrtt(topic);
        }
        /*log.info("Lista RTT per ogni root: ");
        for(CellTable c : main.topicOverlays.get(topic).getTopicTable())
            log.info("TOT RTT con root " + c.getIp_address() + " è: " + c.getTotRtt());*/

        //ora che ho calcolato tutti gli rtt scelgo la root con rtt minore
        double min = 1000000.0;
        //log.info("my root count: " + min);
        String address = "";
        for (CellTable c : main.topicOverlays.get(topic).getTopicTable()) {
            //log.info("totRTT for root " + c.getIp_address() + ": " + c.getTotRtt());
                if(c.getTotRtt() < min) {
                    min = c.getTotRtt();
                    address = c.getIp_address();
                } else if(c.getTotRtt() == min){ //in caso siano uguali scelgo broker con indirizzo più piccolo
                    if (main.getLastAddressDigit(c.getIp_address()) < main.getLastAddressDigit(address)) {
                        min = c.getTotRtt();
                        address = c.getIp_address();

                    }

                }

        }
        main.topicOverlays.get(topic).setRoot_address(address);

        log.info("setto come root: " + main.topicOverlays.get(topic).getRoot_address() +
                " per topic " + topic);
        main.topicOverlays.get(topic).setOnBuilding(false);

    }


    public void chooseRootOptMax(String topic){
        // costruisco ST provando ogni broker come radice
        // e mi salvo l'rtt totale
        for(CellTable c : main.topicOverlays.get(topic).getTopicTable()) {
            main.topicOverlays.get(topic).setRoot_address(c.getIp_address());
            computeTotalSTrtt(topic);
        }
        /*log.info("Lista MaxRTT per ogni root: ");
        for(CellTable c : main.topicOverlays.get(topic).getTopicTable())
            log.info("TOT RTT con root " + c.getIp_address() + " è: " + c.getMaxRtt());*/

        //ora che ho calcolato tutti gli rtt scelgo la root con rtt minore
        double min = 1000000.0;
        //log.info("my root count: " + min);
        String address = "";
        for (CellTable c : main.topicOverlays.get(topic).getTopicTable()) {
            //log.info("totRTT for root " + c.getIp_address() + ": " + c.getTotRtt());
            if(c.getMaxRtt() < min) {
                min = c.getMaxRtt();
                address = c.getIp_address();
            } else if(c.getMaxRtt() == min){ //in caso siano uguali scelgo broker con indirizzo più piccolo
                if (main.getLastAddressDigit(c.getIp_address()) < main.getLastAddressDigit(address)) {
                    min = c.getMaxRtt();
                    address = c.getIp_address();

                }

            }

        }
        main.topicOverlays.get(topic).setRoot_address(address);

        log.info("setto come root: " + main.topicOverlays.get(topic).getRoot_address() +
                " per topic " + topic);
        main.topicOverlays.get(topic).setOnBuilding(false);

    }

    public void computeTotalSTrtt(String topic){
        for (CellTable c : main.topicOverlays.get(topic).getTopicTable()) {
            if (!c.getIp_address().equals(main.topicOverlays.get(topic).getRoot_address())) {
                simulateBestPathToRoot(topic, c.getIp_address(), main.topicOverlays.get(topic).getRoot_address());
            }
        }
    }

    public void simulateBestPathToRoot(String topic, String broker, String root){
        HashMap<String, Double> rttTable = buildRttTopicList(topic);
        //log.info("simulate best path for broker " + broker + " to root " + root);
        ArrayList<String> finalPath = new ArrayList<>();
        String r = String.valueOf(main.getLastAddressDigit(root));
        String b = String.valueOf(main.getLastAddressDigit(broker));
        List<String> level1;
        List<String> level2;
        //link diretto alla root
        double min = extractFromRttTable_addr(broker, root);
        //log.info("min path distance 1: " + min);
        finalPath.add(main.buildRttKey(r, b));
        //log.info("direct rtt to root: " + min);
        // distanza 1 (link diretto ad altri broker)
        level1 = pathsNotToRoot(broker, root, topic, 0, min);
        /*for(String key : rttTable.keySet())
            if(key.contains(String.valueOf(main.getLastAddressDigit(broker))) && rttTable.get(key)<min)
                level1.add(key);*/
        // path to root a distanza 1
        for(String l1 : level1){
            //log.info("cerco path migliori dal link " + l1);
            double rtt = extractFromRttTable_digit(r, getOtherBroker(l1, b));
            double new_rtt = rttTable.get(l1) + rtt;
            if(new_rtt < min) {
                //log.info("trovato path migliore");
                String l2 = main.buildRttKey(r, getOtherBroker(l1, b));
                min = new_rtt;
                //finalPath[0] = l1;
                //finalPath[1] = l2;
                finalPath.set(0, l1);
                while (finalPath.size() > 1)
                    finalPath.remove(finalPath.size()-1);
                finalPath.add(l2);
                //log.info("best path at distance 2 " + broker + " is: ");
                for (String s : finalPath)
                    log.info(s);
                //log.info("min cost distance 2: " + min);
            }

        }

        //distanza 2
        for(String l1 : level1) {
            //log.info("l1_path: " + l1);
            String endNode_l1 = getOtherBroker(l1, b);

            level2 = pathsNotToRoot(getAddresseFromLastDigit(endNode_l1), root, topic, rttTable.get(l1), min);
            /*log.info("possible middle path: ");
            for (String s : level2)
                log.info(s);*/
            if(level2.size() > 0) {
                for (String l2 : level2) {
                    if(!l2.equals(l1)){
                        //log.info("provo per path: " + l2);

                        //log.info("calcolo costo path totale");
                        double rtt = extractFromRttTable_digit(r, getOtherBroker(l2, endNode_l1));
                        double new_rtt = rttTable.get(l1) + rttTable.get(l2) + rtt;
                        //log.info("min cost: " + min);
                        //log.info("tot cost at d3: " + new_rtt);
                        if (new_rtt < min) {
                            //log.info("trovato path migliore");
                            String l3 = main.buildRttKey(r, getOtherBroker(l2, endNode_l1));
                            min = new_rtt;
                            //finalPath[0] = l1;
                            //finalPath[1] = l2;
                            finalPath.set(0, l1);
                            while (finalPath.size() > 1)
                                finalPath.remove(finalPath.size()-1);
                            finalPath.add(l2);
                            finalPath.add(l3);
                        }
                    }
                }
            }
        }

        //add rtt of this path to total RTT
        double rttPath = 0;
        double rttTot = main.topicOverlays.get(topic).getBrokerCell(main.topicOverlays.get(topic).getRoot_address()).getTotRtt();
        for (String s : finalPath){
            if(s.contains(String.valueOf(main.getLastAddressDigit(broker))))
                rttPath = rttPath + rttTable.get(s);
        }
        main.topicOverlays.get(topic).getBrokerCell(main.topicOverlays.get(topic).getRoot_address()).setTotRtt(rttTot + rttPath);

        double maxRtt = main.topicOverlays.get(topic).getBrokerCell(main.topicOverlays.get(topic).getRoot_address()).getMaxRtt();
        for (String s : finalPath){
            if(rttTable.get(s) > maxRtt)
                maxRtt = rttTable.get(s);
        }
        main.topicOverlays.get(topic).getBrokerCell(main.topicOverlays.get(topic).getRoot_address()).setMaxRtt(maxRtt);


        /*log.info("best path to root for broker " + broker + " is: ");
            for (String s : finalPath)
                log.info(s);
        double t = rttTot + rttPath;
        log.info("old rtt: " + rttTot);
        log.info("rtt path: " + rttPath);
        log.info("tot rtt: " + t);*/




    }

    public void createSpanningTree(String topic) {

        for (CellTable c : main.topicOverlays.get(topic).getTopicTable()) {
            if (!c.getIp_address().equals(main.topicOverlays.get(topic).getRoot_address())) {
                computeBestPathToRoot(topic, c.getIp_address(), main.topicOverlays.get(topic).getRoot_address());
            }
            if(c.getIp_address().equals(main.own_address)){
                log.info("");
                log.info("TopicTree completed for topic " + topic + " with ROOT: " + main.topicOverlays.get(topic).getRoot_address());
                log.info("Num of brokers in the tree: " + main.topicOverlays.get(topic).getTopicTable().size());
                log.info("");
            }


        }
    }

    public List<String> pathsNotToRoot(String broker, String root, String topic, double curCost, double min){
        HashMap<String, Double> rttTable = buildRttTopicList(topic);
        String r = String.valueOf(main.getLastAddressDigit(root));
        String b = String.valueOf(main.getLastAddressDigit(broker));
        List<String> paths = new ArrayList<>();
        //log.info("min: " + min);
        //log.info("curCost: " + curCost);
        for(String key : rttTable.keySet()){
            if(!key.contains(r) && key.contains(b)) {
                //log.info("Possible middle path sel: " + key);
                double rtt = rttTable.get(key);
                double rttPath = curCost + rtt;
                //log.info("rttPath: " + rttPath);
                if(rttPath<min) {
                    //log.info("add this path");
                    paths.add(key);
                }
            }
        }
        return paths;
    }

    public void computeBestPathToRoot(String topic, String broker, String root){
        HashMap<String, Double> rttTable = buildRttTopicList(topic);
        //log.info("compute best path for broker " + broker + " to root " + root);
        ArrayList<String> finalPath = new ArrayList<>();
        String r = String.valueOf(main.getLastAddressDigit(root));
        String b = String.valueOf(main.getLastAddressDigit(broker));
        List<String> level1 = new ArrayList<>();
        List<String> level2 = new ArrayList<>();
        //link diretto alla root
        double min = extractFromRttTable_addr(broker, root);
        //log.info("min path distance 1: " + min);
        finalPath.add(main.buildRttKey(r, b));
        //log.info("direct rtt to root: " + min);
        // distanza 1 (link diretto ad altri broker)
        level1 = pathsNotToRoot(broker, root, topic, 0, min);
        /*for(String key : rttTable.keySet())
            if(key.contains(String.valueOf(main.getLastAddressDigit(broker))) && rttTable.get(key)<min)
                level1.add(key);*/
        // path to root a distanza 1
        for(String l1 : level1){
            //log.info("cerco path migliori dal link " + l1);
            double rtt = extractFromRttTable_digit(r, getOtherBroker(l1, b));
            double new_rtt = rttTable.get(l1) + rtt;
            if(new_rtt < min) {
                //log.info("trovato path migliore");
                String l2 = main.buildRttKey(r, getOtherBroker(l1, b));
                min = new_rtt;
                //finalPath[0] = l1;
                //finalPath[1] = l2;
                finalPath.set(0, l1);
                while (finalPath.size() > 1)
                    finalPath.remove(finalPath.size()-1);
                finalPath.add(l2);
                //log.info("best path at distance 2 " + broker + " is: ");
                //for (String s : finalPath)
                   // log.info(s);
                //log.info("min cost distance 2: " + min);
            }

        }

        //distanza 2 TODO
        for(String l1 : level1) {
            //log.info("l1_path: " + l1);
            String endNode_l1 = getOtherBroker(l1, b);

            level2 = pathsNotToRoot(getAddresseFromLastDigit(endNode_l1), root, topic, rttTable.get(l1), min);
            /*log.info("possible middle path: ");
            for (String s : level2)
                log.info(s);*/
            if(level2.size() > 0) {
                for (String l2 : level2) {
                    if(!l2.equals(l1)){
                        //log.info("provo per path: " + l2);

                        //log.info("calcolo costo path totale");
                        double rtt = extractFromRttTable_digit(r, getOtherBroker(l2, endNode_l1));
                        double new_rtt = rttTable.get(l1) + rttTable.get(l2) + rtt;
                        //log.info("min cost: " + min);
                        //log.info("tot cost at d3: " + new_rtt);
                        if (new_rtt < min) {
                            //log.info("trovato path migliore");
                            String l3 = main.buildRttKey(r, getOtherBroker(l2, endNode_l1));
                            min = new_rtt;
                            //finalPath[0] = l1;
                            //finalPath[1] = l2;
                            finalPath.set(0, l1);
                            while (finalPath.size() > 1)
                                finalPath.remove(finalPath.size()-1);
                            finalPath.add(l2);
                            finalPath.add(l3);
                        }
                    }
                }
            }
        }


        /*log.info("final best path to root " + root + "for broker " + broker + " is: ");
        for (String s : finalPath)
            log.info(s);*/

        //setto gli next hops
        if(broker.equals(main.own_address)) {
            //log.info("setto next hop");
            String myPath = finalPath.get(0);
            String[] addresses = getAddressesFromRttKey(myPath);
            for (String a : addresses)
                if (!a.equals(main.own_address)) {
                    main.topicOverlays.get(topic).getBrokerCell(a).setNext_hop(true);

                }
        } else { //setto prev hops
            //log.info("setto prev hop");
            String myLastDigit = String.valueOf(main.getLastAddressDigit(main.own_address));
            if(finalPath.contains(main.buildRttKey(myLastDigit, b))) {
                //log.info("broker " + broker + " è il mio prev hop");
                main.topicOverlays.get(topic).getBrokerCell(broker).setPrev_hop(true);
            }
        }

        main.topicOverlays.get(topic).setCompleted(true);
        main.topicOverlays.get(topic).setOnBuilding(false);

    }

    public HashMap<String, Double> buildRttTopicList(String topic){
        HashMap<String, Double> rttTable = new HashMap<>();
        ArrayList<String> nodes = new ArrayList<>();
        for(CellTable c : main.topicOverlays.get(topic).getTopicTable())
            nodes.add(String.valueOf(main.getLastAddressDigit(c.getIp_address())));
        for(String key : main.rttTable.keySet()){
            String[] parts = key.split(":");
            if(nodes.contains(parts[0]) && nodes.contains(parts[1]))
                rttTable.put(key, main.rttTable.get(key));
        }
        return rttTable;
    }

    public void updateRttTable(String rttList){
        /*log.info("rtt list: " + rttList);
        log.info("actual rtt table: ");
        for(String key: main.rttTable.keySet())
            log.info(key + " --> " +main.rttTable.get(key));*/
        String[] rtt_parts = rttList.split("_");
        for(String rtt : rtt_parts) {
            String[] rtt_cell = rtt.split("-");
            if(!main.rttTable.containsKey(rtt_cell[0]))
                main.rttTable.put(rtt_cell[0], Double.parseDouble(rtt_cell[1]));
            else if(main.rttTable.get(rtt_cell[0]) != Double.parseDouble(rtt_cell[1]))
                main.rttTable.replace(rtt_cell[0], Double.parseDouble(rtt_cell[1]));
        }
        /*log.info("rttTable updated: ");
        for(String key: main.rttTable.keySet())
            log.info(key + " --> " +main.rttTable.get(key));*/
    }

    ///////////////////////////////////// UTILS /////////////////////////////////////7

    public boolean checkBrokerInTopicTable(String topic, String address){
        boolean b = false;
        for(CellTable c : main.topicOverlays.get(topic).getTopicTable())
            if(c.getIp_address().equals(address)){
                b = true;
                break;
            }
        return b;
    }

    public String findMyNextHop(String topic){
        String ip_address = null;
        for(int i = 0; i< main.topicOverlays.get(topic).getTopicTable().size(); i++){
            if(main.topicOverlays.get(topic).getTopicTable().get(i).isNext_hop()){
                //log.info("next hop founded");
                ip_address = main.topicOverlays.get(topic).getTopicTable().get(i).getIp_address();
            }
        }
        return ip_address;
    }

    public List<String> findMyPrevHops(String topic){
        List<String> myPrevHops = new ArrayList<>();
        for(int i = 0; i<main.topicOverlays.get(topic).getTopicTable().size(); i++){
            if(main.topicOverlays.get(topic).getTopicTable().get(i).isPrev_hop()){
                myPrevHops.add(main.topicOverlays.get(topic).getTopicTable().get(i).getIp_address());
            }
        }
        return myPrevHops;
    }


    public String getOtherBroker(String key, String c){
        String[] parts = key.split(":");
        String b1 = parts[0];
        String b2 = parts[1];
        if(b1.equals(c))
            return b2;
        else
            return b1;
    }


    public double extractFromRttTable_addr(String addr1, String addr2){
        double rtt;
        int a1 = main.getLastAddressDigit(addr1);
        int a2 = main.getLastAddressDigit(addr2);
        //log.info("extract rtt of " + a1 + ":" + a2);
        if(a1<a2)
            rtt = main.rttTable.get(a1 + ":" + a2);
        else
            rtt = main.rttTable.get(a2 + ":" + a1);

        return rtt;

    }


    public double extractFromRttTable_digit(String digit1, String digit2){
        double rtt;
        int a1 = Integer.parseInt(digit1);
        int a2 = Integer.parseInt(digit2);
        if(a1<a2)
            rtt = main.rttTable.get(a1 + ":" + a2);
        else
            rtt = main.rttTable.get(a2 + ":" + a1);

        return rtt;

    }

    public String[] getAddressesFromRttKey(String key){
        String[] parts = key.split(":");
        String addr1 = "172.17.0." + parts[0];
        String addr2 = "172.17.0." + parts[1];
        String[] addresses = new String[2];
        addresses[0] = addr1;
        addresses[1] = addr2;
        return addresses;
    }

    private String bb_to_str(ByteBuffer buffer, Charset charset) {
        byte[] bytes;
        if(buffer.hasArray()) {
            bytes = buffer.array();
        } else {
            bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
        }
        return new String(bytes, charset);
    }

    public byte[] longToBytes(long x) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(x);
        return buffer.array();
    }

    public long bytesToLong(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put(bytes);
        buffer.flip();//need flip
        return buffer.getLong();
    }

    public String getAddresseFromLastDigit(String d){
        String addr = "172.17.0." + d;
        return addr;
    }


}