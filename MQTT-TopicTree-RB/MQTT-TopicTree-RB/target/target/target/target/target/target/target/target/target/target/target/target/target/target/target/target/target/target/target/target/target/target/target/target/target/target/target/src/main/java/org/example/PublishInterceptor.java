
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

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
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
                int numOfbrokers = Integer.parseInt(numofb_str);
                //updateRttTable(payload_parts[1]);
                if(main.interestedTopic.contains(topic_received_part2) && numOfbrokers == main.topicOverlays.get(topic_received_part2).getTopicTable().size() ) {
                    if (main.topicOverlays.get(topic_received_part2).getTopicTable().size() != main.topicOverlays.get(topic_received_part2).getNumBrokers()) {
                            resetTopicTree(topic_received_part2);
                            main.onBuilding = true;
                            log.info("ho ricevuto nuovi broker per topic " + topic_received_part2 + ", posso costruire nuovo tree con n broker: " + main.topicOverlays.get(topic_received_part2).getTopicTable().size());
                            main.topicOverlays.get(topic_received_part2).setNumBrokers(main.topicOverlays.get(topic_received_part2).getTopicTable().size());
                            sendRootCountReq(topic_received_part2);

                    }
                }
                break;
            case "$SUBADV":
                main.stopPing = false;
                //log.info("SUBADV receveid from " + broker_incoming_address);
                topic_received_part2 = parts[1];
                if(!main.isNewTopicSub(topic_received_part2)){
                    //log.info("anche io ho la stessa sub, mi salvo il broker se non l'ho gia salvato");
                    if(!checkBrokerInTopicTable(topic_received_part2, broker_incoming_address)){
                        log.info("broker " + broker_incoming_address + " nuovo, lo salvo");
                        CellTable cb = new CellTable();
                        cb.setIp_address(broker_incoming_address);
                        main.topicOverlays.get(topic_received_part2).addBroker(cb);
                        main.topicOverlays.get(topic_received_part2).setCompleted(false);
                        //main.msg_count = 0;

                    }
                    log.info("Rispondo al broker del mio stesso interesse");
                    //invio subresp
                    //log.info("$SUBRESP/" + broker_incoming_address + "/" + topic_received_part2);

                    main.sendPublishToBroker("$SUBRESP/" + broker_incoming_address + "/" + topic_received_part2, "");
                    //log.info("sub resp inviata");
                }
            break;

            case "$SUBRESP":

                topic_received_part2 = parts[1]; //broker inc addr
                topic_received_part3 = parts[2]; //topic
                //log.info("ricevuta SUBRESP per topic " + topic_received_part3 + "da " + broker_incoming_address);
                if(!checkBrokerInTopicTable(topic_received_part3, broker_incoming_address)) {
                    //log.info("broker " + broker_incoming_address + " nuovo, lo salvo");
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
                if(!main.pubBuffer.get(pubId).getSent()){
                    main.clientBuf.put(cId, broker_incoming_address);
                    main.pubBuffer.get(pubId).setsent(true);
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
                switch(topic_received_part3){
                    case "ROOTCOUNTREQ":
                            //log.info("ROOTCOUNTREQ da " + broker_incoming_address);
                        if (main.interestedTopic.contains(topic_received_part2)){
                            double balance_value = computeBalanceRootValue(topic_received_part2);
                            main.b_value = balance_value;
                            //log.info("il mio balance value è: " + balance_value);

                            main.sendPublishToBroker("$TOPIC/" + topic_received_part2 + "/" +
                                    broker_incoming_address + "/ROOTCOUNT", String.valueOf(balance_value));
                        }
                        break;
                    case "ROOTCOUNT":
                        if (main.interestedTopic.contains(topic_received_part2)) {
                            payload_bb = publishInboundInput.getPublishPacket().getPayload().orElse(null);
                            String bv_str = bb_to_str(payload_bb, charset);
                            Double b_value = Double.parseDouble(bv_str);
                            //log.info("received ROOTCOUNT da " + broker_incoming_address + " with count: " + rootCount);

                            main.topicOverlays.get(topic_received_part2).getBrokerCell(broker_incoming_address).setRootCount(b_value);

                            boolean allCount = true;
                            for (CellTable c : main.topicOverlays.get(topic_received_part2).getTopicTable())
                                if (!c.getIp_address().equals(main.own_address) && c.getRootCount() == -1.0) {
                                    //log.info("mi manca ancora la root count di " + c.getIp_address());
                                    allCount = false;
                                    break;
                                }
                            if (allCount) {
                                //log.info("all root count: ");
                                //chooseRoot(topic_received_part2);
                                chooseRootTest(topic_received_part2);
                                log.info("start tree creation");
                                createSpanningTree(topic_received_part2);
                            }
                        }

                        break;
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
                            if(main.topicOverlays.get(topic_received_part2).getNumBrokers()>0)
                                sendRootCountReq(topic_received_part2);
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
            case "bvalue":
                log.info(main.own_address + "  bvalue: " + main.b_value);
                break;
            //messaggi ricevuti dai client
            default:
                main.stopPing = true;
                String id = publishInboundInput.getClientInformation().getClientId();
                if(!main.clientBuf.containsKey(id))
                    main.clientBuf.put(id, null);
                String topic_client = publishInboundInput.getPublishPacket().getTopic();
                //int pub_id = publishInboundInput.getPublishPacket().getPacketId();
                long tmpPubId = publishInboundInput.getPublishPacket().getTimestamp();
                //log.info("pub id: " + tmpPubId);
                ByteBuffer payload_client = publishInboundInput.getPublishPacket().getPayload().get();
                //per prima cosa invio pub ai miei client
                if(main.topicOverlays.containsKey(topic_client))
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





    public void sendRootCountReq(String topic){
        //log.info("send root count req");
        int nob = main.topicOverlays.get(topic).getTopicTable().size();
        String numOfBroker = String.valueOf(nob);
        // resetto rootcount
        for(CellTable c : main.topicOverlays.get(topic).getTopicTable())
            if (!c.getIp_address().equals(main.own_address))
                c.setRootCount(-1);

        // invio root_count req solo agli altri broker interessati
        for(CellTable c : main.topicOverlays.get(topic).getTopicTable()) {
            if (!c.getIp_address().equals(main.own_address)) {
                main.sendPublishToBroker("$TOPIC/" + topic + "/" +
                        c.getIp_address() + "/ROOTCOUNTREQ", numOfBroker);
            }
        }
    }

    public void chooseRoot(String topic){
        //log.info("set root...");
        double min = main.b_value;
        log.info(main.own_address + "b_value: " + min);

        String address = main.own_address;
        for (CellTable c : main.topicOverlays.get(topic).getTopicTable()) {
            if (!c.getIp_address().equals(main.own_address)) {
                log.info(c.getIp_address() + " b_value: " + c.getRootCount());
                if(c.getRootCount() < min) {
                    min = c.getRootCount();
                    address = c.getIp_address();
                } else if(c.getRootCount() == min){ //in caso siano uguali scelgo broker con indirizzo più piccolo
                    if (main.getLastAddressDigit(c.getIp_address()) < main.getLastAddressDigit(address)) {
                        min = c.getRootCount();
                        address = c.getIp_address();

                    }


               }
            }

        }
        main.topicOverlays.get(topic).setRoot_address(address);

        log.info("setto come root: " + main.topicOverlays.get(topic).getRoot_address() +
                " per topic " + topic);
        //log.info("number of brokers: " + main.topicOverlays.get(topic).getTopicTable().size());
        main.topicOverlays.get(topic).setOnBuilding(false);
        //log.info(("setto next hop..."));





    }

    public void chooseRootTest(String topic){
        int n = Integer.parseInt(topic.substring(1));
        int r = n % main.topicOverlays.get(topic).getTopicTable().size();

        String[] addresses = new String[main.topicOverlays.get(topic).getTopicTable().size()];
        int i = 0;
        for(CellTable ct: main.topicOverlays.get(topic).getTopicTable()){
            addresses[i] = ct.getIp_address();
            i++;
        }
        for(String s: addresses)
            log.info(s);
        String[] orderedAddr = orderAddress(addresses);
        for(String s: orderedAddr)
            log.info(s);
        log.info("r: " + r);
        main.topicOverlays.get(topic).setRoot_address(orderedAddr[r]);
        log.info("set root " + orderedAddr[r] + "for topic " + topic);

    }


    public void resetTopicTree(String topic){
        //resetta le tabelle per un topic. Da chiamare quando un topicTree deve
        //essere ricostruito
        //log.info("reset topic tree");
        //main.root_sent = false;
        //if(main.own_address.equals(main.topicOverlays.get(topic).getRoot_address())) {
          //  main.root_count--;
            //log.info("ero la root, diminuisco root_count: ora rootcount = " + main.root_count);
        //}
        main.topicOverlays.get(topic).setRoot_address(null);
        main.topicOverlays.get(topic).setCompleted(false);
        main.topicOverlays.get(topic).setOnBuilding(false);
        for(CellTable cb : main.topicOverlays.get(topic).getTopicTable()) {
            cb.setNext_hop(false);
            cb.setPrev_hop(false);
            cb.setRootCount(-1.0);
        }


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



    public void forwardPub(String topic, ByteBuffer payload, String broker_incoming_address, long pubId, String id){
        //inviare pub a tutti i broker all'interno del topicTree
        //log.info("in forward pub");
        //Caso 1: broker fa parte del topicTree
        if(!main.isNewTopicSub(topic)){
            //log.info("broker gia nel topic tree");


            //ora invio pub agli altri broker nell'albero
            String next_hop = findMyNextHop(topic);
            List<String> prev_hops = findMyPrevHops(topic);
            if(!prev_hops.isEmpty()){
                for (String prev_hop : prev_hops) {
                    if(!prev_hop.equals(broker_incoming_address)){
                        String new_topic = "$TOPIC/" + topic + "/" + prev_hop
                                + "/FORWARD";
                        main.sendPublishToBroker(new_topic, payload);
                    }
                }
            }

            if(next_hop != null && !next_hop.equals(broker_incoming_address)){
                String new_topic = "$TOPIC/" + topic + "/" + next_hop
                        + "/FORWARD";
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

    }

    public void createSpanningTree(String topic) {
        if(main.own_address.equals(main.topicOverlays.get(topic).getRoot_address())) {
            main.topicOverlays.get(topic).setCompleted(true);
            main.root_count++;
        }

        for (CellTable c : main.topicOverlays.get(topic).getTopicTable()) {
            if (!c.getIp_address().equals(main.topicOverlays.get(topic).getRoot_address())) {
                computeBestPathToRoot(topic, c.getIp_address(), main.topicOverlays.get(topic).getRoot_address());
            }
            if(c.getIp_address().equals(main.own_address)){
                log.info("TopicTree completed for topic " + topic + " with ROOT: " + main.topicOverlays.get(topic).getRoot_address());
                log.info("Num of brokers in the tree: " + main.topicOverlays.get(topic).getTopicTable().size());
            }


        }
        main.onBuilding = false;
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
                    //log.info(s);
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


        /*log.info("final best path to root for broker " + broker + " is: ");
            for (String s : finalPath)
                log.info(s);*/

        //setto gli next hops
        if(broker.equals(main.own_address)) {
            //log.info("setto next hop");
            String myPath = finalPath.get(0);
            String[] addresses = getAddressesFromRttKey(myPath);
            for (String a : addresses) {
                if (!a.equals(main.own_address)) {
                    main.topicOverlays.get(topic).getBrokerCell(a).setNext_hop(true);
                }
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

    public String getAddresseFromLastDigit(String d){
        String addr = "172.17.0." + d;
        return addr;
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

    public int computeNumRootCount(){
        int count = 0;
        for(String key: main.topicOverlays.keySet())
            if(main.topicOverlays.get(key).getRoot_address() != null)
                if(main.topicOverlays.get(key).getRoot_address().equals(main.own_address))
                    count++;
        return count;
    }

    public int computeNumTrees(){
        int count = 0;
        for(String key: main.topicOverlays.keySet())
            if(main.topicOverlays.get(key).getRoot_address() != null)
                if(main.topicOverlays.get(key).getRoot_address().equals(main.own_address))
                    count++;
        return count;
    }


    public double computeBalanceRootValue(String topic){
        double alpha = 1;
        double beta = 0.5;
        int rc = computeNumRootCount();
        if(main.topicOverlays.get(topic).getRoot_address() != null)
            if (main.topicOverlays.get(topic).getRoot_address().equals(main.own_address))
                rc--;
        /*if (main.topicOverlays.get(topic).getCompleted()) {
            //log.info("tree gia completato");

        } else if (main.topicOverlays.get(topic).getRoot_address() != null) {
            //log.info("tree in costruzione");
            if (main.topicOverlays.get(topic).getRoot_address().equals(main.own_address))
                rc--;
        }*/
        //log.info("my root count: " + rc);
        int numOfClients = main.subscriptionTable.size();
        double value = alpha * rc + beta * numOfClients;
        /*log.info("Own address: " + main.own_address);
        log.info("root count: " + rc);
        log.info("num of clients: " + numOfClients);
        log.info("bvalue: " + value);*/
        return value;
    }

    public String[] orderAddress(String[] addresses){
        int[] a = new int[addresses.length];
        for(int i=0; i<addresses.length;i++)
            a[i] = main.getLastAddressDigit(addresses[i]);
        Arrays.sort(a);

        String[] orderedAddr = new String[addresses.length];
        for(int j=0;j<a.length;j++)
            orderedAddr[j] = getAddresseFromLastDigit(String.valueOf(a[j]));

        return orderedAddr;
    }

}