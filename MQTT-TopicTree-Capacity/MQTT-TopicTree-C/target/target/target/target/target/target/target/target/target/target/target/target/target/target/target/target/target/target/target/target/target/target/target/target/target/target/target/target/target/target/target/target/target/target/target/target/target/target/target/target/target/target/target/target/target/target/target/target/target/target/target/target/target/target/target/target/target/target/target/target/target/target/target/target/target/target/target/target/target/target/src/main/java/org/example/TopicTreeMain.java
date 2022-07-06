
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

import com.hivemq.extension.sdk.api.ExtensionMain;
import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.events.EventRegistry;
import com.hivemq.extension.sdk.api.parameter.*;
import com.hivemq.extension.sdk.api.services.Services;
import com.hivemq.extension.sdk.api.services.builder.Builders;
import com.hivemq.extension.sdk.api.services.builder.PublishBuilder;
import com.hivemq.extension.sdk.api.services.intializer.ClientInitializer;
import com.hivemq.extension.sdk.api.services.intializer.InitializerRegistry;
import com.hivemq.extension.sdk.api.services.publish.Publish;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

/**
 * This is the main class of the extension,
 * which is instantiated either during the HiveMQ start up process (if extension is enabled)
 * or when HiveMQ is already started by enabling the extension.
 *
 * @author Florian Limpöck
 * @since 4.0.0
 */
public class TopicTreeMain implements ExtensionMain {

    private static final @NotNull Logger log = LoggerFactory.getLogger(TopicTreeMain.class);
    public static Charset charset = StandardCharsets.UTF_8;

    public HashMap<String, TopicTable> topicOverlays = new HashMap<>();
    public List<CellTable> table = new ArrayList<>();
    public List<SubTable> subscriptionTable = new ArrayList<>();
    public String own_address = null;
    public double capacity = -1;
    //public int msg_count = 0;
    public List<String> interestedTopic = new ArrayList<>();
    public HashMap<Long, PubBuffer> pubBuffer = new HashMap<>();
    public HashMap<String, Double> rttTable = new HashMap<>();
    public BufferedReader reader;
    public HashMap<String, Double> myRttTable = new HashMap<>();
    public HashMap<String, String> clientBuf = new HashMap<>();
    public boolean testFlag = false;
    public boolean stopPing = false;
    public boolean onBuilding = false;

    @Override
    public void extensionStart(final @NotNull ExtensionStartInput extensionStartInput, final @NotNull ExtensionStartOutput extensionStartOutput) {

        try {

            final ExtensionInformation extensionInformation = extensionStartInput.getExtensionInformation();
            log.info("Started " + extensionInformation.getName() + ":" + extensionInformation.getVersion());


            InetAddress inetAddress;
            inetAddress = InetAddress.getLocalHost();
            own_address = inetAddress.getHostAddress();

            //compute capacity
            Runtime runtime = Runtime.getRuntime();
            long maxMemory = runtime.maxMemory();
            int cpu_cores = runtime.availableProcessors();

            double alpha, beta;
            int gamma;
            alpha = 0.000_000_1;
            beta = 1000.0;

            if(own_address.equals("172.17.0.4")){
                gamma = 100;
            }else{
                Random random = new Random();
                gamma = random.nextInt(99);
            }

            double c_value = alpha*maxMemory + beta*cpu_cores + gamma;
            capacity = c_value;

            log.info("own IP address: " + own_address + " with C: " + capacity);

            //salvo RTT da file
            try {
                reader = new BufferedReader(new FileReader("/opt/hivemq-4.7.2/extensions/Rtt.txt"));
                String line;
                line = reader.readLine();


                while (line != null) {
                    //log.info("riga: " + line);
                    String[] parts = line.split(":");
                    String key = buildRttKey(String.valueOf(getLastAddressDigit(own_address)), String.valueOf(getLastAddressDigit(parts[0])));
                    myRttTable.put(key, Double.valueOf(parts[1]));
                    //rttTable.put(key, Double.valueOf(parts[1]));
                    //log.info("rtt for " + key + ": " + parts[1]);
                    line = reader.readLine();
                }

                reader.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }

            hardCodeRTTUniform();

            //addConnectModifier();
            addClientLifecycleEventListener();
            addInterceptors();


        } catch (Exception e) {
            log.error("Exception thrown at extension start: ", e);
        }

    }

    @Override
    public void extensionStop(final @NotNull ExtensionStopInput extensionStopInput, final @NotNull ExtensionStopOutput extensionStopOutput) {

        final ExtensionInformation extensionInformation = extensionStopInput.getExtensionInformation();
        log.info("Stopped " + extensionInformation.getName() + ":" + extensionInformation.getVersion());

    }

    private void addInterceptors() {

        final PublishInterceptor publishInterceptor = new PublishInterceptor(this);
        final PublishOutInterceptor publishOutInterceptor = new PublishOutInterceptor(this);
        final PingReqInterceptor pingReqInterceptor = new PingReqInterceptor(this);
        final SubInterceptor subInterceptor = new SubInterceptor(this);
        final DisconnectInterceptor disconnectInterceptor = new DisconnectInterceptor(this);
        final ClientInitializer clientInitializer = (initializerInput, clientContext) -> {

            clientContext.addPublishInboundInterceptor(publishInterceptor);
            clientContext.addPingReqInboundInterceptor(pingReqInterceptor);
            clientContext.addPublishOutboundInterceptor(publishOutInterceptor);
            clientContext.addSubscribeInboundInterceptor(subInterceptor);
            clientContext.addDisconnectInboundInterceptor(disconnectInterceptor);
        };
        Services.initializerRegistry().setClientInitializer(clientInitializer);

    }

    private void addClientLifecycleEventListener() {
        final EventRegistry eventRegistry = Services.eventRegistry();
        final TopicTreeListener TopicTreeListener = new TopicTreeListener(this);
        eventRegistry.setClientLifecycleEventListener(input -> TopicTreeListener);
    }


    public void printMainTable(){
        int i=0;
        for(i=0;i<table.size();i++){
            table.get(i).printCellTable();
        }
    }

    public boolean isNewTopicSub(String topic){
        //log.info("controllo lista sub list");
        boolean isFirst = true;
        for(SubTable s: subscriptionTable){
            //log.info("controllo se " + s.getTopic() + "= " + topic);
            if(topic.equals(s.getTopic()))
                isFirst = false;
        }
        return isFirst;
    }

    public void removeSub(String clientId){
        for(int i=0;i<subscriptionTable.size();i++){
            if(clientId.equals(subscriptionTable.get(i).getClientId())) {
                String topic = subscriptionTable.get(i).getTopic();
                subscriptionTable.remove(i);
                //se non ho più sottoiscrizioni a quel topic, devo rimuovere il broker dal topictree
                if(isNewTopicSub(topic)){
                    interestedTopic.remove(topic);
                    String new_topic;
                    log.info("questo broker non è più interessato al topic " + topic + ", " +
                            "lo devo rimuovere dal topicTree");
                    //aggiornare mie tabelle, e inviare messaggio ad altri broker per avvisarli
                   for(CellTable c : topicOverlays.get(topic).getTopicTable())
                        if(!c.getIp_address().equals(own_address)) {
                            new_topic = "$TOPIC/" + topic + "/" + c.getIp_address() + "/REMOVEBROKER";
                            sendPublishToBroker(new_topic, "");
                        }
                    topicOverlays.remove(topic);
                }
            }
        }

    }

    public void sendPublishToBroker(String topic, String pub_payload){
        //log.info("invio a broker topic " + topic);
        PublishBuilder publishBuilder = Builders.publish();
        publishBuilder.topic(topic);
        String payload_string = pub_payload;
        ByteBuffer payload = ByteBuffer.wrap(payload_string.getBytes(charset));
        publishBuilder.payload(payload);
        Publish publish = publishBuilder.build();
        Services.publishService().publish(publish);
    }

    public void sendPublishToClient(String topic, ByteBuffer payload, String clientId){
        log.info("pubblico al subscriber sul topic " + topic);
        PublishBuilder publishBuilder = Builders.publish();
        publishBuilder.topic(topic);
        publishBuilder.payload(payload);
        Publish publish = publishBuilder.build();
        Services.publishService().publishToClient(publish, clientId);
    }

    public void sendPublishToBroker(String topic, ByteBuffer pub_payload){
        //log.info("invio a broker topic " + topic);
        PublishBuilder publishBuilder = Builders.publish();
        publishBuilder.topic(topic);
        publishBuilder.payload(pub_payload);
        Publish publish = publishBuilder.build();
        Services.publishService().publish(publish);
    }

    public int getLastAddressDigit(String address){
        String[] addr_parts = address.split("\\.");
        String last = addr_parts[3];
        int a = Integer.parseInt(last);
        return a;
    }


    public void hardCodeRTT4B(){
        rttTable.put("2:3", 60.1);
        rttTable.put("2:4", 1.20);
        rttTable.put("2:5", 40.35);
        rttTable.put("3:4", 60.11);
        rttTable.put("3:5", 100.50);
        rttTable.put("4:5", 40.15);
    }

    public void hardCodeRTT5B(){
        rttTable.put("2:3", 60.1);
        rttTable.put("2:4", 1.20);
        rttTable.put("2:5", 40.35);
        rttTable.put("2:6", 1.25);
        rttTable.put("3:4", 60.11);
        rttTable.put("3:5", 100.50);
        rttTable.put("3:6", 60.55);
        rttTable.put("4:5", 40.15);
        rttTable.put("4:6", 1.05);
        rttTable.put("5:6", 40.45);
    }

    public void hardCodeRTTUniform(){
        rttTable.put("2:3", 10.0);
        rttTable.put("2:4", 10.0);
        rttTable.put("2:5", 10.0);
        rttTable.put("2:6", 10.0);
        rttTable.put("3:4", 10.0);
        rttTable.put("3:5", 10.0);
        rttTable.put("3:6", 10.0);
        rttTable.put("4:5", 10.0);
        rttTable.put("4:6", 10.0);
        rttTable.put("5:6", 10.0);
    }

    public String buildRttKey(String digit1, String digit2){
        String key;
        int a1 = Integer.parseInt(digit1);
        int a2 = Integer.parseInt(digit2);
        if(a1<a2)
            key = digit1 + ":" + digit2;
        else
            key = digit2 + ":" + digit1;

        return key;

    }




}
