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
import com.hivemq.extension.sdk.api.client.parameter.ConnectionInformation;
import com.hivemq.extension.sdk.api.events.client.ClientLifecycleEventListener;
import com.hivemq.extension.sdk.api.events.client.parameters.AuthenticationSuccessfulInput;
import com.hivemq.extension.sdk.api.events.client.parameters.ConnectionStartInput;
import com.hivemq.extension.sdk.api.events.client.parameters.DisconnectEventInput;
import com.hivemq.extension.sdk.api.packets.general.MqttVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * This is a very simple {@link ClientLifecycleEventListener}
 * which logs the MQTT version and identifier of every connecting client.
 *
 * @author Florian Limpöck
 * @since 4.0.0
 */
public class TopicTreeListener implements ClientLifecycleEventListener {

    private static final Logger log = LoggerFactory.getLogger(TopicTreeMain.class);
    TopicTreeMain main = new TopicTreeMain();
    public static Charset charset = StandardCharsets.UTF_8;
    public TopicTreeListener(TopicTreeMain TopicTreeMain) {
        this.main = TopicTreeMain;

    }

    @Override
    //metodo chiamato quando il broker si collega a un altro broker tramite bridging
    public void onMqttConnectionStart(final @NotNull ConnectionStartInput connectionStartInput) {
        log.info("onMqttConnectionStart...");
        ConnectionInformation connectionInformation = connectionStartInput.getConnectionInformation();
        String address_old = String.valueOf(connectionInformation.getInetAddress().orElse(null));
        String address = address_old.substring(1);
        CellTable new_cell = new CellTable();
        log.info("connection from " + address);

        //log.info("{}",connectionStartInput.getConnectPacket().getKeepAlive());
        //log.info("keepAlive: " + connectionStartInput.getConnectPacket().getKeepAlive());
        int keepAlive = connectionStartInput.getConnectPacket().getKeepAlive();

        String id = connectionStartInput.getClientInformation().getClientId();
        if (keepAlive == 11) {
            log.info("");
            log.info("Connection from broker " + address);
            log.info("MY ADDRESS: " + main.own_address);
            log.info("");
        } else {
            log.info("connection from client " + id);
        }





        /*
        //add address in table only if not already present and if is a broker
        if(address != null && keepAlive == 11){
            new_cell.setId(id);
            new_cell.setIp_address(address);
            new_cell.setRTT(-1);
            new_cell.setC(-1);

            boolean addr_in_table = false;
            for(int i = 0; i < main.table.size(); i++){
                String cell_address = main.table.get(i).getIp_address();
                if(cell_address.equals(address)){
                    addr_in_table = true;
                    break;
                }
            }
            if(!addr_in_table){
                log.info("here");
                main.table.add(new_cell);
            }

        }
        log.info("main table: ");
        main.printMainTable();
    */
    }

    @Override
    public void onAuthenticationSuccessful(final @NotNull AuthenticationSuccessfulInput authenticationSuccessfulInput) {

    }

    @Override
    public void onDisconnect(final @NotNull DisconnectEventInput disconnectEventInput) {
        String clientId = disconnectEventInput.getClientInformation().getClientId();
        if(main.clientBuf.containsKey(clientId))
            main.clientBuf.remove(clientId);
        main.removeSub(clientId);
        log.info("Client disconnected with id: {} ", clientId);
    }
}
