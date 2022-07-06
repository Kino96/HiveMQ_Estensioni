package org.example;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.interceptor.disconnect.DisconnectInboundInterceptor;
import com.hivemq.extension.sdk.api.interceptor.disconnect.parameter.DisconnectInboundInput;
import com.hivemq.extension.sdk.api.interceptor.disconnect.parameter.DisconnectInboundOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class DisconnectInterceptor implements DisconnectInboundInterceptor {
    private static final Logger log = LoggerFactory.getLogger(TopicTreeMain.class);
    TopicTreeMain main = new TopicTreeMain();
    public static Charset charset = StandardCharsets.UTF_8;

    public DisconnectInterceptor(TopicTreeMain main) {
        this.main = main;
    }

    @Override
    public void onInboundDisconnect(@NotNull DisconnectInboundInput disconnectInboundInput, @NotNull DisconnectInboundOutput disconnectInboundOutput) {
        log.info("disconnect interceptor working");
        String clientId = disconnectInboundInput.getClientInformation().getClientId();
        //remove from table
        main.removeSub(clientId);
        String address = String.valueOf(disconnectInboundInput.getConnectionInformation().getInetAddress().orElse(null));
        log.info("broker disconnected: ip {}", address);
    }
}
