package org.example;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.interceptor.puback.PubackInboundInterceptor;
import com.hivemq.extension.sdk.api.interceptor.puback.parameter.PubackInboundInput;
import com.hivemq.extension.sdk.api.interceptor.puback.parameter.PubackInboundOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class PubAckInterceptor implements PubackInboundInterceptor {
    private static final Logger log = LoggerFactory.getLogger(TopicTreeMain.class);
    TopicTreeMain main = new TopicTreeMain();
    public static Charset charset = StandardCharsets.UTF_8;

    public PubAckInterceptor(TopicTreeMain main) {
        this.main = main;
    }

    public void onInboundPuback(@NotNull PubackInboundInput pubackInboundInput, @NotNull PubackInboundOutput pubackInboundOutput){
        log.info("received puback");
        log.info(String.valueOf(pubackInboundInput.getPubackPacket().getUserProperties()));
        log.info("addr: " + String.valueOf(pubackInboundInput.getConnectionInformation().getInetAddress()));

    }

}
