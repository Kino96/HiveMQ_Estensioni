package org.example;

import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.interceptor.pingreq.parameter.PingReqInboundInput;
import com.hivemq.extension.sdk.api.interceptor.pingreq.parameter.PingReqInboundOutput;
import com.hivemq.extension.sdk.api.interceptor.pingresp.PingRespOutboundInterceptor;
import com.hivemq.extension.sdk.api.interceptor.pingresp.parameter.PingRespOutboundInput;
import com.hivemq.extension.sdk.api.interceptor.pingresp.parameter.PingRespOutboundOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class PingRespInterceptor implements PingRespOutboundInterceptor {
    private static final Logger log = LoggerFactory.getLogger(TopicTreeMain.class);
    TopicTreeMain main = new TopicTreeMain();
    public static Charset charset = StandardCharsets.UTF_8;

    public PingRespInterceptor(TopicTreeMain main) {
        this.main = main;
    }


    @Override
    public void onOutboundPingResp(@NotNull PingRespOutboundInput pingRespOutboundInput, @NotNull PingRespOutboundOutput pingRespOutboundOutput) {
        /*log.info(String.valueOf(pingRespOutboundInput.getClientInformation()));
        String broker_incoming_address = String.valueOf(pingRespOutboundInput.getConnectionInformation().getInetAddress());
        log.info("pingresp da " + broker_incoming_address);
        if()*/


    }
}
