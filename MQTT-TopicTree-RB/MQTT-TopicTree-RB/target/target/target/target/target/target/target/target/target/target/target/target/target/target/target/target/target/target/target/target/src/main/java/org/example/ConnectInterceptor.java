package org.example;


import com.hivemq.extension.sdk.api.annotations.NotNull;
import com.hivemq.extension.sdk.api.interceptor.connect.ConnectInboundInterceptor;
import com.hivemq.extension.sdk.api.interceptor.connect.parameter.ConnectInboundInput;
import com.hivemq.extension.sdk.api.interceptor.connect.parameter.ConnectInboundOutput;
import com.hivemq.extension.sdk.api.services.Services;
import com.hivemq.extension.sdk.api.services.builder.Builders;
import com.hivemq.extension.sdk.api.services.builder.PublishBuilder;
import com.hivemq.extension.sdk.api.services.publish.Publish;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class ConnectInterceptor implements ConnectInboundInterceptor {
    private static final Logger log = LoggerFactory.getLogger(TopicTreeMain.class);
    TopicTreeMain main = new TopicTreeMain();
    public static Charset charset = StandardCharsets.UTF_8;


    public ConnectInterceptor(TopicTreeMain main) {
        this.main = main;
    }

    @Override
    public void onConnect(@NotNull ConnectInboundInput connectInboundInput, @NotNull ConnectInboundOutput connectInboundOutput) {
        log.info("connect interceptor working");
        log.info("actual table: " + main.table);
        String brokerId = connectInboundInput.getClientInformation().getClientId();
        log.info("onConnect broker: " + brokerId);
        PublishBuilder publishBuilder = Builders.publish();
        publishBuilder.topic("setup/c_value");
        String my_c = String.valueOf(main.table.get(0).getC());
        ByteBuffer payload = ByteBuffer.wrap(my_c.getBytes(charset));
        publishBuilder.payload(payload);
        Publish publish = publishBuilder.build();
        log.info("publish to that broker...");
        Services.publishService().publishToClient(publish, brokerId);



    }
}
