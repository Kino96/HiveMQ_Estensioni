package org.example;

import java.nio.ByteBuffer;
import java.util.ArrayList;

public class PubBuffer {
    private String topic;
    private String clientId;
    private ByteBuffer payload;
    private boolean sent;

    public PubBuffer(){
        this.topic = null;
        this.clientId = null;
        this.payload = null;
        this.sent = false;

    }

    public void setClientId(String c){this.clientId = c;}
    public String getClientId(){return this.clientId;}

    public void setPayload(ByteBuffer p){this.payload = p;}
    public ByteBuffer getPayload(){return this.payload;}

    public void setsent(boolean s){this.sent = s;}
    public boolean getSent(){return this.sent;}
}
