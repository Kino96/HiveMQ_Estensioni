package org.example;

public class SubTable {
    private String clientId;
    private String topic;

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void printSubscriber(){
        System.out.println("clientId: " + this.clientId);
        System.out.println("topic: " + this.topic);
    }
}
