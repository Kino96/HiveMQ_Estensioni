package org.example;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class TopicTable {
    private String topic;
    private List<CellTable> topicTable;
    private String root_address;
    private boolean completed;
    private boolean on_building;
    private int num_brokers;

    public TopicTable(String topic){
        this.topic = topic;
        this.topicTable = new ArrayList<CellTable>();
        this.root_address = null;
        this.completed = false;
        this.on_building = false;
        this.num_brokers = 1;
    }

    public List<CellTable> getTopicTable(){
        return this.topicTable;
    }


    public String getTopic(){
        return this.topic;
    }

    public String getRoot_address(){
        return this.root_address;
    }

    public boolean getCompleted(){
        return this.completed;
    }

    public void setCompleted(boolean c){
         this.completed = c;
    }

    public int getNumBrokers(){
        return this.num_brokers;
    }
    public void setNumBrokers(int n){ this.num_brokers = n; }
    public void incNumBrokers(){
        this.num_brokers++;
    }

    public boolean getOnBuilding(){
        return this.on_building;
    }

    public void setOnBuilding(boolean b){
        this.on_building = b;
    }

    public void addBroker(CellTable cb){
        this.topicTable.add(cb);
    }

    public void removeBroker(String brokerAddr){
        for(int i=0;i<topicTable.size();i++)
            if(topicTable.get(i).getIp_address().equals(brokerAddr)){
                topicTable.remove(i);
                break;
            }
    }

    public CellTable getBrokerCell(String brokerAddress){
        for(CellTable ct: topicTable){
            if(ct.getIp_address().equals(brokerAddress)){
                return  ct;
            }
        }
        return null;
    }


    public void setRoot_address(String address){
        this.root_address= address;
    }

}
