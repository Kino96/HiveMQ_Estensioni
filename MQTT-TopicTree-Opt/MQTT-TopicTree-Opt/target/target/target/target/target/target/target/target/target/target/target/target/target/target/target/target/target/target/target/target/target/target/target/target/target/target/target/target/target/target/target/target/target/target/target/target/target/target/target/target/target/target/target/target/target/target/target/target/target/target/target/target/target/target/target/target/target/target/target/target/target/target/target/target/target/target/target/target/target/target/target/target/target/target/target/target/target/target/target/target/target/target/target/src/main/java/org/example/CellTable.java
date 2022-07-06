package org.example;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CellTable {
    private static final Logger log = LoggerFactory.getLogger(TopicTreeMain.class);
    private String id;
    private String ip_address;
    private double C;
    private boolean next_hop;
    private boolean prev_hop;
    private double root_count;
    private boolean received;
    private double totRtt;
    private double maxRtt;
;
    public CellTable() {
        this.ip_address = null;
        this.C = -1;
        this.id = null;
        this.next_hop = false;
        this.prev_hop = false;
        this.root_count = -1.0;
        this.received = false;
        this.totRtt = 0;
        this.maxRtt = 0;
    }

    public boolean isNext_hop() {
        return next_hop;
    }

    public void setNext_hop(boolean next_hop) {
        this.next_hop = next_hop;
    }

    public boolean isPrev_hop() {
        return prev_hop;
    }

    public void setPrev_hop(boolean prev_hop) {
        this.prev_hop = prev_hop;
    }

    public double getTotRtt() { return this.totRtt;}
    public void setTotRtt(double rtt) { this.totRtt = rtt;}

    public double getMaxRtt() { return this.maxRtt;}
    public void setMaxRtt(double rtt) { this.maxRtt = rtt;}

    public String getId() {
        return id;
    }

    public void setId(String id){this.id = id; }

    public boolean getReceived() {
        return received;
    }

    public void setReceived(boolean r) {
        this.received = r;
    }

    public double getRootCount() {
        return root_count;
    }

    public void setRootCount(double rootCount) {
        this.root_count = rootCount;
    }

    public void setIp_address(String address){
        this.ip_address = address;
    }

    public String getIp_address(){
        return ip_address;
    }

    public double getC() {
        return C;
    }

    public void setC(double c) {
        C = c;
    }


    public void printCellTable(){
        log.info("ip address: " + ip_address);
        log.info("id: " + this.id);
        log.info("next_hop: " + this.next_hop);
        log.info("prev_hop: " + this.prev_hop);
    }
}
