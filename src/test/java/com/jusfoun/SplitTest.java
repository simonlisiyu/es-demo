package com.jusfoun;

import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by lisiyu on 2016/12/28.
 */
public class SplitTest {
    public static void main(String[] args) throws UnknownHostException {
        String str = "123--22--33";
        TransportAddress[] addresses = new TransportAddress[str.split("--").length];
        for(int i=0; i<str.split("--").length; i++){
            System.out.println("Config.nodeHost="+str+", i="+i);
            addresses[i] = new InetSocketTransportAddress(InetAddress.getByName(str.split("--")[i]), 9300);
        }
        System.out.println(addresses[0].toString());
    }
}
