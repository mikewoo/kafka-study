package com.study.kafka.partition;

import java.util.Map;
import java.util.Random;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

public class RandomPartitioner implements Partitioner {
    private Random random = new Random();
    @Override
    public void configure(Map<String, ?> configs) {
    }
    
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        int numPartitions = 3;  
        int res = 1;  
        if (value == null) {  
            res = random.nextInt(numPartitions);  
        } else {  
            res = Math.abs(value.hashCode()) % numPartitions;  
        }  
        System.out.println("value is " + value + ", data partitions is " + res);  
        System.out.println(this);
        return res;  
    }
    @Override
    public void close() {
    } 

}
