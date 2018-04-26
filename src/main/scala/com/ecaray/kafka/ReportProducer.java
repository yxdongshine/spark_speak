package com.ecaray.kafka;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.javaapi.producer.Producer;
import java.util.Properties;

/**
 * Created by YXD on 2018/4/26.
 */
public class ReportProducer {
    public final String TOPIC_NAME = "report_message";
    //构造私有
    private ReportProducer(){

    }
    private static Producer<String, String> producer = null;
    private static ReportProducer reportProducer = null;
    /**
     * 单利获取生产者
     * @return
     */
    public static ReportProducer getReportProducerInstance(){
        if(null == reportProducer){
            reportProducer = new ReportProducer();
            if(null == producer){
                producer = new Producer(init());
            }
        }
        return  reportProducer;
    }

    /**
     * 初始化kafka发送参数
     */
    public static ProducerConfig init(){
        Properties props = new Properties();
        props.put("metadata.broker.list", "broker1:9092,broker2:9092 ");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");//表示至少一个broker响应
        ProducerConfig config = new ProducerConfig(props);
        return config;
    }


    /**
     * 发送消息
     * @param Key
     * @param message
     */
    public void sendMessage(String Key,String message){
        KeyedMessage<String, String> data = new KeyedMessage<String, String>(TOPIC_NAME, Key, message);
        producer.send(data);
    }



}
