package com.atguigu.gmall.logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.common.constant.GmallConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
public class LoggerController {

    @Autowired
    KafkaTemplate<String,String> KafkaTemplate;

    @PostMapping("log")
    public String dolog(String logString) {
        //补充时间戳
        JSONObject jsonObject = JSON.parseObject(logString);
        jsonObject.put("ts",System.currentTimeMillis());

        //写日志（用于离线采集）
        String logJson = jsonObject.toJSONString();
        log.info(logJson);

        //发送到Kafka
        if ("startup".equals(jsonObject.getString("type"))) {
            KafkaTemplate.send(GmallConstants.KAFKA_TOPIC_STARTUP,logJson);
        } else {
            KafkaTemplate.send(GmallConstants.KAFKA_TOPIC_EVENT,logJson);
        }
        return "success";
    }

}
