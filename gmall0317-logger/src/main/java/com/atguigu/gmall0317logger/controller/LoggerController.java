package com.atguigu.gmall0317logger.controller;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;


@Slf4j
@RestController
public class LoggerController {
    //不要在这写临时值,会产生内存泄漏

    @RequestMapping("/hello")
    public String sayHello(@RequestParam("name") String name) {
        System.out.println("你好");
        return "你好" + name;
    }
    //对某个接口或者类,进行自动装配,单例模式,不仅仅是简单的new
    @Autowired
    KafkaTemplate kafkaTemplate;


    @RequestMapping("/applog")
    public String applog(@RequestBody JSONObject jsonObject) {
        String logJson = jsonObject.toJSONString();
        log.info(logJson);
        if (jsonObject.getString("start") != null) {
            kafkaTemplate.send("GMALL_START", logJson);
        } else {
            kafkaTemplate.send("GMALL_EVENT", logJson);
        }

        return "success";
    }
}


