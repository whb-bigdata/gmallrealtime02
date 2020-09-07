package com.atguigu.gmall.gmallpublisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall.gmallpublisher.service.DauService;
import com.atguigu.gmall.gmallpublisher.util.GetDate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
public class ChartController {
    @Autowired
    DauService dauService;
    @Autowired
    GetDate getDate;

    @RequestMapping("realtime-total")
    public String getRealtimeTotal(@RequestParam("date") String dt) {
        List<Map<String, Object>> rsList = new ArrayList<>();
        HashMap dauMap = new HashMap();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");

        final Long dauTotal = dauService.getFauTotal(dt);
        dauMap.put("value", dauTotal);
        rsList.add(dauMap);

        HashMap newMap = new HashMap();
        newMap.put("id", "new_mid");
        newMap.put("name", "新增设备");

        //final Long dauTotal = dauService.getFauTotal(dt);
        newMap.put("value", 2);
        rsList.add(newMap);




        return JSON.toJSONString(rsList);
    }

    @RequestMapping("realtime-hour")
    public String getRealtimeHour(@RequestParam("id") String id, @RequestParam("date") String date) {
        Map<String,Map<String,Long>> rsMap = new HashMap<>();

        Map<String,Long> tdMap=dauService.getDauHour(date);
        rsMap.put("today",tdMap);
        Map<String,Long> ydMap=dauService.getDauHour(getDate.getYd(date));
        rsMap.put("yesterday",ydMap);
        return JSON.toJSONString(rsMap);


    }
}
