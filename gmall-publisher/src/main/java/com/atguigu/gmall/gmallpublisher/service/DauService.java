package com.atguigu.gmall.gmallpublisher.service;

import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public interface DauService {
    public Long getFauTotal(String date);

    public Map<String, Long> getDauHour(String date);
}
