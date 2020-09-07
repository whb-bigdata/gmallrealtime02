package com.atguigu.gmall.gmallpublisher.util;

import org.apache.commons.lang3.time.DateUtils;
import org.springframework.stereotype.Service;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.SimpleFormatter;
@Service
public class GetDate {
    public String getYd(String td){
        final SimpleDateFormat dateFormat = new SimpleDateFormat("yyy-MM-dd");
        try {
            final Date date = dateFormat.parse(td);
            final Date yDate = DateUtils.addDays(date, -1);
            return dateFormat.format(yDate);
        } catch (ParseException e) {
            e.printStackTrace();
            throw new RuntimeException("格式转换异常");
        }


    }
}
