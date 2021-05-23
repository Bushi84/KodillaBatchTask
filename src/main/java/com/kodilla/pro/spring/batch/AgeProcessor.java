package com.kodilla.pro.spring.batch;

import com.kodilla.pro.spring.batch.dto.InData;
import com.kodilla.pro.spring.batch.dto.OutData;
import org.springframework.batch.item.ItemProcessor;

import javax.annotation.Resource;

public class AgeProcessor implements ItemProcessor<InData, OutData> {

    @Resource
    private CalculateAge calculateAge;

    @Override
    public OutData process(InData item) throws Exception {
        return new OutData(item.getName(), item.getSurname(), calculateAge.calculateAgeFromDate(item.getBirthDate()));
    }
}
