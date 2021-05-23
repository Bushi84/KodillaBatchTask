package com.kodilla.pro.spring.batch;

import javax.annotation.Resource;
import java.time.LocalDate;

public class CalculateAge {

    @Resource
    private LocalDate currentLocalDate;

    public int calculateAgeFromDate(LocalDate birthDate) {
        int age;

        if (birthDate == null || birthDate.isAfter(currentLocalDate)) {
            return -1;
        }else {

            age = currentLocalDate.getYear() - birthDate.getYear();
            if (currentLocalDate.getDayOfYear()<birthDate.getDayOfYear()) {
                age--;
            }
            return age;
        }
    }
}
