package com.maximyasn.converters;

import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;

@Converter(autoApply = true)
public class NumBoolConverter implements AttributeConverter<Boolean, Integer> {
    @Override
    public Integer convertToDatabaseColumn(Boolean aBoolean) {
        if(aBoolean != null) {
            return aBoolean ? 1 : 0;
        }
        return null;
    }

    @Override
    public Boolean convertToEntityAttribute(Integer integer) {
        if(integer == null || integer != 0 || integer != 1) {
            return null;
        } else {
            return integer == 1;
        }
    }
}
