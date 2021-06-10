package com.shz.serializer;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.commons.lang3.SerializationUtils;

import java.io.Serializable;

public class ObjectSerializer<T> implements Serializer<T> {

    @Override
    public byte[] serialize(String topic, T data) {
        return SerializationUtils.serialize((Serializable) data);
    }
}
