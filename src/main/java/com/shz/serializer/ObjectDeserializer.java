package com.shz.serializer;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.common.serialization.Deserializer;

public class ObjectDeserializer<T> implements Deserializer<T> {
    @Override
    public T deserialize(String topic, byte[] data) {
        return SerializationUtils.deserialize(data);
    }
}
