package com.suyun.common.kafka;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;

import java.io.IOException;
import java.util.Arrays;

/**
 * JsonSerializer
 *
 * Created by IT on 2017/3/31.
 */
public class JsonSerializer {

    private static final ObjectMapper objectMapper;

    static {
        objectMapper = new ObjectMapper();
        objectMapper.configure(MapperFeature.DEFAULT_VIEW_INCLUSION, false);
		objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    /**
     * serialize
     *
     * @param data
     * @param <T>
     * @return
     */
    public static <T> byte[] serialize(T data) {
        try {
			byte[] result = null;
			if (data != null) {
				result = objectMapper.writeValueAsBytes(data);
			}
			return result;
		}
		catch (IOException ex) {
			throw new SerializationException("Can't serialize data [" + data + "]", ex);
		}
    }

    /**
     * deserialize
     *
     * @param data
     * @param valueType
     * @param <T>
     * @return
     */
    public static <T> T deserialize(byte[] data, Class<T> valueType) {
        try {
			T result = null;
			if (data != null) {
				result = objectMapper.readValue(data, valueType);
			}
			return result;
		}
		catch (IOException e) {
			throw new SerializationException("Can't deserialize data [" + Arrays.toString(data) +
					"]", e);
		}
    }

    /**
     * deserialize
     *
     * @param data
     * @param valueType
     * @param <T>
     * @return
     */
    public static <T> T deserialize(byte[] data, TypeReference valueType) {
        try {
			T result = null;
			if (data != null) {
				result = objectMapper.readValue(data, valueType);
			}
			return result;
		}
		catch (IOException e) {
			throw new SerializationException("Can't deserialize data [" + Arrays.toString(data) +
					"]", e);
		}
    }
}
