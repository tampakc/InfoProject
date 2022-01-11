package infoproject;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import scala.runtime.StringFormat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class TimeSeriesDeserializer implements DeserializationSchema {
    @Override
    public Object deserialize(byte[] bytes) throws IOException {
        String str = new String(bytes, StandardCharsets.UTF_8);
        String[] parts = str.split(" ");

        String value = parts[0];
        String timestamp = parts[1];

        int val = Integer.valueOf(value);
        long time = Long.valueOf(timestamp);

        return new TimeSeriesData(time, val);
    }

    @Override
    public boolean isEndOfStream(Object o) {
        return false;
    }

    @Override
    public TypeInformation getProducedType() {
        return TypeInformation.of(TimeSeriesData.class);
    }
}
