package infoproject;



import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class TimeSeriesDeserializer implements DeserializationSchema<Tuple2<Long, Integer>> {
    @Override
    public Tuple2<Long, Integer> deserialize(byte[] bytes) throws IOException {
        String str = new String(bytes, StandardCharsets.UTF_8);
        String[] parts = str.split(" ");

        String value = parts[0];
        String timestamp = parts[1];

        int val = Integer.parseInt(value);
        long time = Long.parseLong(timestamp);

        return new Tuple2<>(time, val);
    }

    @Override
    public boolean isEndOfStream(Tuple2<Long, Integer> o) {
        return false;
    }

    @Override
    public TypeInformation<Tuple2<Long, Integer>> getProducedType() {
        return TypeInformation.of(new TypeHint<Tuple2<Long, Integer>>(){});
    }
}
