package lxy.model;

import lombok.Builder;
import lombok.Value;

@Builder
@Value
public class AggMd {
    int uid;
    String configName;
    String time;
}
