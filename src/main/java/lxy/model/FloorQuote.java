package lxy.model;

import lombok.Builder;
import lombok.ToString;
import lombok.Value;

@Builder
@ToString
@Value
public class FloorQuote {
    int uid;
    String name;
    String time;
}
