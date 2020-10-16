package lxy.model;

import lombok.Builder;
import lombok.ToString;
import lombok.Value;

@Builder
@ToString
@Value
public class RawQuote {
    int uid;
    String name;
    String time;
}
