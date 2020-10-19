package lxy.model;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

@Builder
@ToString
@Getter
public class MarketData {
    @NonNull
    String exDestination;
    @NonNull
    Integer amount;
    @NonNull
    Long index;
    @NonNull
    Long timestamp;
    @NonNull
    Long groupIndex;
}
