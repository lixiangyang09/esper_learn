package lxy.model;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

@Builder
@Value
public class AggMd {
    @NonNull
    Long index;
}
