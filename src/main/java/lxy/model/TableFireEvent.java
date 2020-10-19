package lxy.model;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

@Builder
@Getter
@ToString
public class TableFireEvent {
    @NonNull
    Long index;
}
