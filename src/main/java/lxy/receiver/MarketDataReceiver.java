package lxy.receiver;

import com.espertech.esper.runtime.client.EPStatement;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lxy.model.FloorQuote;
import lxy.model.MarketData;


@Slf4j
public class MarketDataReceiver {

    public void update(@NonNull EPStatement statement, @NonNull MarketData marketData) {
        log.info("MarketDataReceiver, update receive message. marketData: {}", marketData);
    }
}
