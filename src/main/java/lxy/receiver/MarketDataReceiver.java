package lxy.receiver;

import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.runtime.client.EPStatement;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lxy.model.AggMd;
import lxy.model.FloorQuote;
import lxy.model.MarketData;
import lxy.model.TableFireEvent;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


@Slf4j
public class MarketDataReceiver {

    public void update(@NonNull EPStatement statement, @NonNull MarketData[] marketDatas) {
        log.info("MarketDataReceiver, update receive message from {}. marketDatas: {}", statement, marketDatas);

    }

    public void update(@NonNull EPStatement statement, @NonNull MarketData marketData) {
        log.info("MarketDataReceiver, update receive message from {}. marketData: {}", statement, marketData);

        var iterator = statement.iterator();
        int count = 0;
        while (iterator.hasNext()) {
            count++;
            EventBean eventBean = iterator.next();
            log.info("Event in statement, {}", eventBean.getUnderlying());
        }
        log.info("Event count in statement: {}", count);
    }

    public void update(@NonNull EPStatement statement, Long count, Integer amount) {
        log.info("MarketDataReceiver, update receive message from {}. count: {}, amount: {}", statement, count, amount);
        var iterator = statement.iterator();
        int eventCount = 0;
        while (iterator.hasNext()) {
            eventCount++;
            EventBean eventBean = iterator.next();
            log.info("Event in statement, {}", eventBean.getUnderlying());
        }
        log.info("Event count in statement: {}", eventCount);
    }

    public void update(@NonNull EPStatement statement, @NonNull MarketData marketData, @NonNull AggMd aggMd) {
        log.info("MarketDataReceiver, update receive message from {}.\n marketData: {}, \naggMd: {}", statement, marketData, aggMd);

    }

    public void update(@NonNull EPStatement statement, @NonNull Double avgAmount) {
        log.info("MarketDataReceiver, update receive message from {}.\n avgAmount: {}", statement, avgAmount);

    }

    public void update(@NonNull EPStatement statement, @NonNull Map res) {
        // var dataWindow = (MarketData[]) res.get("myWindow");
        log.info("MarketDataReceiver, update receive message from {}.\n res: {}", statement, res);
        for (var tmp : (MarketData[])res.get("myWindow")) {
            log.info("myWindow event: {}", tmp);
        }
    }

    public void update(@NonNull EPStatement statement, @NonNull TableFireEvent fireEvent, @NonNull MarketData marketData) {
        log.info("MarketDataReceiver, update receive message from {}.\n fireEvent: {}\n, marketData: {}", statement, fireEvent, marketData);
    }
}
