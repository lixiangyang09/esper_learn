package lxy.receiver;

import com.espertech.esper.common.client.metric.MetricEvent;
import com.espertech.esper.common.client.metric.RuntimeMetric;
import com.espertech.esper.common.client.metric.StatementMetric;
import com.espertech.esper.runtime.client.EPStatement;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MetricsReceiver {

    public void update(@NonNull EPStatement statement, @NonNull RuntimeMetric runtimeMetric) {
        //log.info("lixiangyang debug. update receive message. quote: {}", quote);
        log.info("lixinagyang debug. receive runtimeMetric: {}", runtimeMetric);
        log.info("lixiangyang debug.\n" +
                "runtimeMetric: {}\n" +
                "statement: {}\n" +
                        " uri: {}, \n" +
                "timestamp: {}\n" +
                "inputCount: {} \n" +
                "inputCountDelta: {} \n" +
                "scheduleDepth: {}"
                ,
                runtimeMetric,
                statement,
                runtimeMetric.getRuntimeURI(),
                runtimeMetric.getTimestamp(),
                runtimeMetric.getInputCount(),
                runtimeMetric.getInputCountDelta(),
                runtimeMetric.getScheduleDepth()
        );
    }

    public void update(@NonNull EPStatement statement, @NonNull StatementMetric statementMetric) {
        //log.info("lixiangyang debug. update receive message. quote: {}", quote);
        log.info("lixinagyang debug. receive statementMetric: {}", statementMetric);
        log.info("lixiangyang debug.\n" +
                        " statement: {}, \n" +
                        " statementMetric: {}, \n" +
                        " uri: {}, \n" +
                        "timestamp: {}\n" +
                        "statementName: {}\n" +
                        "cpuTime: {}\n" +
                        "wallTime: {}\n" +
                        "numInput: {}\n" +
                        "numOutputIStreamNumber: {}\n"
                ,
                statement,
                statementMetric,
                statementMetric.getRuntimeURI(),
                statementMetric.getTimestamp(),
                statementMetric.getStatementName(),
                statementMetric.getCpuTime(),
                statementMetric.getWallTime(),
                statementMetric.getNumInput(),
                statementMetric.getNumOutputIStream()

        );

    }

    public void update(@NonNull EPStatement statement, @NonNull MetricEvent metricEvent) {
        //log.info("lixiangyang debug. update receive message. quote: {}", quote);
        log.info("lixinagyang debug. receive metricEvent: {}", metricEvent);
    }
}
