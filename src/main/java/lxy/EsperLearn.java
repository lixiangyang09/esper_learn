package lxy;

import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.common.client.metric.RuntimeMetric;
import com.espertech.esper.common.client.metric.StatementMetric;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.runtime.client.EPRuntime;
import com.espertech.esper.runtime.client.EPRuntimeProvider;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lxy.model.AggMd;
import lxy.model.MarketData;
import lxy.model.TableFireEvent;

import java.util.Random;
import java.util.concurrent.locks.LockSupport;


@Slf4j
public class EsperLearn {
    @SneakyThrows
    public static void main(String[] args) {
        // Compiling EPL
        // This could also be done in a configuration file.
        Configuration configuration = new Configuration();
        configuration.getCompiler().getByteCode().setAllowSubscriber(true);

        // metric related
        configuration.getCommon().addEventType(RuntimeMetric.class.getSimpleName(), RuntimeMetric.class.getName());
        configuration.getCommon().addEventType(StatementMetric.class.getSimpleName(), StatementMetric.class.getName());
        configuration.getRuntime().getMetricsReporting().setJmxRuntimeMetrics(true);
        configuration.getRuntime().getMetricsReporting().setEnableMetricsReporting(true);

        configuration.getCommon().addEventType(MarketData.class);
        configuration.getCommon().addEventType(AggMd.class);
        configuration.getCommon().addEventType(TableFireEvent.class);


        EPRuntime runtime = EPRuntimeProvider.getDefaultRuntime(configuration);
        DeployHelper deployHelper = new DeployHelper(
                runtime,
                new CompilerArguments(configuration));

        deployHelper.demo(DemoType.EPL_PATTERN);

        long index = 0;
        while (true) {
            LockSupport.parkNanos((long) 1e9);
            var random = new Random();
            var msg = MarketData.builder().exDestination(String.format("ex%d", random.nextInt(10))).amount(random.nextInt(100)).timestamp(System.currentTimeMillis() / 1000).groupIndex(index % 5).index(index).build();
            log.info("send event to runtime: {}", msg);
            runtime.getEventService().sendEventBean(msg, msg.getClass().getSimpleName());

            var tableFireEvent = TableFireEvent.builder().index(index % 5).build();
            log.info("send event to runtime: {}", tableFireEvent);
            runtime.getEventService().sendEventBean(tableFireEvent, tableFireEvent.getClass().getSimpleName());

            index++;
            if (index % 10 == 0) {
                var aggMd = AggMd.builder().index(index).build();
                log.info("send event to runtime: {}", aggMd);
                runtime.getEventService().sendEventBean(aggMd, aggMd.getClass().getSimpleName());
            }
        }
    }

}
