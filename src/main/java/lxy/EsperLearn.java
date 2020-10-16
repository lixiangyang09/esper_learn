package lxy;

import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.common.client.metric.RuntimeMetric;
import com.espertech.esper.common.client.metric.StatementMetric;
import com.espertech.esper.common.client.module.Module;
import com.espertech.esper.common.client.module.ModuleItem;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompiler;
import com.espertech.esper.compiler.client.EPCompilerProvider;
import com.espertech.esper.runtime.client.EPDeployment;
import com.espertech.esper.runtime.client.EPRuntimeProvider;
import com.espertech.esper.runtime.client.EPStatement;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lxy.model.MarketData;
import lxy.receiver.MarketDataReceiver;
import lxy.receiver.MetricsReceiver;

import java.util.Random;
import java.util.concurrent.locks.LockSupport;


@Slf4j
public class EsperLearn {
    @SneakyThrows
    public static void main(String[] args) {
        // Compiling EPL
        EPCompiler compiler = EPCompilerProvider.getCompiler();
        // This could also be done in a configuration file.
        Configuration configuration = new Configuration();
        configuration.getCompiler().getByteCode().setAllowSubscriber(true);

        // metric related
        configuration.getCommon().addEventType(RuntimeMetric.class.getSimpleName(), RuntimeMetric.class.getName());
        configuration.getCommon().addEventType(StatementMetric.class.getSimpleName(), StatementMetric.class.getName());
        configuration.getRuntime().getMetricsReporting().setJmxRuntimeMetrics(true);
        configuration.getRuntime().getMetricsReporting().setEnableMetricsReporting(true);

        configuration.getCommon().addEventType(MarketData.class);
        CompilerArguments compilerArguments = new CompilerArguments(configuration);

        Module module = new Module();
        module.getItems().add(new ModuleItem("@name('marketdata-statement') select * from MarketData"));

        // metrics
        module.getItems().add(new ModuleItem("@name('RuntimeMetric-statement')  select * from RuntimeMetric"));
        module.getItems().add(new ModuleItem("@name('StatementMetric-statement')  select * from StatementMetric"));


        // Compile a module
        EPCompiled epCompiled = compiler.compile(module, compilerArguments);

        // Get runtime
        var runtime = EPRuntimeProvider.getDefaultRuntime(configuration);

        // deploy compiled module
        EPDeployment deployment;
        deployment = runtime.getDeploymentService().deploy(epCompiled);
        EPStatement statement = runtime.getDeploymentService().getStatement(deployment.getDeploymentId(), "marketdata-statement");
        statement.setSubscriber(new MarketDataReceiver());
        MetricsReceiver metricsReceiver = new MetricsReceiver();
        EPStatement statementMetrics1 = runtime.getDeploymentService().getStatement(deployment.getDeploymentId(), "RuntimeMetric-statement");
        EPStatement statementMetrics2 = runtime.getDeploymentService().getStatement(deployment.getDeploymentId(), "StatementMetric-statement");
        statementMetrics1.setSubscriber(metricsReceiver);
        statementMetrics2.setSubscriber(metricsReceiver);

        while (true) {
            LockSupport.parkNanos((long) 1e9);
            var random = new Random();
            var msg = MarketData.builder().exDestination(String.format("ex%d", random.nextInt())).build();
            log.info("send event to runtime: {}", msg);
            runtime.getEventService().sendEventBean(msg, msg.getClass().getSimpleName());
        }
    }
}
