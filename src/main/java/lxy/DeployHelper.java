package lxy;

import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.common.client.module.Module;
import com.espertech.esper.common.client.module.ModuleItem;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompiler;
import com.espertech.esper.compiler.client.EPCompilerProvider;
import com.espertech.esper.runtime.client.EPDeployment;
import com.espertech.esper.runtime.client.EPRuntime;
import com.espertech.esper.runtime.client.EPStatement;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lxy.receiver.MarketDataReceiver;

import java.util.Arrays;
import java.util.List;

@Slf4j
public class DeployHelper {
    @NonNull
    final EPRuntime runtime;
    @NonNull
    final EPCompiler compiler;
    @NonNull
    final CompilerArguments compilerArguments;
    public DeployHelper(EPRuntime runtime, CompilerArguments compilerArguments) {
        compiler = EPCompilerProvider.getCompiler();
        this.runtime = runtime;
        this.compilerArguments = compilerArguments;
    }

    @SneakyThrows
    private EPDeployment  deploy(Module module) {
        // Compile a module
        EPCompiled epCompiled = compiler.compile(module, compilerArguments);
        return runtime.getDeploymentService().deploy(epCompiled);
    }

    private void deployEpls(List<String> statementNames, List<String> epls, Object subscriber) {
        Module module = new Module();
        for (var epl : epls) {
            module.getItems().add(new ModuleItem(epl));
        }
        EPDeployment deployment = deploy(module);

        for (var statementName : statementNames) {
            EPStatement statement1 = runtime.getDeploymentService().getStatement(deployment.getDeploymentId(), statementName);
            statement1.setSubscriber(subscriber);
        }
    }

    public void demo(DemoType demoType) {
        switch (demoType) {
            case SELECT:
                // select
                deployEpls(
                        Arrays.asList("select"),
                        Arrays.asList("@name('select') select * from MarketData"),
                        new MarketDataReceiver());
                break;
            case AGGREGATION:
                deployEpls(
                        Arrays.asList("agg"),
                        Arrays.asList("@name('agg') select count(*), amount from MarketData"),
                        new MarketDataReceiver());
                break;
            case FILTER:
                deployEpls(
                        Arrays.asList("filter"),
                        Arrays.asList("@name('filter') select * from MarketData(amount > 50)"),
                        new MarketDataReceiver());
                break;
            case FILTER_AND_AGGREGATION:
                deployEpls(
                        Arrays.asList("filter_and_agg"),
                        Arrays.asList("@name('filter_and_agg') select count(*), sum(amount) from MarketData(amount >= 50)"),
                        new MarketDataReceiver());
                break;
            case DATA_WINDOW:
                deployEpls(
                        Arrays.asList("data_window"),
                        Arrays.asList("@name('data_window')  select * from MarketData#length(5)"),
                        new MarketDataReceiver());
                break;
            case DATA_WINDOW_AND_AGGREGATION:
                deployEpls(
                        Arrays.asList("data_window_and_agg"),
                        Arrays.asList("@name('data_window_and_agg')   select count(*), sum(amount) from MarketData#length(5)"),
                        new MarketDataReceiver());
                break;
            case FILTER_DATA_WINDOW_AND_AGGREGATION:
                deployEpls(
                        Arrays.asList("filter_data_window_and_agg"),
                        Arrays.asList("@name('filter_data_window_and_agg')    select count(*), sum(amount) from MarketData(amount>=200)#length(5)"),
                        new MarketDataReceiver());
                break;
            case WHERE:
                deployEpls(
                        Arrays.asList("where"),
                        Arrays.asList("@name('where')   select * from MarketData as md, select * from AggMd as agg where md.index == agg.index"),
                        new MarketDataReceiver());
                break;
            case TIME_WINDOW_AGGREGATION:
                deployEpls(
                        Arrays.asList("time_window_and_agg"),
                        Arrays.asList("@name('time_window_and_agg')    select count(*), sum(amount) as total from MarketData#time(2)"),
                        new MarketDataReceiver());
                break;
            case PARTITIONED_STATEMENT:
                deployEpls(
                        Arrays.asList("partitioned_statement"),
                        Arrays.asList("create context Batch4Seconds start @now end after 4 sec",
                                "@name('partitioned_statement')  context Batch4Seconds  select count(*), sum(amount) as total from MarketData"),
                        new MarketDataReceiver());
                break;
            case OUTPUT_RATE_LIMIT:
                deployEpls(
                        Arrays.asList("output_rate_limit"),
                        Arrays.asList("@name('output_rate_limit')   select count(*), total(amount) from MarketData output last every 4 seconds"),
                        new MarketDataReceiver());
                break;
            case PARTITIONED_OUTPUT_RATE_LIMIT:
                deployEpls(
                        Arrays.asList("partitioned_output_rate_limit"),
                        Arrays.asList("create context Batch4Seconds start @now end after 4 sec",
                                "@name('partitioned_output_rate_limit') context Batch4Seconds  select count(*), total(amount) from MarketData output last when terminated"),
                        new MarketDataReceiver());
                break;
            case NAMED_WINDOW:
                deployEpls(
                        Arrays.asList("named_window"),
                        Arrays.asList("create window MarketDataWindow#time(10) as MarketData",
                                "on MarketData merge MarketDataWindow insert select *",
                                "@name('named_window')  select avg(amount) as avgAmount from MarketDataWindow"),
                        new MarketDataReceiver());
                break;
            case TABLE:
                deployEpls(
                        Arrays.asList("table"),
                        Arrays.asList("create table MarketAvgAmount(groupIndex long primary key, avgAmount avg(int), myWindow window(*) @type(MarketData))",
                                "into table MarketAvgAmount select avg(amount) as avgAmount, window(*) as myWindow from MarketData#time(10) group by groupIndex",
                                "@name('table')  select MarketAvgAmount[index] from TableFireEvent"),
                        new MarketDataReceiver());
                break;
            case AGG_TYPE_UNAGG_UNGRP:
                // MarketDataWindow内的每个事件，都会触发一次
                deployEpls(
                        Arrays.asList("AGG_TYPE_UNAGG_UNGRP"),
                        Arrays.asList("create window MarketDataWindow#time(10) as MarketData",
                                "on MarketData merge MarketDataWindow insert select *",
                                "@name('AGG_TYPE_UNAGG_UNGRP')  select * from TableFireEvent unidirectional, MarketDataWindow"),
                        new MarketDataReceiver());
                break;
            case AGG_TYPE_FULL_AGG_UNGRP:
                // 只会输出一个值，sum(amount)
                deployEpls(
                        Arrays.asList("AGG_TYPE_FULL_AGG_UNGRP"),
                        Arrays.asList("create window MarketDataWindow#time(10) as MarketData",
                                "on MarketData merge MarketDataWindow insert select *",
                                "@name('AGG_TYPE_FULL_AGG_UNGRP')  select sum(amount) from TableFireEvent unidirectional, MarketDataWindow"),
                        new MarketDataReceiver());
                break;
            case AGG_TYPE_AGG_UNGRP:
                // MarketDataWindow内的每个事件，都会触发一次
                deployEpls(
                        Arrays.asList("AGG_TYPE_FULL_AGG_UNGRP"),
                        Arrays.asList("create window MarketDataWindow#time(10) as MarketData",
                                "on MarketData merge MarketDataWindow insert select *",
                                "@name('AGG_TYPE_FULL_AGG_UNGRP')  select exDestination, sum(amount) from TableFireEvent unidirectional, MarketDataWindow"),
                        new MarketDataReceiver());
                break;
            case AGG_TYPE_FULL_AGG_GRP:
                // unique group，都会触发一次
                deployEpls(
                        Arrays.asList("AGG_TYPE_FULL_AGG_GRP"),
                        Arrays.asList("create window MarketDataWindow#time(10) as MarketData",
                                "on MarketData merge MarketDataWindow insert select *",
                                "@name('AGG_TYPE_FULL_AGG_GRP')  select exDestination, sum(amount) from TableFireEvent unidirectional, MarketDataWindow group by exDestination"),
                        new MarketDataReceiver());
                break;
            case AGG_TYPE_AGG_GRP:
                // MarketDataWindow内的每个事件，都会触发一次
                deployEpls(
                        Arrays.asList("AGG_TYPE_AGG_GRP"),
                        Arrays.asList("create window MarketDataWindow#time(10) as MarketData",
                                "on MarketData merge MarketDataWindow insert select *",
                                "@name('AGG_TYPE_AGG_GRP')  select exDestination, sum(amount), timestamp from TableFireEvent unidirectional, MarketDataWindow group by exDestination"),
                        new MarketDataReceiver());
                break;
            case Match_Recognize_Patterns:
                deployEpls(
                        Arrays.asList("Match_Recognize_Patterns"),
                        Arrays.asList("create window MarketDataWindow#length(30) as MarketData",
                                "on MarketData merge MarketDataWindow insert select *",
                                "@name('Match_Recognize_Patterns')  select * from MarketDataWindow\n" +
                                        "match_recognize ( \n" +
                                            "partition by groupIndex\n" +
                                            "measures a.groupIndex as partitioned, a.amount as m1, b.amount as m2\n" +
                                            "pattern (a b)\n" +
                                                "define\n" +
                                                    "b as Math.abs(b.amount - a.amount) >= 10\n" +
                                        ")"),
                        new MarketDataReceiver());
                break;
            case EPL_PATTERN:
                deployEpls(
//                        Arrays.asList("epl_pattern1""),
//                        Arrays.asList("@name('epl_pattern1') select * from pattern [every a=MarketData or b=AggMd]"),

//                        Arrays.asList("epl_pattern2"),
//                        Arrays.asList("@name('epl_pattern1') select * from pattern [every a=MarketData -> AggMd]"),

                        Arrays.asList("epl_pattern3"),
                        Arrays.asList("@name('epl_pattern3') select * from pattern [every a=MarketData -> b=TableFireEvent(a.index=b.index) where timer:within(2 seconds)]"),
                        new MarketDataReceiver());
                break;
            default:
                log.error("Unsupported demoType: {}", demoType);
        }
    }

}
