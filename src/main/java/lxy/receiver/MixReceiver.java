package lxy.receiver;

import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.common.client.context.ContextPartitionSelectorHash;
import com.espertech.esper.runtime.client.EPDeployment;
import com.espertech.esper.runtime.client.EPRuntime;
import com.espertech.esper.runtime.client.EPStatement;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lxy.model.AggMd;
import lxy.model.FloorQuote;
import lxy.model.RawQuote;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.*;
import java.util.zip.CRC32;

/**
 * @author lixiangyang
 */
@Slf4j
public class MixReceiver {
        final EPRuntime runtime;
        final List<EPStatement> statements;
        final List<String> statementsNames;
        final Random random = new Random();

        final Map<Integer, Long> count = new HashMap<>();

        public MixReceiver(EPRuntime runtime, EPDeployment deployment, List<String> statementsNames) {
            this.runtime = runtime;
            this.statementsNames = statementsNames;
            this.statements = new ArrayList<>(statementsNames.size());
            for (var statementName : statementsNames) {
                EPStatement statement = runtime.getDeploymentService().getStatement(deployment.getDeploymentId(), statementName);
                log.info("lixiangyang debug. statementName: {}, statement: {}", statementName, statement);
                statements.add(statement);
                statement.setSubscriber(this);
            }
        }

        public void update(@NonNull EPStatement statement, @NonNull RawQuote rawQuote) {
            //log.info("lixiangyang debug. update receive message. type: {}, quote: {}", rawQuote.getClass().getSimpleName(), rawQuote);
            //getSnapshot(rawQuote.getUid());
        }

        public void update(@NonNull EPStatement statement, @NonNull FloorQuote floorQuote) {
            //log.info("lixiangyang debug. update receive message. type: {}, quote: {}", floorQuote.getClass().getSimpleName(), floorQuote);
            //getSnapshot(floorQuote.getUid());
        }

        public void update(@NonNull EPStatement statement, @NonNull AggMd aggMd) {
            //log.info("lixiangyang debug. update receive message. type: {}, quote: {}", aggMd.getClass().getSimpleName(), aggMd);
            //getSnapshot(aggMd.getUid());
        }

        void getSnapshot(int uid) {
            //log.info("\n");
            //log.info("lixiangyang debug, getSnapshot begin --------------------------");
    //        ContextPartitionSelectorSegmented partitionSelector = () -> {
    //            Object[] strArr = new Object[]{uid};
    //            var k = new ArrayList<Object[]>(1);
    //            k.add(0, strArr);
    //            return k;
    //        };
            Set<Integer> res = new HashSet<>();
            int hashedRes = stringToCRC32Hash(uid, 10000);
            res.add(hashedRes);
            // log.info("hashed res: {}", res);
            ContextPartitionSelectorHash partitionSelector = () -> res;

            //log.info("lixiangyang getSnapshot input, uid: {}", uid);
            for (var statement : statements) {
                Set<Integer> hashConflict = new HashSet<>();
                var safeIter = statement.iterator(partitionSelector);
                while (safeIter.hasNext()) {
                    //iterator.next()返回迭代的下一个元素
                    EventBean event = safeIter.next();
                    Object obj = event.getUnderlying();
                    if (obj instanceof RawQuote) {
                        hashConflict.add(((RawQuote)obj).getUid());
                    } else if (obj instanceof FloorQuote) {
                        hashConflict.add(((FloorQuote)obj).getUid());
                    } else if (obj instanceof AggMd) {
                        hashConflict.add(((AggMd)obj).getUid());
                    }
                    //log.info("getSnapshot. type: {}, simpleName: {}, value: {}", statement.getEventType().getUnderlyingType(), event.getClass().getSimpleName(), event.getUnderlying());
                }
                count.put(hashConflict.size(), count.getOrDefault(hashConflict.size(), 0L) + 1);
            }
            if (random.nextInt(50000) < 2) {
                log.info("getSnapshot index: {}", uid);
                log.info("statement conflict: {}", count);
            }
            // log.info("lixiangyang debug, getSnapshot end --------------------------\n");
        }

        @SneakyThrows
        public static int stringToCRC32Hash(int uid, int granularity) {
            byte[] bytes;

            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            DataOutputStream ds = new DataOutputStream(buf);
            ds.writeInt(uid);
            bytes = buf.toByteArray();

            CRC32 crc = new CRC32();
            crc.update(bytes);
            long value = crc.getValue() % granularity;

            int result = (int) value;
            if (result >= 0) {
                return result;
            }
            return -result;

        }
}
