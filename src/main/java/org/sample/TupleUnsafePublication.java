package org.sample;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.task.GeneralTopologyContext;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.MessageId;
import org.apache.storm.tuple.TupleImpl;
import org.openjdk.jcstress.annotations.*;
import org.openjdk.jcstress.infra.results.IntResult1;

import java.util.*;

@JCStressTest
@Description("Tests if unsafe publication is unsafe.")
@Outcome(id = "-1", expect = Expect.ACCEPTABLE, desc = "The object is not yet published")
@Outcome(id = "0", expect = Expect.FORBIDDEN, desc = "No result submitted")
@Outcome(id = "1", expect = Expect.FORBIDDEN, desc = "The object is published, MessageId == null")
@Outcome(id = "2", expect = Expect.FORBIDDEN, desc = "The object is published, MessageId.anchorsToIds == null")
@Outcome(id = "3", expect = Expect.FORBIDDEN, desc = "The object is published, MessageId.anchorsToIds.keySet == null")
@Outcome(id = "4", expect = Expect.ACCEPTABLE, desc = "The object is published, MessageId.anchorsToIds.keySet != null")
@State
public class TupleUnsafePublication {
    static ArrayList<Object> VALUES = new ArrayList<>();
    static String RANDOM_STREAM_ID = "randomStreamId";
    static int TASK_ID = -1;
    static String SYSTEM = "__system";
    static Fields VALUE = new Fields();

    static GeneralTopologyContext CONTEXT = prepareContext();
    TupleImpl o;

    private static GeneralTopologyContext prepareContext() {
        return new GeneralTopologyContext(new StormTopology(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.singletonMap(SYSTEM, Collections.singletonMap(RANDOM_STREAM_ID, VALUE)),
                RANDOM_STREAM_ID);
    }

    @Actor
    public void publish() {
        o = new TupleImpl(CONTEXT, VALUES, TASK_ID, RANDOM_STREAM_ID);
    }

    @Actor
    public void consume(IntResult1 res) {
        TupleImpl lo = o;
        if (lo != null) {
            MessageId messageId = lo.getMessageId();
            if (messageId != null) {
                Map<Long, Long> anchorsToIds = messageId.getAnchorsToIds();
                if (anchorsToIds != null) {
                    Set<Long> longs = anchorsToIds.keySet();
                    if (longs != null) {
                        res.r1 = 4;
                    } else {
                        res.r1 = 3;
                    }
                } else {
                    res.r1 = 2;
                }
            } else {
                res.r1 = 1;
            }
        } else {
            res.r1 = -1;
        }
    }

}
