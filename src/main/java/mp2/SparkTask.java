package mp2;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;

import java.util.List;

public class SparkTask implements IRecordProcessor {
    @Override
    public void initialize(String shardId) {
        
    }

    @Override
    public void processRecords(List<Record> list, IRecordProcessorCheckpointer iRecordProcessorCheckpointer) {

    }

    @Override
    public void shutdown(IRecordProcessorCheckpointer iRecordProcessorCheckpointer, ShutdownReason shutdownReason) {

    }
}
