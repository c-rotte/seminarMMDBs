import site.ycsb.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.io.File;
import javax.swing.JOptionPane;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class Benchmark extends DB {

    public Benchmark() {

    }

    public void init() throws DBException {
        getProperties();
    }

    @Override
    public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {

        return Status.OK;
    }

    @Override
    public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                       Vector<HashMap<String, ByteIterator>> result) {
        return Status.NOT_IMPLEMENTED;
    }

    @Override
    public Status update(String table, String key, Map<String, ByteIterator> values) {
        return Status.OK;
    }

    @Override
    public Status insert(String table, String key, Map<String, ByteIterator> values) {
        return Status.OK;
    }

    @Override
    public Status delete(String table, String key) {
        return Status.NOT_IMPLEMENTED;
    }

}
