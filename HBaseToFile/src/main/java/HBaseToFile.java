import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by 2016st19 on 11/8/16.
 */
public class HBaseToFile {

    static Configuration conf = null;
    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "localhost");
    }

    public static void main(String[] args) throws IOException {
        java.io.File outfile = new java.io.File("./HBaseOut.txt");
        java.io.PrintWriter output = new java.io.PrintWriter(outfile);
        Scan scan = new Scan();
        ResultScanner rs = null;
        HTable table = new HTable(conf, Bytes.toBytes("Wuxia"));
        try {
            rs = table.getScanner(scan);
            for (Result r : rs) {
                for (KeyValue kv : r.list()) {
                    output.printf("%s\t%s\r\n", Bytes.toString(kv.getRow()), Bytes.toString(kv.getValue()));
		            output.flush();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            rs.close();
        }
        output.close();
    }
}
