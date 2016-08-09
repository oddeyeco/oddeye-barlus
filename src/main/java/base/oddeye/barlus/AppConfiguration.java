/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package base.oddeye.barlus;

import java.util.Properties;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import javax.servlet.ServletContext;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

import java.util.Arrays;
import java.util.Collections;
import java.util.logging.FileHandler;
import java.util.logging.SimpleFormatter;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;

/**
 *
 * @author vahan
 */
public class AppConfiguration {

    private static final String sFileName = "config.properties";
    private static String sDirSeparator = System.getProperty("file.separator");
    private static Properties configProps = new Properties();
    private static String BrokerList = "localhost:9093,localhost:9094";
    private static String BrokerTopic = "oddeyecoconutdefaulttopic";
    private static String BrokerTSDBTopic = "oddeyecoconutdefaultTSDBtopic";
//    private static String ZookeeperQuorum = "localhost";
//    private static String ZookeeperClientPort = "2181";

    private static String[] users;
    private static Producer<String, String> producer;

//broker.topic = topic2
//
//uid.list = 79f68e1b-ddb3-4065-aec8-bf2eeb9718e8:607984d6-d2ed-4144-936d-5310def8f26e:1719e495-687b-49a1-ac48-75227bcc5ab6
    public static boolean Close() {
        producer.close();
        return true;
    }

    public static boolean initUsers() {
        try {

            Configuration config = HBaseConfiguration.create();
            config.clear();
            config.set("hbase.zookeeper.quorum", configProps.getProperty("zookeeper.quorum"));
            config.set("hbase.zookeeper.property.clientPort", configProps.getProperty("zookeeper.clientPort"));

            Connection connection = ConnectionFactory.createConnection(config);
            TableName tableName = TableName.valueOf("oddeyeusers");
            Table table = connection.getTable(tableName);

            SingleColumnValueFilter filter = new SingleColumnValueFilter(
                    Bytes.toBytes("technicalinfo"),
                    Bytes.toBytes("active"),
                    CompareFilter.CompareOp.NOT_EQUAL,
                    new BinaryComparator(Bytes.toBytes(Boolean.FALSE)));
            filter.setFilterIfMissing(false);              
            
            Scan scan1 = new Scan();
            scan1.setFilter(filter);
            ResultScanner scanner1 = table.getScanner(scan1);
            ArrayList<String> UserList = new ArrayList<>();
            for (Result res : scanner1) {
                UserList.add(new String(res.getRow()));
            }
            scanner1.close();
            table.close();
            connection.close();
            String userlist = configProps.getProperty("uid.list");
            users = UserList.toArray(new String[UserList.size()]);
            Arrays.sort(users);
            Arrays.sort(users, Collections.reverseOrder());
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public static boolean Initbyfile(ServletContext cntxt) {
        String sFilePath = "";
        File currentDir = new File(".");

        try {
//            BrokerList = "aaaaaa";
            ServletContext ctx = cntxt;
            String path = null;
            String p = ctx.getResource("/").getPath();
            path = p.substring(0, p.lastIndexOf("/"));
            path = path.substring(path.lastIndexOf("/") + 1);
            sFilePath = p + sFileName;
            FileInputStream ins = new FileInputStream(sFilePath);
            configProps.load(ins);
            BrokerList = configProps.getProperty("broker.list");
            BrokerTopic = configProps.getProperty("broker.classic.topic");
            BrokerTSDBTopic = configProps.getProperty("broker.tsdb.topic");            
//            ZookeeperQuorum = configProps.getProperty("zookeeper.quorum");
//            ZookeeperClientPort = configProps.getProperty("zookeeper.clientPort");

            initUsers();

            // Init kafka Produser
            Properties props = new Properties();
            props.put("metadata.broker.list", AppConfiguration.getBrokerList());
            props.put("serializer.class", "kafka.serializer.StringEncoder");
            props.put("request.required.acks", "1");
            ProducerConfig config = new ProducerConfig(props);
            producer = new Producer<String, String>(config);

            FileHandler fileTxt = new FileHandler("/tmp/oddeye.log", 1000000, 1);
            fileTxt.setFormatter(new SimpleFormatter());
            write.log.addHandler(fileTxt);

        } catch (Exception e) {
//        } catch (Exception e) {
            System.out.println("File not found!");
            e.printStackTrace();

        }
        return true;

    }

    /**
     * @return the sFileName
     */
    public static String getsFileName() {
        return sFileName;
    }

    /**
     * @return the sDirSeparator
     */
    public static String getsDirSeparator() {
        return sDirSeparator;
    }

    /**
     * @return the configProps
     */
    public static Properties getConfigProps() {
        return configProps;
    }

    /**
     * @return the BrokerList
     */
    public static String getBrokerList() {
        return BrokerList;
    }

    /**
     * @return the BrokerTopic
     */
    public static String getBrokerTopic() {
        return BrokerTopic;
    }

    /**
     * @return the users
     */
    public static String[] getUsers() {
        return users;
    }

    /**
     * @param aUsers the users to set
     */
    public static void setUsers(String[] aUsers) {
        users = aUsers;
    }

    /**
     * @return the producer
     */
    public static Producer<String, String> getProducer() {
        return producer;
    }

    /**
     * @return the BrokerTSDBTopic
     */
    public static String getBrokerTSDBTopic() {
        return BrokerTSDBTopic;
    }
}
