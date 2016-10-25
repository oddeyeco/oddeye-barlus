/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package base.oddeye.barlus;

import java.util.Properties;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import javax.servlet.ServletContext;
//import kafka.javaapi.producer.Producer;
//import kafka.producer.ProducerConfig;

import java.util.Arrays;
import java.util.Collections;

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
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author vahan
 */
public class AppConfiguration {

    private static final String sFileName = "config.properties";
    private static final String sDirSeparator = System.getProperty("file.separator");
    private static final Properties configProps = new Properties();
    private static String BrokerList = "localhost:9093,localhost:9094";
    private static String BrokerTopic = "oddeyecoconutdefaulttopic";
    private static String BrokerTSDBTopic = "oddeyecoconutdefaultTSDBtopic";

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

            ArrayList<String> UserList;
            try (Connection connection = ConnectionFactory.createConnection(config)) {
                TableName tableName = TableName.valueOf("oddeyeusers");
                try (Table table = connection.getTable(tableName)) {
                    SingleColumnValueFilter filter = new SingleColumnValueFilter(
                            Bytes.toBytes("technicalinfo"),
                            Bytes.toBytes("active"),
                            CompareFilter.CompareOp.NOT_EQUAL,
                            new BinaryComparator(Bytes.toBytes(Boolean.FALSE)));
                    filter.setFilterIfMissing(false);
                    Scan scan1 = new Scan();
                    scan1.setFilter(filter);
                    try (ResultScanner scanner1 = table.getScanner(scan1)) {
                        UserList = new ArrayList<>();
                        for (Result res : scanner1) {
                            UserList.add(new String(res.getRow()));
                        }
                    }
                }
            }            
            users = UserList.toArray(new String[UserList.size()]);
            Arrays.sort(users);
            Arrays.sort(users, Collections.reverseOrder());
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    public static boolean Initbyfile(ServletContext cntxt) {
        String sFilePath;        
        // initialize log4j here        
        String log4jConfigFile = cntxt.getInitParameter("log4j-config-location");
        String fullPath = cntxt.getRealPath("") + log4jConfigFile;
         
        PropertyConfigurator.configure(fullPath);    

        try {
            ServletContext ctx = cntxt;
            String path;
            String p = ctx.getResource("/").getPath();
            path = p.substring(0, p.lastIndexOf("/"));
            sFilePath = p + sFileName;
            FileInputStream ins = new FileInputStream(sFilePath);
            configProps.load(ins);
            BrokerList = configProps.getProperty("broker.list");
            BrokerTopic = configProps.getProperty("broker.classic.topic");
            BrokerTSDBTopic = configProps.getProperty("broker.tsdb.topic");
            initUsers();

            // Init kafka Produser
            Properties props = new Properties();            
            props.put("bootstrap.servers", AppConfiguration.getBrokerList());
            props.put("acks", "all");
            props.put("retries", 0);
            props.put("batch.size", 16384);
            props.put("linger.ms", 1);
            props.put("buffer.memory", 33554432);
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

            producer = new KafkaProducer<>(props);

//            FileHandler fileTxt = new FileHandler("/tmp/oddeye.log", 1000000, 1);
//            fileTxt.setFormatter(new SimpleFormatter());
//            write.log.addHandler(fileTxt);

        } catch (IOException | SecurityException e) {
//        } catch (Exception e) {
            System.out.println("File not found!");

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
