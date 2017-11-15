/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package base.oddeye.barlus;

import co.oddeye.core.OddeyeHttpURLConnection;
import co.oddeye.core.globalFunctions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.Properties;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import javax.servlet.ServletContext;
//import kafka.javaapi.producer.Producer;
//import kafka.producer.ProducerConfig;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.hbase.async.Config;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;

/**
 *
 * @author vahan
 */
public class AppConfiguration {

    public static final Logger LOGGER = Logger.getLogger(AppConfiguration.class.getName());

    private static final String CONFIG_FILE = "config.properties";
    private static final String DIR_SEPARATOR = System.getProperty("file.separator");
    private static final Properties PROPS_CONFIGS = new Properties();
    private static String BrokerList = "localhost:9093,localhost:9094";
    private static String BrokerSandboxTopic = "sandbox";
    private static String BrokerTSDBTopic = "oddeyecoconutdefaultTSDBtopic";
    private static Cache<String, barlusUser> users = CacheBuilder.newBuilder().concurrencyLevel(4).expireAfterWrite(1, TimeUnit.MINUTES).build();
//    private static ConcurrentHashMap<String, barlusUser> users = new ConcurrentHashMap<>();
    private static Producer<String, String> producer;

//broker.topic = topic2
//
//uid.list = 79f68e1b-ddb3-4065-aec8-bf2eeb9718e8:607984d6-d2ed-4144-936d-5310def8f26e:1719e495-687b-49a1-ac48-75227bcc5ab6
    public static boolean Close() {
        producer.close();
        return true;
    }

    public static barlusUser getUser(String uuid) {

        Config clientconf = new org.hbase.async.Config();
        clientconf.overrideConfig("hbase.zookeeper.quorum", PROPS_CONFIGS.getProperty("hbase.zookeeper.quorum"));
        clientconf.overrideConfig("hbase.rpcs.batch.size", PROPS_CONFIGS.getProperty("hbase.rpcs.batch.size"));

        HBaseClient client = globalFunctions.getClient(clientconf);
        GetRequest getUserRequest = new GetRequest(PROPS_CONFIGS.getProperty("hbase.usertable").getBytes(), uuid.getBytes(), "technicalinfo".getBytes());
        try {
            final ArrayList<KeyValue> userkvs = client.get(getUserRequest).joinUninterruptibly();
            final barlusUser User = new barlusUser(userkvs);
            users.put(User.getId().toString(), User);
            return User;
        } catch (Exception ex) {
            LOGGER.error("ERROR: " + globalFunctions.stackTrace(ex));
        }

        return null;
    }

    public static boolean initUsers() {
        Config clientconf = new org.hbase.async.Config();
        clientconf.overrideConfig("hbase.zookeeper.quorum", PROPS_CONFIGS.getProperty("hbase.zookeeper.quorum"));
        clientconf.overrideConfig("hbase.rpcs.batch.size", PROPS_CONFIGS.getProperty("hbase.rpcs.batch.size"));

        HBaseClient client = globalFunctions.getClient(clientconf);
//        Configuration config = HBaseConfiguration.create();
//        config.clear();
//        config.set("hbase.zookeeper.quorum", PROPS_CONFIGS.getProperty("zookeeper.quorum"));
//        config.set("hbase.zookeeper.property.clientPort", PROPS_CONFIGS.getProperty("zookeeper.clientPort"));
        ArrayList<String> UserList;
//        UserList = new ArrayList<>();
//        try (Connection connection = ConnectionFactory.createConnection(config)) {
//            TableName tableName = TableName.valueOf(PROPS_CONFIGS.getProperty("hbase.usertable"));
//            try (Table table = connection.getTable(tableName)) {
//                SingleColumnValueFilter filter = new SingleColumnValueFilter(
//                        Bytes.toBytes("technicalinfo"),
//                        Bytes.toBytes("active"),
//                        CompareFilter.CompareOp.NOT_EQUAL,
//                        new BinaryComparator(Bytes.toBytes(Boolean.FALSE)));
//                filter.setFilterIfMissing(false);
//                Scan scan1 = new Scan();
//                scan1.setFilter(filter);
//                try (ResultScanner scanner1 = table.getScanner(scan1)) {
//                    for (Result res : scanner1) {
//                        UserList.add(new String(res.getRow()));
//                    }
//                } catch (Exception e) {
//                    LOGGER.error("ERROR In get Scanner " + globalFunctions.stackTrace(e));
//                    return false;
//                }
//            } catch (Exception e) {
//                LOGGER.error("ERROR In get Table " + globalFunctions.stackTrace(e));
//                return false;
//            }
//        } catch (Exception e) {
//            LOGGER.error("ERROR In Create Connection " + globalFunctions.stackTrace(e));
//            return false;
//        } 
//        users = UserList.toArray(new String[UserList.size()]);
//        Arrays.sort(users);
//        Arrays.sort(users, Collections.reverseOrder());
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
            sFilePath = p + CONFIG_FILE;
            FileInputStream ins = new FileInputStream(sFilePath);
            PROPS_CONFIGS.load(ins);
            BrokerList = PROPS_CONFIGS.getProperty("broker.list");
            BrokerSandboxTopic = PROPS_CONFIGS.getProperty("broker.sandbox.topic");
            BrokerTSDBTopic = PROPS_CONFIGS.getProperty("broker.tsdb.topic");
            boolean readusers = initUsers();

            // Init kafka Produser
            if (!readusers) {
                return false;
            }
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

        } catch (IOException | SecurityException e) {
            LOGGER.error("ERROR Connect to kafka " + globalFunctions.stackTrace(e));
            return false;

        }
        return true;

    }

    /**
     * @return the CONFIG_FILE
     */
    public static String getsFileName() {
        return CONFIG_FILE;
    }

    /**
     * @return the DIR_SEPARATOR
     */
    public static String getsDirSeparator() {
        return DIR_SEPARATOR;
    }

    /**
     * @return the PROPS_CONFIGS
     */
    public static Properties getConfigProps() {
        return PROPS_CONFIGS;
    }

    /**
     * @return the BrokerList
     */
    public static String getBrokerList() {
        return BrokerList;
    }

    /**
     * @return the BrokerSandboxTopic
     */
    public static String getBrokerSandboxTopic() {
        return BrokerSandboxTopic;
    }

    /**
     * @return the users
     */
    public static ConcurrentMap<String, barlusUser> getUsers() {
        return users.asMap();
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
