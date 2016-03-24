/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package oddeye.barlus.servlets;

import java.util.Properties;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import javax.servlet.ServletContext;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

import java.util.Arrays;
import java.util.Collections;

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
    private static String[] users;
    private static Producer<String, String> producer;

//broker.topic = topic2
//
//uid.list = 79f68e1b-ddb3-4065-aec8-bf2eeb9718e8:607984d6-d2ed-4144-936d-5310def8f26e:1719e495-687b-49a1-ac48-75227bcc5ab6
    
    public static boolean Close() {
       producer.close();
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
            BrokerTopic = configProps.getProperty("broker.topic");
            String userlist = configProps.getProperty("uid.list");
            users = userlist.split(":");
            Arrays.sort(users);
            Arrays.sort(users, Collections.reverseOrder());
            Properties props = new Properties();
            props.put("metadata.broker.list", AppConfiguration.getBrokerList());
            props.put("serializer.class", "kafka.serializer.StringEncoder");
            props.put("request.required.acks", "1");
            ProducerConfig config = new ProducerConfig(props);
            producer = new Producer<String, String>(config);
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
}
