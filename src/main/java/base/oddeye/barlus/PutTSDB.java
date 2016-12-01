/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package base.oddeye.barlus;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import org.apache.log4j.Logger;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Level;

/**
 *
 * @author vahan
 */
public class PutTSDB extends HttpServlet {

    public static final Logger logger = Logger.getLogger(PutTSDB.class.getName());

    /**
     * Processes requests for both HTTP <code>GET</code> and <code>POST</code>
     * methods.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    protected void processRequest(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {

//        Enumeration<String> headerNames = request.getHeaderNames();
//        PutTSDB.logger.log(Level.ERROR, "************************************************");
//        while (headerNames.hasMoreElements()) {
//            String headerName = headerNames.nextElement();
//            Enumeration<String> headers = request.getHeaders(headerName);
//            while (headers.hasMoreElements()) {
//                String headerValue = headers.nextElement();
//                PutTSDB.logger.log(Level.ERROR, "Headers " + headerName + " - " + headerValue);
//            }
//        }
//        PutTSDB.logger.log(Level.ERROR, "************************************************");

        PutTSDB.logger.log(Level.INFO, "Start servlet TSDB process request:" + request.getSession().getId());
        JsonArray jsonResult;
        JsonParser parser = new JsonParser();
        try {
            String Httpresponse = "";
            String uid = request.getParameter("UUID");
            String msg = "";
            String topic = AppConfiguration.getBrokerTSDBTopic();
//             KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, msg);            
//            Map JsonMap = null;
            if ((uid != null) & (!uid.equals(""))) {

                int idx = Arrays.binarySearch(AppConfiguration.getUsers(), uid, Collections.reverseOrder());

                if (idx > -1) {
                    msg = request.getParameter("data");
                } else {
                    AppConfiguration.initUsers();
                    idx = Arrays.binarySearch(AppConfiguration.getUsers(), uid, Collections.reverseOrder());
                    if (idx > -1) {
                        msg = request.getParameter("data");
                    } else {
//                        Httpresponse = "UUID Not exist User count = " + AppConfiguration.getUsers().length;
                        PutTSDB.logger.log(Level.ERROR, "NOT VALID UUID:" + request.getSession().getId());
                    }
                }
            } else {
                msg = "";
                PutTSDB.logger.log(Level.ERROR, "EMPTY UUID:" + request.getSession().getId());
            }
            PutTSDB.logger.log(Level.INFO, "Check UUID:" + request.getSession().getId());

            if (!msg.equals("")) {
                PutTSDB.logger.log(Level.INFO, "MSG:" + msg + " :" + request.getSession().getId());
                try {
                    jsonResult = (JsonArray) parser.parse(msg);

                    if (jsonResult.size() > 0) {
                        for (int i = 0; i < jsonResult.size(); i++) {
                            JsonElement Metric = jsonResult.get(i);
                            if (Metric.getAsJsonObject().get("tags") != null) {
                                if (Metric.getAsJsonObject().get("tags").getAsJsonObject().get("host") == null) {
                                    PutTSDB.logger.log(Level.WARN, "host not exist in input " + msg);
                                }
                                if (Metric.getAsJsonObject().get("tags").getAsJsonObject().get("type") == null) {
                                    PutTSDB.logger.log(Level.WARN, "type not exist in input " + msg);
                                }
                                if (Metric.getAsJsonObject().get("tags").getAsJsonObject().get("alert_level") == null) {
                                    PutTSDB.logger.log(Level.WARN, "alert_level not exist in input " + msg);
                                }
                                if (Metric.getAsJsonObject().get("tags").getAsJsonObject().get("group") == null) {
                                    PutTSDB.logger.log(Level.WARN, "group not exist in input " + msg);
                                }
                                if (Metric.getAsJsonObject().get("timestamp") == null) {
                                    PutTSDB.logger.log(Level.WARN, "timestamp not exist in input " + msg);
                                }

                                if (Metric.getAsJsonObject().get("metric") == null) {
                                    PutTSDB.logger.log(Level.WARN, "metric not exist in input " + msg);
                                }
                                if (Metric.getAsJsonObject().get("value") == null) {
                                    PutTSDB.logger.log(Level.WARN, "value not exist in input " + msg);
                                }
                                Metric.getAsJsonObject().get("tags").getAsJsonObject().addProperty("UUID", uid);

                            } else {
                                jsonResult.remove(i);
                                PutTSDB.logger.log(Level.ERROR, "tags not exist in input " + msg);
                                i--;
                            }

                        }

                        if (jsonResult.size() > 0) {
//                            final KeyedMessage<String, String> data = new KeyedMessage<>(topic, jsonResult.toString());
                            final ProducerRecord<String, String> data = new ProducerRecord<String, String>(topic, jsonResult.toString());
                            PutTSDB.logger.log(Level.INFO, "Prepred data to send:" + request.getSession().getId());
                            AppConfiguration.getProducer().send(data);
                            PutTSDB.logger.log(Level.INFO, "Prepred data send:" + request.getSession().getId());
                        }
                    } else {
//                        Httpresponse = "Not valid json Array";
                        PutTSDB.logger.log(Level.ERROR, "NOT VALID JSON Array:" + request.getSession().getId());
                    }
                } catch (JsonSyntaxException e) {
//                    Httpresponse = "Not json Array";
                    PutTSDB.logger.log(Level.ERROR, "NOT JSON Array:" + request.getSession().getId());
                }

            }

            response.setContentType(
                    "text/html;charset=UTF-8");
            try (PrintWriter out = response.getWriter()) {
//                out.println(Httpresponse + "\n\r");
                out.println("Send message OK");
            }
        } catch (Exception e) {
            PutTSDB.logger.log(Level.ERROR, "Exception: ", e);
            PutTSDB.logger.log(Level.ERROR, "Exception for request: " + request.getParameterMap().toString());
            PutTSDB.logger.log(Level.ERROR, "Exception for xIp: " + request.getHeader("X-Real-IP"));                        
        }
    }

    // <editor-fold defaultstate="collapsed" desc="HttpServlet methods. Click on the + sign on the left to edit the code.">
    /**
     * Handles the HTTP <code>GET</code> method.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        processRequest(request, response);
    }

    /**
     * Handles the HTTP <code>POST</code> method.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        processRequest(request, response);
    }

    /**
     * Returns a short description of the servlet.
     *
     * @return a String containing servlet description
     */
    @Override
    public String getServletInfo() {
        return "Short description";
    }// </editor-fold>

}
