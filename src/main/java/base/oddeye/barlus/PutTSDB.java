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
    public static final Logger loggerTest = Logger.getLogger("test");

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
            int code = HttpServletResponse.SC_OK;
            String Httpresponse = "OK";
            String uid = request.getParameter("UUID");
            uid = uid.trim();
            String checkerrors = "";
            boolean sandbox = request.getParameter("sandbox") != null;
            String msg = "";
            String topic;
            if (sandbox) {
                topic = AppConfiguration.getBrokerSandboxTopic();
            } else {
                topic = AppConfiguration.getBrokerTSDBTopic();
            }

            if (loggerTest.isDebugEnabled()) {
                loggerTest.debug("uid =" + uid);
            }
            if ((uid != null) & (!uid.equals(""))) {

                int idx = Arrays.binarySearch(AppConfiguration.getUsers(), uid, Collections.reverseOrder());

                if (loggerTest.isDebugEnabled()) {
                    loggerTest.debug("msg =" + request.getParameter("data"));
                }
                if (idx > -1) {
                    msg = request.getParameter("data");
                } else {
                    AppConfiguration.initUsers();
                    idx = Arrays.binarySearch(AppConfiguration.getUsers(), uid, Collections.reverseOrder());
                    if (idx > -1) {
                        msg = request.getParameter("data");
                    } else {
                        code = 424;
                        Httpresponse = "\"FAILURE\",messge:\"NOT VALID UUID\"";
                        PutTSDB.logger.log(Level.ERROR, "NOT VALID UUID:" +uid +"IP:"+request.getHeader("X-Real-IP")+ " data:"+ request.getParameter("data"));
                    }
                }
            } else {
                code = HttpServletResponse.SC_NOT_ACCEPTABLE;
                Httpresponse = "\"FAILURE\",messge:\"EMPTY UUID\"";
                msg = "";
                Httpresponse = "FAILURE";
                PutTSDB.logger.log(Level.ERROR, "EMPTY UUID:" + request.getSession().getId());
            }
            PutTSDB.logger.log(Level.INFO, "Check UUID:" + request.getSession().getId());
            if (code == HttpServletResponse.SC_OK) {
                if (!msg.equals("")) {
                    PutTSDB.logger.log(Level.INFO, "MSG:" + msg + " :" + request.getSession().getId());
                    try {
                        jsonResult = (JsonArray) parser.parse(msg);

                        if (jsonResult.size() > 0) {
                            checkerrors = "";
                            for (int i = 0; i < jsonResult.size(); i++) {
                                JsonElement Metric = jsonResult.get(i);
                                if (Metric.getAsJsonObject().get("tags") != null) {
                                    if (!Metric.getAsJsonObject().get("tags").isJsonPrimitive()) {
                                        if (Metric.getAsJsonObject().get("tags").getAsJsonObject().get("host") == null) {
                                            PutTSDB.logger.log(Level.INFO, "host not exist in input " + msg);
                                        }
                                        if (Metric.getAsJsonObject().get("tags").getAsJsonObject().get("type") == null) {
                                            PutTSDB.logger.log(Level.INFO, "type not exist in input " + msg);
                                        }
                                        if (Metric.getAsJsonObject().get("tags").getAsJsonObject().get("group") == null) {
                                            PutTSDB.logger.log(Level.INFO, "group not exist in input " + msg);
                                        }
                                        if (Metric.getAsJsonObject().get("timestamp") == null) {
                                            PutTSDB.logger.log(Level.ERROR, "timestamp not exist in input " + msg);
                                            jsonResult.remove(i);
                                            checkerrors = checkerrors + "{\"message\":\"timestamp not exist in input\"},";
                                            i--;
                                            continue;

                                        }

                                        if (Metric.getAsJsonObject().get("metric") == null) {
                                            PutTSDB.logger.log(Level.INFO, "metric not exist in input " + msg);
                                            jsonResult.remove(i);
                                            checkerrors = checkerrors + "{\"message\":\"metric not exist in input\"},";
                                            i--;
                                            continue;
                                        }
                                        if (Metric.getAsJsonObject().get("value") == null) {
                                            PutTSDB.logger.log(Level.INFO, "value not exist in input " + msg);
                                            jsonResult.remove(i);
                                            checkerrors = checkerrors + "{\"message\":\"value not exist in input\"},";
                                            i--;
                                        }
                                        Metric.getAsJsonObject().get("tags").getAsJsonObject().addProperty("UUID", uid);
                                    } else {
                                        jsonResult.remove(i);
                                        checkerrors = checkerrors + "{\"message\":\"tags not json in input\"},";
                                        PutTSDB.logger.log(Level.ERROR, "tags not json in input " + msg);
                                        i--;
                                    }
                                } else {
                                    checkerrors = checkerrors + "{\"message\":\"tags not exist in input\"},";
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
                                if (loggerTest.isDebugEnabled()) {
                                    loggerTest.debug("Send data =" + jsonResult.toString());
                                }
                                PutTSDB.logger.log(Level.INFO, "Prepred data send:" + msg);
                            } else {
//                        Httpresponse = "Not valid json Array";
                                code = 411;
                                Httpresponse = "\"FAILURE\",\"message\":\"Not valid data\"";
                                PutTSDB.logger.log(Level.ERROR, "NOT VALID JSON Array Remove by barlus:"  +"IP:"+request.getHeader("X-Real-IP")+" data:"+ msg);
                            }

                        } else {
//                        Httpresponse = "Not valid json Array";
                            code = 411;
                            Httpresponse = "\"FAILURE\",\"message\":\"NOT VALID JSON Array\"";
                            PutTSDB.logger.log(Level.ERROR, "NOT VALID JSON Empty array:"  +"IP:"+request.getHeader("X-Real-IP")+ " data:"+ msg);
                        }
                    } catch (Exception e) {
//                    Httpresponse = "Not json Array";                        
                        code = 415;
                        Httpresponse = "\"FAILURE\",\"message\":\"" + e.getMessage() + " \"";
                        PutTSDB.logger.log(Level.ERROR, "NOT JSON Array:" + msg);
                    }

                } else {
                    code = 424;
                    Httpresponse = "\"FAILURE\",\"message\":\"Empty Data\"";
                }
            }
            response.setContentType(
                    "json;charset=UTF-8");
            try (PrintWriter out = response.getWriter()) {
                response.setStatus(code);
                if (!checkerrors.isEmpty()) {
                    checkerrors = ", \"advansed\":[" + checkerrors.substring(0, checkerrors.length() - 1) + "]";
                }
                out.println(" {\"state\":" + Httpresponse + checkerrors + "}");
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
