/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package base.oddeye.barlus;

import static base.oddeye.barlus.write.log;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.logging.Level;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import kafka.producer.KeyedMessage;

/**
 *
 * @author vahan
 */
public class PutTSDB extends HttpServlet {

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
            try (PrintWriter out = response.getWriter()) {                
                out.println("Send message OK");
            }        
        
//        JsonArray jsonResult = new JsonArray();
//        JsonParser parser = new JsonParser();
//        try {
//            String Httpresponse = "";
//            String uid = request.getParameter("UUID");
//            String msg = uid;
//            String topic = AppConfiguration.getBrokerTSDBTopic();
//            KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, msg);
//            msg = "";
////            Map JsonMap = null;
//            if ((uid != null) & (uid != "")) {
//                int idx = Arrays.binarySearch(AppConfiguration.getUsers(), uid, Collections.reverseOrder());
//                if (idx > -1) {
//                    msg = request.getParameter("data");
//                } else {
//                    AppConfiguration.initUsers();
//                    idx = Arrays.binarySearch(AppConfiguration.getUsers(), uid, Collections.reverseOrder());
//                    if (idx > -1) {
//                        msg = request.getParameter("data");
//                    } else {
//                        Httpresponse = "UUID Not exist User count = " + AppConfiguration.getUsers().length;
//                    }
//                }
//            } else {
//                msg = "";
//                Httpresponse = "UUID is empty";
//            }
//            if (!msg.equals("")) {
//                try {
//                    jsonResult = (JsonArray) parser.parse(msg);
//
//                    if (jsonResult.size() > 0) {
//                        for (int i = 0; i < jsonResult.size(); i++) {
//                            JsonElement Metric = jsonResult.get(i);
//                            if (Metric.getAsJsonObject().get("tags") != null) {
//                                Metric.getAsJsonObject().get("tags").getAsJsonObject().addProperty("UUID", uid);
//                            } else {
//                                jsonResult.remove(i);
//                                i--;
//                            }
//
//                        }
//
//                        if (jsonResult.size() > 0) {
//                            data = new KeyedMessage<String, String>(topic, jsonResult.toString());
//                            AppConfiguration.getProducer().send(data);
//                        }
//                    } else {
//                        Httpresponse = "Not valid json Array";
//                    }
//                } catch (Exception e) {
//                    Httpresponse = "Not json Array";
//                }
//
//            }
//
//            response.setContentType(
//                    "text/html;charset=UTF-8");
//            try (PrintWriter out = response.getWriter()) {
//                out.println(Httpresponse + "\n\r");
//                out.println("Send message " + data);
//            }
//        } catch (Exception e) {
//            log.log(Level.SEVERE, "Exception: ", e);
//        }
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
