/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package base.oddeye.barlus;

import static base.oddeye.barlus.write.log;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.logging.Level;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import kafka.producer.KeyedMessage;
import kafka.utils.Json;
import scala.Function1;
import scala.Option;
import scala.collection.immutable.Map;
import scala.runtime.AbstractFunction1;
import scala.util.parsing.json.JSON;

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
        Option msgObject = null;
        try {

            String Httpresponse = "";
            String uid = request.getParameter("UUID");
            String msg = uid;
            String topic = AppConfiguration.getBrokerTopic();
            KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, msg);
            msg = "";
            Map JsonMap = null;
            if ((uid != null) & (uid != "")) {
                int idx = Arrays.binarySearch(AppConfiguration.getUsers(), uid, Collections.reverseOrder());
                if (idx > -1) {
                    msg = request.getParameter("data");
                } else {
                    AppConfiguration.initUsers();
                    idx = Arrays.binarySearch(AppConfiguration.getUsers(), uid, Collections.reverseOrder());
                    if (idx > -1) {
                        msg = request.getParameter("data");
                    } else {
                        Httpresponse = "UUID Not exist User count = " + AppConfiguration.getUsers().length;
                    }
                }
            } else {
                msg = "";
                Httpresponse = "UUID is empty";
            }
            if (msg != "") {

                Function1<String, Object> f = new AbstractFunction1<String, Object>() {
                    public Object apply(String s) {
                        try {
                            return Integer.parseInt(s);
                        } catch (NumberFormatException e) {
                            return Double.parseDouble(s);
                        }

                    }
                };

                JSON.globalNumberParser_$eq(f);
                msgObject = JSON.parseFull(msg);
                if (!msgObject.isEmpty()) {
                    Object maps = msgObject.productElement(0);
                    if (maps instanceof Map) {
                        JsonMap = (Map) maps;
                        if (!JsonMap.get("metric").isEmpty() & !JsonMap.get("tags").isEmpty() & !JsonMap.get("timestamp").isEmpty()) {
                            if (JsonMap.get("UUID").productElement(0).equals(uid)) {
//                            msg = msg;
                                topic = AppConfiguration.getBrokerTSDBTopic();
                                data = new KeyedMessage<String, String>(topic, Json.encode(msgObject.productElement(0)));

                                //TODO fral u sax tageri mech lcnel UUID
                                
                                AppConfiguration.getProducer().send(data);
                                Httpresponse = "Data Sended";
                            } else {
                                Httpresponse = "UUID Not valid";
                            }
                        } else {
                            Httpresponse = "JSON Not valid";
                        }
                    } else {
                        Httpresponse = "Data Not Mapped";
                    }
                } else {
                    Httpresponse = "Data Not Json";
                }
            }

            response.setContentType(
                    "text/html;charset=UTF-8");
            try (PrintWriter out = response.getWriter()) {                
                out.println(Httpresponse + "\n\r");
                out.println("Send message " + data);
            }
        } catch (Exception e) {
            log.log(Level.SEVERE, "Exception: ", e);
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
