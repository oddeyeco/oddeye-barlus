/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package base.oddeye.barlus;

import java.io.IOException;
import java.io.PrintWriter;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Arrays;
import java.util.Collections;
import scala.util.parsing.json.JSON;

import scala.Option;
import scala.collection.immutable.Map;

import kafka.utils.Json;

import java.util.logging.Logger;
import java.util.logging.Level;
import org.apache.kafka.clients.producer.ProducerRecord;
import scala.Function1;
import scala.runtime.AbstractFunction1;

/**
 *
 * @author vahan
 */
public class write extends HttpServlet {

    public static final Logger log = Logger.getLogger(write.class.getName());
    /**ifif
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
        Option msgObject;
        try {

            String Httpresponse = "";
            String uid = request.getParameter("UUID");
            String msg;
            String topic;

            msg = "";
            Map JsonMap;
//            if ((uid != null)&(!"".equals(uid))) {
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
            if (!"".equals(msg)) {

                Function1<String, Object> f = new AbstractFunction1<String, Object>() {
                    @Override
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
                        if (!JsonMap.get("UUID").isEmpty() & !JsonMap.get("tags").isEmpty() & !JsonMap.get("data").isEmpty()) {
                            if (JsonMap.get("UUID").productElement(0).equals(uid)) {
//                            msg = msg;
                                topic = AppConfiguration.getBrokerSandboxTopic();                                
                                final ProducerRecord<String, String> data = new ProducerRecord<>(topic, Json.encode(msgObject.productElement(0)));

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
                /* TODO output your page here. You may use following sample code. */
                out.println(Httpresponse + "\n\r");
//                out.println("Send message " + data);

//                out.println("<!DOCTYPE html>");
//                out.println("<html>");
//                out.println("<head>");
//                out.println("<title>Servlet write Data 2</title>");
//                out.println("</head>");
//                out.println("<body>");
//                out.println("<h1>Servlet write at " + request.getContextPath() + "</h1>");
//                out.println("<h1>" + Httpresponse + "</h1>");
//                out.println("<h2>Send message " + data + "</h3>");
//                out.println("</body>");
//                out.println("</html>");
            }
        } catch (Exception e) {
            log.log(Level.SEVERE, "Exception: ", e);
//            log.log(Level.SEVERE, "Exception: ", msgObject);
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
