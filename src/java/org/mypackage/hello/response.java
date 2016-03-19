/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.mypackage.hello;

import java.io.IOException;
import java.io.PrintWriter;
import javax.servlet.ServletException;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Enumeration;

import java.util.Arrays;
import java.util.Collections;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

import java.util.Properties;
import java.util.Date;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 *
 * @author vahan
 */
public class response extends HttpServlet {

    private static final String sFileName = "config.properties";
    private static String sDirSeparator = System.getProperty("file.separator");
    private static Properties configProps = new Properties();

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
        //global props
        String sFilePath = "";
        File currentDir = new File(".");
        try {
            ServletContext ctx = request.getSession().getServletContext();
            String path = null;
            String p = ctx.getResource("/").getPath();
            path = p.substring(0, p.lastIndexOf("/"));
            path = path.substring(path.lastIndexOf("/") + 1);
            sFilePath = p + sFileName;
            FileInputStream ins = new FileInputStream(sFilePath);
            configProps.load(ins);
        } catch (FileNotFoundException e) {
            System.out.println("File not found!");
            e.printStackTrace();

        }
        //kafka part
        String brokerlist = configProps.getProperty("broker.list");

        Properties props = new Properties();
        props.put("metadata.broker.list",
                brokerlist);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);
        String runtime = new Date().toString();
        String msg = "time=" + runtime;

        String userlist = configProps.getProperty("uid.list");
        String[] users = userlist.split(":");
        Arrays.sort(users);
        Arrays.sort(users, Collections.reverseOrder());
        String uid = request.getParameter("UID");
//        String uid = "1";
        int idx = Arrays.binarySearch(users, uid, Collections.reverseOrder());

        response.setContentType("text/html;charset=UTF-8");
        try (PrintWriter out = response.getWriter()) {
            if (idx > -1) {

                out.println("<!DOCTYPE html>");
                out.println("<html>");
                out.println("<head>");
                out.println("<title>Response</title>");
                out.println("</head>");
                out.println("<body>");                
                Enumeration<String> parameterNames = request.getParameterNames();

                while (parameterNames.hasMoreElements()) {
                    String paramName = parameterNames.nextElement();
                    out.println(paramName);
                    msg = msg + ";" + paramName + "=>";
                    out.println("=");
                    String[] paramValues = request.getParameterValues(paramName);
                    for (int i = 0; i < paramValues.length; i++) {
                        String paramValue = paramValues[i];
                        out.println(paramValue);
                        msg = msg + paramValue;
                    }
                }
                out.println("</body>");
                out.println("</html>");
                msg = msg + "\n";
                String topic = configProps.getProperty("broker.topic");
                KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, msg);

                producer.send(data);
            } else {
                out.println("<!DOCTYPE html>");
                out.println("<html>");
                out.println("<head>");
                out.println("<title>Response</title>");
                out.println("</head>");
                out.println("<body>");
                out.println("<h1>Invalid UID</h1>");
                out.println("</body>");
                out.println("</html>");
            }
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
