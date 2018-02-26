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

        PutTSDB.logger.log(Level.INFO, "Start servlet TSDB process request:" + request.getSession().getId());
        JsonElement jsonResult;

        try {
            int code = HttpServletResponse.SC_OK;
            String Httpresponse = "OK";
            String uid = request.getParameter("UUID");
            uid = uid.trim();

            String version = request.getParameter("version");
            if (version != null) {
                version = version.trim();
            } else {
                version = "";
            }
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
                barlusUser User;
                if (AppConfiguration.getUsers().containsKey(uid)) {
                    User = AppConfiguration.getUsers().get(uid);
                } else {
                    User = AppConfiguration.getUser(uid);
                }

                if (loggerTest.isDebugEnabled()) {
                    loggerTest.debug("msg =" + request.getParameter("data"));
                }
                if (User == null) {
                    code = 406;
                    Httpresponse = "\"FAILURE\",messge:\"NOT VALID UUID\"";
                    PutTSDB.logger.log(Level.ERROR, "NOT VALID UUID:" + uid + "IP:" + request.getHeader("X-Real-IP") + " data:" + request.getParameter("data"));
                } else {
                    if (User.isActive()) {
                        if (User.getBalance() > 0) {
                            msg = request.getParameter("data");
                        } else {
                            code = 402;
                            Httpresponse = "\"FAILURE\",messge:\"insufficient funds on the account\"";
                        }
                    } else {
                        code = 418;
                        Httpresponse = "\"FAILURE\",messge:\"NOT ACTIVE UUID\"";
                    }
                }
//                if (idx > -1) {
//                    msg = request.getParameter("data");
//                } else {
//                    AppConfiguration.initUsers();
//                    idx = Arrays.binarySearch(AppConfiguration.getUsers(), uid, Collections.reverseOrder());
//                    if (idx > -1) {
//                        msg = request.getParameter("data");
//                    } else {
//                        code = 424;
//                        Httpresponse = "\"FAILURE\",messge:\"NOT VALID UUID\"";
//                        PutTSDB.logger.log(Level.ERROR, "NOT VALID UUID:" + uid + "IP:" + request.getHeader("X-Real-IP") + " data:" + request.getParameter("data"));
//                    }
//                }
            } else {
                code = HttpServletResponse.SC_NOT_ACCEPTABLE;
                Httpresponse = "\"FAILURE\",messge:\"EMPTY UUID\"";
                msg = "";
//                Httpresponse = "FAILURE";
                PutTSDB.logger.log(Level.ERROR, "EMPTY UUID:" + request.getSession().getId());
            }
            PutTSDB.logger.log(Level.INFO, "Check UUID:" + request.getSession().getId());
            if (code == HttpServletResponse.SC_OK) {
                if (!msg.equals("")) {
                    PutTSDB.logger.log(Level.INFO, "MSG:" + msg + " :" + request.getSession().getId());
                    try {
                        JsonParser parser = new JsonParser();
                        JsonElement json = parser.parse(msg);
                        switch (version) {
                            case "":
                                if (json.isJsonArray()) {

                                    ParseResult result = this.prepareArray(json.getAsJsonArray(), uid, topic, request);
                                    code = result.getCode();
                                    if (!result.getMessage().isEmpty()) {
                                        Httpresponse = result.getMessage();
                                    }

                                } else {
                                    if (json.isJsonObject()) {
                                        ParseResult result = this.prepareJsonObject(json, uid, topic, request);
                                        code = result.getCode();
                                        if (!result.getMessage().isEmpty()) {
                                            Httpresponse = result.getMessage();
                                        }

                                    } else {
                                        code = 415;
                                        Httpresponse = "\"FAILURE\",\"message\":\" NOT Valid Json \"";
                                        PutTSDB.logger.log(Level.ERROR, "NOT Valid Json:" + msg);
                                    }
                                }
                                break;
                            case "2":
                                if (json.isJsonObject()) {
                                    ParseResult result = this.prepareJsonObjectV2(json, uid, topic, request);
                                    code = result.getCode();
                                    if (!result.getMessage().isEmpty()) {
                                        Httpresponse = result.getMessage();
                                    }
                                } else {
                                    code = 415;
                                    Httpresponse = "\"FAILURE\",\"message\":\"NOT Valid Json \"";
                                    PutTSDB.logger.log(Level.ERROR, "NOT Valid Json:" + msg);
                                }
                                break;
                            default:
                                code = 415;
                                Httpresponse = "\"FAILURE\",\"message\":\"Not Avalible Version \"";
                                PutTSDB.logger.log(Level.ERROR, "Not Avalible Version:" + msg);
                                break;
                        }

                    } catch (JsonSyntaxException e) {
                        code = 415;
                        Httpresponse = "\"FAILURE\",\"message\":\"" + e.getMessage() + " \"";
                        PutTSDB.logger.log(Level.ERROR, "NOT Valid Json:" + msg);
                    }

                } else {
                    code = 422;
                    Httpresponse = "\"FAILURE\",\"message\":\"Empty Data\"";
                }
            }
            response.setContentType(
                    "json;charset=UTF-8");
            try (PrintWriter out = response.getWriter()) {
                response.setStatus(code);
//                if (!checkerrors.isEmpty()) {
//                    checkerrors = ", \"advansed\":[" + checkerrors.substring(0, checkerrors.length() - 1) + "]";
//                }
                out.println(" {\"state\":" + Httpresponse + "}");
            }
        } catch (Exception e) {
            PutTSDB.logger.log(Level.ERROR, "Exception: ", e);
            PutTSDB.logger.log(Level.ERROR, "Exception for request: " + request.getParameterMap().toString());
            PutTSDB.logger.log(Level.ERROR, "Exception for xIp: " + request.getHeader("X-Real-IP"));
        }
    }

    private ParseResult prepareJsonObjectV2(JsonElement Metric, String uid, String topic, HttpServletRequest request) {

        if (Metric.getAsJsonObject().get("data") != null) {
            if (Metric.getAsJsonObject().get("data").isJsonPrimitive()) {
                PutTSDB.logger.log(Level.ERROR, "data not json in input " + Metric.toString());
                return new ParseResult(411, "{\"message\":\"data not json in input\"}");
            }
        } else {
            PutTSDB.logger.log(Level.ERROR, "data not exist in input " + Metric.toString());
            return new ParseResult(411, "{\"message\":\"data not exist in input\"}");
        }

        if (Metric.getAsJsonObject().get("tags") != null) {
            if (!Metric.getAsJsonObject().get("tags").isJsonPrimitive()) {
                if (Metric.getAsJsonObject().get("tags").getAsJsonObject().get("host") == null) {
                    PutTSDB.logger.log(Level.INFO, "host not exist in input " + Metric.toString());
                }
                if (Metric.getAsJsonObject().get("tags").getAsJsonObject().get("type") == null) {
                    PutTSDB.logger.log(Level.INFO, "type not exist in input " + Metric.toString());
                }
                if (Metric.getAsJsonObject().get("tags").getAsJsonObject().get("group") == null) {
                    PutTSDB.logger.log(Level.INFO, "group not exist in input " + Metric.toString());
                }
                if (Metric.getAsJsonObject().get("tags").getAsJsonObject().size() > 7) {
                    PutTSDB.logger.log(Level.ERROR, "tags not json in input " + Metric.toString());
                    return new ParseResult(411, "{\"message\":\"Too many tags: " + Metric.getAsJsonObject().get("tags").getAsJsonObject().size() + " maximum allowed: 7\"}");
                }
                Metric.getAsJsonObject().get("tags").getAsJsonObject().addProperty("UUID", uid);
            } else {
                PutTSDB.logger.log(Level.ERROR, "tags not json in input " + Metric.toString());
                return new ParseResult(411, "{\"message\":\"tags not json in input\"}");
            }
        } else {
            PutTSDB.logger.log(Level.ERROR, "tags not exist in input " + Metric.toString());
            return new ParseResult(411, "{\"message\":\"tags not exist in input\"}");
        }

        if (Metric.getAsJsonObject().get("timestamp") == null) {
            PutTSDB.logger.log(Level.ERROR, "timestamp not exist in input " + Metric.toString());
            return new ParseResult(411, "{\"message\":\"timestamp not exist in input\"}");
        }

        Metric.getAsJsonObject().addProperty("version", 2);
//        if (Metric.getAsJsonObject().get("metric") == null) {
//            PutTSDB.logger.log(Level.ERROR, "metric name not exist in input " + Metric.toString());
//            return new ParseResult(411, "{\"message\":\"metric name not exist in input\"}");
//        }
//        if (Metric.getAsJsonObject().get("value") == null) {
//            PutTSDB.logger.log(Level.ERROR, "value not exist in input " + Metric.toString());
//            return new ParseResult(411, "{\"message\":\"value not exist in input\"}");
//        }

        final ProducerRecord<String, String> data = new ProducerRecord<>(topic, Metric.toString());
        PutTSDB.logger.log(Level.INFO, "Prepred data to send:" + request.getSession().getId());
        AppConfiguration.getProducer().send(data);
        if (loggerTest.isDebugEnabled()) {
            loggerTest.debug("Send data =" + Metric.toString());
        }
        PutTSDB.logger.log(Level.INFO, "Prepred data send:" + Metric.toString());

        return new ParseResult(HttpServletResponse.SC_OK, "");
    }

    private ParseResult prepareJsonObject(JsonElement Metric, String uid, String topic, HttpServletRequest request) {
        if (Metric.getAsJsonObject().get("tags") != null) {
            if (!Metric.getAsJsonObject().get("tags").isJsonPrimitive()) {
                if (Metric.getAsJsonObject().get("tags").getAsJsonObject().get("host") == null) {
                    PutTSDB.logger.log(Level.INFO, "host not exist in input " + Metric.toString());
                }
                if (Metric.getAsJsonObject().get("tags").getAsJsonObject().get("type") == null) {
                    PutTSDB.logger.log(Level.INFO, "type not exist in input " + Metric.toString());
                }
                if (Metric.getAsJsonObject().get("tags").getAsJsonObject().get("group") == null) {
                    PutTSDB.logger.log(Level.INFO, "group not exist in input " + Metric.toString());
                }
                if (Metric.getAsJsonObject().get("timestamp") == null) {
                    PutTSDB.logger.log(Level.ERROR, "timestamp not exist in input " + Metric.toString());
                    return new ParseResult(411, "{\"message\":\"timestamp not exist in input\"}");
                }
                if (Metric.getAsJsonObject().get("tags").getAsJsonObject().size() > 7) {
                    PutTSDB.logger.log(Level.ERROR, "tags not json in input " + Metric.toString());
                    return new ParseResult(411, "{\"message\":\"Too many tags: " + Metric.getAsJsonObject().get("tags").getAsJsonObject().size() + " maximum allowed: 7\"}");
                }
                if (Metric.getAsJsonObject().get("metric") == null) {
                    PutTSDB.logger.log(Level.ERROR, "metric name not exist in input " + Metric.toString());
                    return new ParseResult(411, "{\"message\":\"metric name not exist in input\"}");
                }
//                if (Metric.getAsJsonObject().get("value") == null) {
//                    PutTSDB.logger.log(Level.ERROR, "value not exist in input " + Metric.toString());
//                    return new ParseResult(411, "{\"message\":\"value not exist in input\"}");
//                }

                if (Metric.getAsJsonObject().get("value") == null) {
                    PutTSDB.logger.log(Level.ERROR, "value not exist in input " + Metric.toString());
                    return new ParseResult(411, "{\"message\":\"value not exist in input\"}");
                }
                if (Metric.getAsJsonObject().get("value").isJsonNull()) {
                    PutTSDB.logger.log(Level.ERROR, "value not exist in input " + Metric.toString());
                    return new ParseResult(411, "{\"message\":\"value not exist in input\"}");
                }
                try {
                    Metric.getAsJsonObject().get("value").getAsDouble();
                } catch (Exception e) {
                    PutTSDB.logger.log(Level.ERROR, "value not Double in input " + Metric.toString());
                    return new ParseResult(411, "{\"message\":\"value not Double in input\"}");
                }

                Metric.getAsJsonObject().get("tags").getAsJsonObject().addProperty("UUID", uid);
            } else {
                PutTSDB.logger.log(Level.ERROR, "tags not json in input " + Metric.toString());
                return new ParseResult(411, "{\"message\":\"tags not json in input\"}");
            }
        } else {
            PutTSDB.logger.log(Level.ERROR, "tags not exist in input " + Metric.toString());
            return new ParseResult(411, "{\"message\":\"tags not exist in input\"}");
        }

        final ProducerRecord<String, String> data = new ProducerRecord<>(topic, Metric.toString());
        PutTSDB.logger.log(Level.INFO, "Prepred data to send:" + request.getSession().getId());
        AppConfiguration.getProducer().send(data);
        if (loggerTest.isDebugEnabled()) {
            loggerTest.debug("Send data =" + Metric.toString());
        }
        PutTSDB.logger.log(Level.INFO, "Prepred data send:" + Metric.toString());

        return new ParseResult(HttpServletResponse.SC_OK, "");
    }

    private ParseResult prepareArray(JsonArray jsonResult, String uid, String topic, HttpServletRequest request) {
        int result = HttpServletResponse.SC_OK;
        String Httpresponse = "";
        String checkerrors = "";
//                jsonResult = json.getAsJsonArray();

        if (jsonResult.size() > 0) {
            for (int i = 0; i < jsonResult.size(); i++) {
                JsonElement Metric = jsonResult.get(i);
                if (Metric.getAsJsonObject().get("tags") != null) {
                    if (!Metric.getAsJsonObject().get("tags").isJsonPrimitive()) {
                        if (Metric.getAsJsonObject().get("tags").getAsJsonObject().get("host") == null) {
                            PutTSDB.logger.log(Level.INFO, "host not exist in input " + jsonResult.toString());
                        }
                        if (Metric.getAsJsonObject().get("tags").getAsJsonObject().get("type") == null) {
                            PutTSDB.logger.log(Level.INFO, "type not exist in input " + jsonResult.toString());
                        }
                        if (Metric.getAsJsonObject().get("tags").getAsJsonObject().get("group") == null) {
                            PutTSDB.logger.log(Level.INFO, "group not exist in input " + jsonResult.toString());
                        }
                        if (Metric.getAsJsonObject().get("tags").getAsJsonObject().size() > 7) {
                            PutTSDB.logger.log(Level.ERROR, "tags not json in input " + Metric.toString());
                            jsonResult.remove(i);
//                            checkerrors = checkerrors + "{\"message\":\"Too many tags: " + Metric.getAsJsonObject().get("tags").getAsJsonObject().size() + " maximum allowed: 7\"},";                            
                            return new ParseResult(411, "{\"message\":\"Too many tags: " + Metric.getAsJsonObject().get("tags").getAsJsonObject().size() + " maximum allowed: 7\"}");
                        }
                        if (Metric.getAsJsonObject().get("timestamp") == null) {
                            PutTSDB.logger.log(Level.ERROR, "timestamp not exist in input " + jsonResult.toString());
                            jsonResult.remove(i);
                            checkerrors = checkerrors + "{\"message\":\"timestamp not exist in input\"},";
                            i--;
                            continue;

                        }

                        if (Metric.getAsJsonObject().get("metric") == null) {
                            PutTSDB.logger.log(Level.ERROR, "metric not exist in input " + jsonResult.toString());
                            jsonResult.remove(i);
                            checkerrors = checkerrors + "{\"message\":\"metric not exist in input\"},";
                            i--;
                            continue;
                        }
                        if (Metric.getAsJsonObject().get("value") == null) {
                            PutTSDB.logger.log(Level.ERROR, "value not exist in input " + jsonResult.toString());
                            jsonResult.remove(i);
                            checkerrors = checkerrors + "{\"message\":\"value not exist in input\"},";
                            i--;
                            continue;
                        }
                        if (Metric.getAsJsonObject().get("value").isJsonNull()) {
                            PutTSDB.logger.log(Level.ERROR, "value not exist in input " + jsonResult.toString());
                            jsonResult.remove(i);
                            checkerrors = checkerrors + "{\"message\":\"value not exist in input\"},";
                            i--;
                            continue;
                        }
                        try {
                            Metric.getAsJsonObject().get("value").getAsDouble();
                        } catch (Exception e) {                            
                            PutTSDB.logger.log(Level.ERROR, "value not Double in input " + Metric.toString());
//                            PutTSDB.logger.log(Level.ERROR, "In JSON " + jsonResult.toString());
                            jsonResult.remove(i);
                            checkerrors = checkerrors + "{\"message\":\"value not Double in input\"},";
                            i--;
                            continue;
                        }

//                                        Date date = new Date(Metric.getAsJsonObject().get("timestamp").getAsLong()*1000);
//                                        PutTSDB.logger.log(Level.ERROR, "Metric Name " + Metric.getAsJsonObject().get("metric") + " in Time:" + date + " by Value: " + Metric.getAsJsonObject().get("value") + " vs host: " + Metric.getAsJsonObject().get("tags").getAsJsonObject().get("host"));
                        Metric.getAsJsonObject().get("tags").getAsJsonObject().addProperty("UUID", uid);
                    } else {
                        jsonResult.remove(i);
                        checkerrors = checkerrors + "{\"message\":\"tags not json in input\"},";
                        PutTSDB.logger.log(Level.ERROR, "tags not json in input " + jsonResult.toString());
                        i--;
                    }
                } else {
                    checkerrors = checkerrors + "{\"message\":\"tags not exist in input\"},";
                    jsonResult.remove(i);
                    PutTSDB.logger.log(Level.ERROR, "tags not exist in input " + jsonResult.toString());
                    i--;
                }
            }

            if (jsonResult.size() > 0) {
//                            final KeyedMessage<String, String> data = new KeyedMessage<>(topic, jsonResult.toString());
                final ProducerRecord<String, String> data = new ProducerRecord<>(topic, jsonResult.toString());
                PutTSDB.logger.log(Level.INFO, "Prepred data to send:" + request.getSession().getId());
                AppConfiguration.getProducer().send(data);
                if (loggerTest.isDebugEnabled()) {
                    loggerTest.debug("Send data =" + jsonResult.toString());
                }
                PutTSDB.logger.log(Level.INFO, "Prepred data send:" + jsonResult.toString());
            } else {
//                        Httpresponse = "Not valid json Array";
                result = 411;
                Httpresponse = "\"FAILURE\",\"message\":\"Not valid data\"";
                PutTSDB.logger.log(Level.ERROR, "NOT VALID JSON Array Remove by barlus:" + " IP:" + request.getHeader("X-Real-IP") + " data:" + jsonResult.toString());
            }

        } else {
//                        Httpresponse = "Not valid json Array";
            result = 411;
            Httpresponse = "\"FAILURE\",\"message\":\"NOT VALID JSON Array\"";
            PutTSDB.logger.log(Level.ERROR, "NOT VALID JSON Empty array:" + " IP:" + request.getHeader("X-Real-IP") + " data:" + jsonResult.toString());
        }
        if (!checkerrors.isEmpty()) {
            checkerrors = ", \"advansed\":[" + checkerrors.substring(0, checkerrors.length() - 1) + "]";
        }
        Httpresponse = Httpresponse + checkerrors;
        return new ParseResult(result, Httpresponse);
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
