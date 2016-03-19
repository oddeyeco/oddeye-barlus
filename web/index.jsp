<%-- 
    Document   : index
    Created on : Mar 16, 2016, 9:02:51 AM
    Author     : vahan
--%>

<%@page contentType="text/html" pageEncoding="UTF-8"%>
<!DOCTYPE html>
<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
        <title>JSP Page</title>
    </head>
    <body>
        <h1>Hello Is Test Page!</h1>

        <form name="Name Input Form " action="response" method="POST" >
            <div>Enter value:</div>
            <div>UID <input type="text" name="UID" value="" /></div>
            <div>RAM <input type="text" name="RAM" value="" /> RAM val2 <input type="text" name="RAM[val2]" value="" />RAM val3 <input type="text" name="RAM[val3]" value="" /></div>
            <div>CPU <input type="text" name="CPU" value="" /></div>
            <div>SSD <input type="text" name="SSD" value="" /></div>
            <div>https <input type="text" name="https" value="" /></div>
            <div>http <input type="text" name="http" value="" /></div>
            <div>database <input type="text" name="database" value="" /></div>
            <input type="submit" value="OK" />
        </form>
    </body>
</html>
