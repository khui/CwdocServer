package de.mpii;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * Copy from http://www.tutorialspoint.com/servlets/servlets-exception-handling.htm
 * Revised by Kai.
 */


// Extend HttpServlet class
public class ErrorHandler extends HttpServlet {

    // Method to handle GET method request.
    public void doGet(HttpServletRequest request,
                      HttpServletResponse response)
            throws ServletException, IOException {

        // Set response content type
        response.setContentType("text/html");

        PrintWriter out = response.getWriter();
        String title = "Sorry, there are something wrong...";
        String docType =
                "<!doctype html public \"-//w3c//dtd html 4.0 " +
                        "transitional//en\">\n";
        out.println(docType +
                "<html>\n" +
                "<head><title>" + title + "</title></head>\n" +
                "<body bgcolor=\"#f0f0f0\">\n");

        out.println("<h2>There are some errors with the requested page.</h2>");
        out.println("<br /><p>Sorry for this inconvenience. Please try current page later and " +
                "move on to the next judgment.</p>");
        out.println("<br /><p>^_^</p>");

        out.println("</body>");
        out.println("</html>");
    }
    // Method to handle POST method request.
    public void doPost(HttpServletRequest request,
                       HttpServletResponse response)
            throws ServletException, IOException {
        doGet(request, response);
    }
}
