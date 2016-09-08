package de.mpii;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.*;
import javax.servlet.http.*;

/**
 *
 @author Arunav, Kai
 */
public class CwdocServer extends HttpServlet {

    private Map<String, String> request2qidcwid = new HashMap<>();
    private String root;

    public void init(ServletConfig cfg)throws ServletException{
        root = cfg.getServletContext().getInitParameter("application.dir");
        String requestid2qidcwid = root + "/cw4y/dictionary/did2docurl.dict";
        try(BufferedReader br = new BufferedReader(new FileReader(new File(requestid2qidcwid)))) {
            while(br.ready()){
                String line = br.readLine();
                String[] cols = line.split(" ");
                request2qidcwid.put(cols[0], cols[1]);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    /**
     Processes requests for both HTTP <code>GET</code> and <code>POST</code> methods.
     @param request servlet request
     @param response servlet response
     @throws ServletException if a servlet-specific error occurs
     @throws IOException if an I/O error occurs
     */
    private void processRequest(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        response.setContentType("text/html;charset=UTF-8");

        String docid = request.getParameter("docid");
        String qidcwid = request2qidcwid.get(docid);
        String fileName = root + "/cw4y/" + qidcwid + "/part-00000";
        StringBuilder sb = new StringBuilder();
        try(BufferedReader br = new BufferedReader(new FileReader(fileName))){
            while (br.ready()) {
                sb.append(br.readLine());
            }
        }
        try (PrintWriter out = response.getWriter()) {
            out.println(sb.toString());
        }
    }

    /**
     Handles the HTTP <code>GET</code> method.
     @param request servlet request
     @param response servlet response
     @throws ServletException if a servlet-specific error occurs
     @throws IOException if an I/O error occurs
     */
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException
    {
        processRequest(request, response);
    }


    /**
     Handles the HTTP <code>POST</code> method.
     @param request servlet request
     @param response servlet response
     @throws ServletException if a servlet-specific error occurs
     @throws IOException if an I/O error occurs
     */
    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        processRequest(request, response);
    }


    /**
     Returns a short description of the servlet.
     @return a String containing servlet description
     */
    @Override
    public String getServletInfo() {
        return "Short description";
    }


}
