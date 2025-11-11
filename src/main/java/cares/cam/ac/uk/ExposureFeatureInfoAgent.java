package cares.cam.ac.uk;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;
import org.apache.http.entity.ContentType;

import org.json.JSONException;
import org.json.JSONObject;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@WebServlet(urlPatterns = { ExposureFeatureInfoAgent.STANDARD_ROUTE, ExposureFeatureInfoAgent.TRAJECTORY_ROUTE })
public class ExposureFeatureInfoAgent extends HttpServlet {
    private static final Logger LOGGER = LogManager.getLogger(ExposureFeatureInfoAgent.class);
    static final String STANDARD_ROUTE = "/feature-info-agent/get";
    static final String TRAJECTORY_ROUTE = "/trajectory/feature-info-agent/get";
    QueryClient queryClient;

    @Override
    public void doGet(HttpServletRequest req, HttpServletResponse resp) {
        String iri = req.getParameter("iri");
        LOGGER.info("Received request for iri = <{}>", iri);

        JSONObject response = new JSONObject();
        if (req.getServletPath().equals(STANDARD_ROUTE)) {
            response.put("meta", queryClient.getResults(iri));
        } else if (req.getServletPath().equals(TRAJECTORY_ROUTE)) {
            int tripIndex = Integer.parseInt(req.getParameter("trip"));
            response.put("meta", queryClient.getResultsTrajectory(iri, tripIndex));
        }

        try {
            resp.setStatus(Response.Status.OK.getStatusCode());
            resp.getWriter().write(response.toString());
            resp.setContentType(ContentType.APPLICATION_JSON.getMimeType());
            resp.setCharacterEncoding("UTF-8");
        } catch (IOException e) {
            LOGGER.error(e.getMessage());
            LOGGER.error("Failed to write HTTP response");
        } catch (JSONException e) {
            LOGGER.error(e.getMessage());
            LOGGER.error("Failed to create JSON object for HTTP response");
        }
    }

    @Override
    public void init() throws ServletException {
        queryClient = new QueryClient();
    }
}
