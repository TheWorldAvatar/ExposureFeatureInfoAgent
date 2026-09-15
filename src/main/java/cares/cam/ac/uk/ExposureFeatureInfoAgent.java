package cares.cam.ac.uk;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;
import org.apache.http.entity.ContentType;

import org.json.JSONException;
import org.json.JSONArray;
import org.json.JSONObject;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@WebServlet(urlPatterns = { ExposureFeatureInfoAgent.STANDARD_ROUTE, ExposureFeatureInfoAgent.TRAJECTORY_ROUTE,
        ExposureFeatureInfoAgent.SQL_ROUTE, ExposureFeatureInfoAgent.TIMELINE_ROUTE })
public class ExposureFeatureInfoAgent extends HttpServlet {
    private static final Logger LOGGER = LogManager.getLogger(ExposureFeatureInfoAgent.class);
    static final String STANDARD_ROUTE = "/feature-info-agent/get";
    static final String SQL_ROUTE = "/sql/feature-info-agent/get";
    static final String TRAJECTORY_ROUTE = "/trajectory/feature-info-agent/get";
    static final String TIMELINE_ROUTE = "/timeline";
    QueryClient queryClient;
    private final TimelineAuthentication timelineAuthentication = new TimelineAuthentication();

    @Override
    public void doGet(HttpServletRequest req, HttpServletResponse resp) {
        String iri = req.getParameter("iri");
        LOGGER.info("Received request for iri = <{}>", iri);

        JSONObject response = new JSONObject();
        JSONArray timelineResponse = null;
        int status = Response.Status.OK.getStatusCode();
        if (req.getServletPath().equals(TIMELINE_ROUTE)) {
            try {
                String userId = timelineAuthentication.authenticate(req.getHeader("Authorization"));
                timelineResponse = queryClient.getTimelineResults(userId,
                        req.getParameter("lowerbound"), req.getParameter("upperbound"));
            } catch (TimelineAuthentication.AuthenticationException e) {
                status = Response.Status.UNAUTHORIZED.getStatusCode();
                resp.setHeader("WWW-Authenticate", "Bearer");
                response.put("error", e.getMessage());
            } catch (IllegalArgumentException e) {
                status = Response.Status.BAD_REQUEST.getStatusCode();
                response.put("error", e.getMessage());
            }
        } else if (req.getServletPath().equals(STANDARD_ROUTE)) {
            response.put("meta", queryClient.getExposureResults(iri));
        } else if (req.getServletPath().equals(SQL_ROUTE)) {
            response.put("meta", queryClient.getExposureResultsSql(iri));
        } else if (req.getServletPath().equals(TRAJECTORY_ROUTE)) {
            String tripIndexString = req.getParameter("trip");
            String time = req.getParameter("time_as_number");

            if (tripIndexString != null) {
                // query result specific to the trip index
                int tripIndex = Integer.parseInt(req.getParameter("trip"));
                response.put("meta", queryClient.getResultsTrajectory(iri, tripIndex, time));
            } else {
                //
                response.put("meta", queryClient.getResultsTrajectory(iri, null, time));
            }
        }

        try {
            resp.setStatus(status);
            resp.setContentType(ContentType.APPLICATION_JSON.getMimeType());
            resp.setCharacterEncoding("UTF-8");
            resp.getOutputStream().write(
                    (timelineResponse != null ? timelineResponse.toString() : response.toString())
                            .getBytes(StandardCharsets.UTF_8));
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
