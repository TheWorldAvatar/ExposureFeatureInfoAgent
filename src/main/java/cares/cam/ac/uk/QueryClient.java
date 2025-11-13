package cares.cam.ac.uk;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Logger;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.sparqlbuilder.constraint.propertypath.builder.PropertyPathBuilder;
import org.eclipse.rdf4j.sparqlbuilder.core.Prefix;
import org.eclipse.rdf4j.sparqlbuilder.core.SparqlBuilder;
import org.eclipse.rdf4j.sparqlbuilder.core.Variable;
import org.eclipse.rdf4j.sparqlbuilder.core.query.Queries;
import org.eclipse.rdf4j.sparqlbuilder.core.query.SelectQuery;
import org.eclipse.rdf4j.sparqlbuilder.graphpattern.TriplePattern;
import org.eclipse.rdf4j.sparqlbuilder.rdf.Iri;
import org.eclipse.rdf4j.sparqlbuilder.rdf.Rdf;
import org.json.JSONArray;
import org.json.JSONObject;

import com.cmclinnovations.stack.clients.postgis.PostGISClient;
import com.cmclinnovations.stack.clients.rdf4j.Rdf4jClient;

import uk.ac.cam.cares.jps.base.derivation.ValuesPattern;
import uk.ac.cam.cares.jps.base.query.RemoteRDBStoreClient;
import uk.ac.cam.cares.jps.base.query.RemoteStoreClient;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class QueryClient {
    private static final Logger LOGGER = LogManager.getLogger(QueryClient.class);

    RemoteStoreClient ontopClient;
    RemoteStoreClient federateClient;
    RemoteRDBStoreClient remoteRDBStoreClient;
    String blazegraphUrl;
    String ontopUrl;

    static final Prefix PREFIX_DERIVATION = SparqlBuilder
            .prefix("derivation", Rdf.iri("https://www.theworldavatar.com/kg/ontoderivation/"));
    static final Prefix PREFIX_EXPOSURE = SparqlBuilder
            .prefix("exposure", Rdf.iri("https://www.theworldavatar.com/kg/ontoexposure/"));
    static final Prefix PREFIX_TIMESERIES = SparqlBuilder
            .prefix("timeseries", Rdf.iri("https://www.theworldavatar.com/kg/ontotimeseries/"));

    static final Iri IS_DERIVED_FROM = PREFIX_DERIVATION.iri("isDerivedFrom");
    static final Iri HAS_CALCULATION_METHOD = PREFIX_EXPOSURE.iri("hasCalculationMethod");
    static final Iri BELONGS_TO = PREFIX_DERIVATION.iri("belongsTo");
    static final Iri HAS_VALUE = PREFIX_EXPOSURE.iri("hasValue");
    static final Iri HAS_DISTANCE = PREFIX_EXPOSURE.iri("hasDistance");
    static final Iri HAS_TIME_SERIES = PREFIX_TIMESERIES.iri("hasTimeSeries");
    static final Iri HAS_TIME_CLASS = PREFIX_TIMESERIES.iri("hasTimeClass");
    static final Iri HAS_UNIT = PREFIX_EXPOSURE.iri("hasUnit");

    static final Iri TRIP = PREFIX_EXPOSURE.iri("Trip");

    public QueryClient() {
        String rdbUrl = PostGISClient.getInstance().readEndpointConfig().getJdbcURL("postgres");
        String user = PostGISClient.getInstance().readEndpointConfig().getUsername();
        String password = PostGISClient.getInstance().readEndpointConfig().getPassword();

        remoteRDBStoreClient = new RemoteRDBStoreClient(rdbUrl, user, password);

        String stackOutgoing = Rdf4jClient.getInstance().readEndpointConfig().getOutgoingRepositoryUrl();
        federateClient = new RemoteStoreClient(stackOutgoing);
    }

    JSONObject getResults(String iri) {
        SelectQuery query = Queries.SELECT();
        Iri subject = Rdf.iri(iri);
        Variable derivation = query.var();
        Variable exposure = query.var();
        Variable calculation = query.var();
        Variable exposureResult = query.var();
        Variable exposureValueVar = query.var();
        Variable distanceVar = query.var();
        Variable calculationType = query.var();
        Variable unitVar = query.var();

        TriplePattern gp1 = derivation.has(IS_DERIVED_FROM, subject)
                .andHas(PropertyPathBuilder.of(IS_DERIVED_FROM).then(RDFS.LABEL).build(), exposure);
        TriplePattern gp2 = exposureResult.has(BELONGS_TO, derivation).andHas(HAS_VALUE, exposureValueVar)
                .andHas(HAS_CALCULATION_METHOD, calculation).andHas(HAS_UNIT, unitVar);
        TriplePattern gp3 = calculation.isA(calculationType).andHas(HAS_DISTANCE, distanceVar);

        query.where(gp1, gp2, gp3).select(exposureValueVar, exposure, calculationType, distanceVar, unitVar).prefix(
                PREFIX_DERIVATION, PREFIX_EXPOSURE);

        JSONArray queryResult = federateClient.executeQuery(query.getQueryString());

        JSONObject metadata = new JSONObject();

        for (int i = 0; i < queryResult.length(); i++) {
            String datasetName = queryResult.getJSONObject(i).getString(exposure.getVarName());
            String calculationName = queryResult.getJSONObject(i).getString(calculationType.getVarName());
            calculationName = calculationName.substring(calculationName.lastIndexOf('/') + 1);
            double distance = parseRdfLiteral(queryResult.getJSONObject(i).getString(distanceVar.getVarName()));
            double exposureValue = parseRdfLiteral(
                    queryResult.getJSONObject(i).getString(exposureValueVar.getVarName()));
            String unit = queryResult.getJSONObject(i).getString(unitVar.getVarName());

            String formattedValue = String.format("%.0f %s", exposureValue, unit);

            String distanceKey = String.format("%.0f", distance) + " m";

            if (!metadata.has(datasetName)) {
                JSONObject exposureJson = new JSONObject();
                JSONObject calculationJson = new JSONObject();
                calculationJson.put("collapse", true);

                metadata.put(datasetName, exposureJson);
                exposureJson.put(calculationName, calculationJson);
                calculationJson.put(distanceKey, formattedValue);
            } else {
                if (metadata.getJSONObject(datasetName).has(calculationName)) {
                    metadata.getJSONObject(datasetName).getJSONObject(calculationName).put(distanceKey, formattedValue);
                } else {
                    JSONObject calculationJson = new JSONObject();
                    calculationJson.put("collapse", true);
                    calculationJson.put(distanceKey, formattedValue);
                    metadata.getJSONObject(datasetName).put(calculationName, calculationJson);
                }
            }

        }

        for (String dataset : metadata.keySet()) {
            JSONObject exposureJson = metadata.getJSONObject(dataset);
            for (String calculationName : exposureJson.keySet()) {
                JSONObject calculationJson = exposureJson.getJSONObject(calculationName);
                // distance keys
                // the main purpose of these few lines is to ensure that that properties like 50
                // m, 100 m appears in ascending order in the visualisation
                Set<String> distanceKeys = calculationJson.keySet();
                List<String> distanceSorted = distanceKeys.stream().filter(d -> !d.contentEquals("collapse"))
                        .sorted(Comparator.comparingDouble(this::extractNumber))
                        .collect(Collectors.toList());
                calculationJson.put("display_order", distanceSorted);
            }
        }

        return metadata;
    }

    /**
     * extracts number from strings like "400 m" or "400m"
     */
    private double extractNumber(String s) {
        Pattern numberPattern = Pattern.compile("(\\d+(?:\\.\\d+)?)");
        Matcher m = numberPattern.matcher(s);

        if (m.find()) {
            return Double.parseDouble(m.group(1));
        } else {
            throw new RuntimeException("Parse number error");
        }
    }

    /**
     * with trips
     * 
     * @param iri
     * @param tripIndex
     * @return
     */
    JSONObject getResultsTrajectory(String iri, Integer tripIndex) {
        // split into two queries due to performance issues
        // first query focuses more on time series data
        // second query gets the metadata of the results

        String query1;
        if (tripIndex != null) {
            try (InputStream is = QueryClient.class.getResourceAsStream("trajectory_query.sparql")) {
                query1 = IOUtils.toString(is, StandardCharsets.UTF_8).replace("[TRIP_IRI]", getTripIri(iri)).replace(
                        "[TRIP_VALUE]",
                        String.valueOf(tripIndex));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            if (getTripIri(iri) != null) {
                throw new RuntimeException("Trip instance detected, trip index must be provided in the request");
            }
            try (InputStream is = QueryClient.class.getResourceAsStream("query_without_trips.sparql")) {
                query1 = IOUtils.toString(is, StandardCharsets.UTF_8).replace("[POINT_IRI]", iri);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        JSONArray queryResult = federateClient.executeQuery(query1);

        Map<String, Double> resultToValueMap = new HashMap<>();

        for (int i = 0; i < queryResult.length(); i++) {
            // variable names are in trajectory_query.sparql in the resources folder
            String resultIri = queryResult.getJSONObject(i).getString("result");
            double exposureValue = queryResult.getJSONObject(i).getDouble("val");
            resultToValueMap.put(resultIri, exposureValue);
        }

        ValuesPattern values = new ValuesPattern(SparqlBuilder.var("result"),
                resultToValueMap.keySet().stream().map(r -> Rdf.iri(r)).collect(Collectors.toList()));

        String query2;
        try (InputStream is = QueryClient.class.getResourceAsStream("result_query.sparql")) {
            query2 = IOUtils.toString(is, StandardCharsets.UTF_8).replace("[VALUES_CLAUSE]", values.getQueryString())
                    .replace("[SUBJECT]", iri);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        JSONArray queryResult2 = federateClient.executeQuery(query2);

        if (queryResult2.length() != resultToValueMap.keySet().size()) {
            throw new RuntimeException("Unexpected query result size");
        }

        Map<String, Double> resultToDistanceMap = new HashMap<>();
        Map<String, String> resultToCalculationMap = new HashMap<>();
        Map<String, String> resultToUnitMap = new HashMap<>();
        Map<String, String> resultToDatasetMap = new HashMap<>();

        for (int i = 0; i < queryResult2.length(); i++) {
            String resultIri = queryResult2.getJSONObject(i).getString("result");
            String calculation = queryResult2.getJSONObject(i).getString("calculation_type");
            double distance = queryResult2.getJSONObject(i).getDouble("distance");
            String unit = queryResult2.getJSONObject(i).getString("unit");
            String datasetName = queryResult2.getJSONObject(i).getString("exposure_dataset_name");

            resultToCalculationMap.put(resultIri, calculation);
            resultToDistanceMap.put(resultIri, distance);
            resultToUnitMap.put(resultIri, unit);
            resultToDatasetMap.put(resultIri, datasetName);
        }

        JSONObject metadata = new JSONObject();
        resultToValueMap.keySet().forEach(r -> {
            double exposureValue = resultToValueMap.get(r);
            double distance = resultToDistanceMap.get(r);
            String datasetName = resultToDatasetMap.get(r);
            String exposureUnit = resultToUnitMap.get(r);
            String calculationName = resultToCalculationMap.get(r);
            calculationName = calculationName.substring(calculationName.lastIndexOf('/') + 1);
            String distanceKey = String.format("%.0f", distance) + " m";
            String formattedValue = String.format("%.0f %s", exposureValue, exposureUnit);

            if (!metadata.has(datasetName)) {
                JSONObject exposureJson = new JSONObject();
                JSONObject calculationJson = new JSONObject();
                calculationJson.put("collapse", true);

                metadata.put(datasetName, exposureJson);
                exposureJson.put(calculationName, calculationJson);
                calculationJson.put(distanceKey, formattedValue);
            } else {
                if (metadata.getJSONObject(datasetName).has(calculationName)) {
                    metadata.getJSONObject(datasetName).getJSONObject(calculationName).put(distanceKey,
                            formattedValue);
                } else {
                    JSONObject calculationJson = new JSONObject();
                    calculationJson.put("collapse", true);
                    calculationJson.put(distanceKey, formattedValue);
                    metadata.getJSONObject(datasetName).put(calculationName, calculationJson);
                }
            }
        });

        return metadata;
    }

    /**
     * without trips
     */
    JSONObject getResultsTrajectory(String iri) {
        if (getTripIri(iri) != null) {
            throw new RuntimeException("Trip instance detected, trip index must be provided in the request");
        }
        JSONObject metadata = new JSONObject();

        return metadata;
    }

    String getTripIri(String trajectoryIri) {
        SelectQuery query = Queries.SELECT();
        Variable tripVar = query.var();
        Variable timeseriesVar = query.var();

        query.where(Rdf.iri(trajectoryIri).has(HAS_TIME_SERIES, timeseriesVar),
                tripVar.has(HAS_TIME_SERIES, timeseriesVar).andIsA(TRIP)).prefix(PREFIX_TIMESERIES, PREFIX_EXPOSURE);

        JSONArray queryResult = federateClient.executeQuery(query.getQueryString());

        if (queryResult.isEmpty()) {
            return null;
        }
        return queryResult.getJSONObject(0).getString(tripVar.getVarName());
    }

    /**
     * parse something like "1000"^^<http://www.w3.org/2001/XMLSchema#integer>
     */
    private double parseRdfLiteral(String literal) {
        try {
            return Double.parseDouble(literal);
        } catch (NumberFormatException e) {
            int start = literal.indexOf('"') + 1;
            int end = literal.indexOf('"', start);

            String value = literal.substring(start, end);
            return Double.parseDouble(value);
        }
    }
}
