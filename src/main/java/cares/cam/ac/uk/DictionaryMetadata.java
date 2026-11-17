package cares.cam.ac.uk;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.json.JSONArray;
import org.json.JSONObject;
import cares.cam.ac.uk.classes.CalculationMethod;
import uk.ac.cam.cares.jps.base.query.RemoteStoreClient;

/** Demonstration conventions seeded from resources; runtime formatting reads the KG. */
final class DictionaryMetadata {
    static final String PROPERTY = "https://www.theworldavatar.com/kg/exposuredictionary/namingConfiguration";
    private static final Pattern TOKEN = Pattern.compile("\\{([a-z_]+)\\}");
    private DictionaryMetadata() {}

    static JSONObject resource(String name) {
        try (InputStream stream = DictionaryMetadata.class.getResourceAsStream("/dictionary/" + name + ".json")) {
            if (stream == null) throw new IllegalArgumentException("Missing dictionary resource: " + name);
            return new JSONObject(new String(stream.readAllBytes(), StandardCharsets.UTF_8));
        } catch (java.io.IOException e) { throw new IllegalStateException(e); }
    }

    static JSONArray configurations() {
        try (InputStream stream = DictionaryMetadata.class.getResourceAsStream("/dictionary/index.json")) {
            return new JSONArray(new String(stream.readAllBytes(), StandardCharsets.UTF_8));
        } catch (java.io.IOException e) { throw new IllegalStateException(e); }
    }

    static JSONObject select(String name) {
        JSONArray ids = configurations();
        for (Object id : ids) {
            JSONObject config = resource(id.toString());
            if (config.getString("id").equals(name) || config.getString("datasetName").equals(name)) return config;
        }
        throw new IllegalArgumentException("No demonstration configuration for " + name + "; supported: " + ids);
    }

    static String iri(String value) {
        QueryClient.buildDatasetsQuery(value); // Reuse absolute SPARQL IRI validation.
        return "<" + value + ">";
    }

    static String literal(String value) {
        return org.eclipse.rdf4j.sparqlbuilder.rdf.Rdf.literalOf(value).getQueryString();
    }

    static JSONObject read(RemoteStoreClient store, String dataset) {
        JSONArray rows = store.executeQuery("SELECT ?config WHERE { " + iri(dataset) + " <" + PROPERTY + "> ?config }");
        if (rows.length() > 1) throw new IllegalArgumentException("Conflicting naming configurations for " + dataset);
        return rows.isEmpty() ? null : new JSONObject(rows.getJSONObject(0).getString("config"));
    }

    static boolean excluded(JSONObject config, CalculationMethod calculation) {
        JSONArray types = config.optJSONArray("excludedCalculationTypes");
        if (types == null) return false;
        for (Object type : types) if (calculation.getRdfTypes().contains(type.toString())) return true;
        return false;
    }

    static String code(JSONObject config, CalculationMethod calculation) {
        if (excluded(config, calculation)) throw new IllegalArgumentException("Calculation type explicitly excluded");
        JSONObject naming = config.getJSONObject("naming");
        JSONObject bindings = naming.getJSONObject("bindings");
        Matcher matcher = TOKEN.matcher(naming.getString("template"));
        StringBuffer output = new StringBuffer();
        while (matcher.find()) {
            String key = matcher.group(1);
            String value;
            if (key.equals("prefix")) value = naming.getString("prefix");
            else {
                JSONObject binding = bindings.getJSONObject(key);
                switch (binding.getString("from")) {
                    case "distance":
                        if (calculation.getDistance() == null) throw new IllegalArgumentException("Missing buffer distance");
                        value = BigDecimal.valueOf(calculation.getDistance()).stripTrailingZeros().toPlainString();
                        break;
                    case "filter": value = calculation.getFilterValues().get(binding.getString("column")); break;
                    case "datasetMetadata": value = config.getJSONObject("datasetMetadata").optString(binding.getString("key"), null); break;
                    case "rdfType":
                        Set<String> matches = new HashSet<>();
                        JSONObject mapping = binding.getJSONObject("mapping");
                        for (String type : calculation.getRdfTypes()) if (mapping.has(type)) matches.add(mapping.getString(type));
                        if (matches.size() != 1) throw new IllegalArgumentException("Missing or ambiguous calculation code");
                        value = matches.iterator().next();
                        break;
                    default: throw new IllegalArgumentException("Unsupported binding source");
                }
                if (value == null) throw new IllegalArgumentException("Missing metadata for " + key);
                if (binding.has("mapping") && !binding.getString("from").equals("rdfType")) {
                    value = binding.getJSONObject("mapping").optString(value, null);
                    if (value == null) throw new IllegalArgumentException("Unmapped filter for " + key);
                }
                if (binding.has("width")) {
                    int number = Integer.parseInt(value);
                    if (key.equals("month") && (number < 1 || number > 12)) throw new IllegalArgumentException("Invalid month");
                    value = String.format(java.util.Locale.ROOT, "%0" + binding.getInt("width") + "d", number);
                }
            }
            if (!value.matches("[A-Za-z0-9]+")) throw new IllegalArgumentException("Invalid code component: " + key);
            matcher.appendReplacement(output, Matcher.quoteReplacement(value));
        }
        matcher.appendTail(output);
        return output.toString();
    }
}
