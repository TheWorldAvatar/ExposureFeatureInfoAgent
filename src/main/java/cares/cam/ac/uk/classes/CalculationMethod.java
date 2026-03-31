package cares.cam.ac.uk.classes;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CalculationMethod {
    String iri;
    Double distance = null;// buffer distance
    String name; // public facing name
    Map<String, String> datasetFilter;
    private static final String YEAR_FILTER = "year";

    public CalculationMethod(String iri) {
        this.iri = iri;

        datasetFilter = new HashMap<>();
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setDistance(double distance) {
        this.distance = distance;
    }

    public void setDatasetFilter(String filter, String value) {
        datasetFilter.put(filter, value);
    }

    public String getFormattedDistance() {
        if (distance != null) {
            return String.format("%.0f", distance) + " m";
        } else {
            return null;
        }
    }

    public String getName() {
        return name;
    }

    public List<String> getDatasetFilters() {
        // if year is present, forced to the front, and the other keys are combined in
        // alphabetical order
        List<String> filtersForOutput = new ArrayList<>();
        List<String> filters = new ArrayList<>(datasetFilter.keySet());
        if (datasetFilter.containsKey(YEAR_FILTER)) {
            filtersForOutput.add(String.format("%s=%s", YEAR_FILTER, datasetFilter.get(YEAR_FILTER)));
            filters.remove(YEAR_FILTER);
        }

        // any other filters that is not year
        if (!filters.isEmpty()) {
            Collections.sort(filters); // sort alphabetical order
            List<String> otherFilters = new ArrayList<>();
            filters.forEach(filter -> {
                otherFilters.add(String.format("%s=%s", filter, datasetFilter.get(filter)));
            });
            filtersForOutput.add(String.join(", ", otherFilters));
        }

        return filtersForOutput;
    }
}
