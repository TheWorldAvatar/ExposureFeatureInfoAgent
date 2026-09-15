package cares.cam.ac.uk.classes;

import java.util.Locale;

public class ExposureResult {
    private String exposureIri;
    private String calculationIri;
    private double value;
    private String unit;
    private Double percentile;

    public ExposureResult(String exposureIri, String calculationIri, double value, String unit) {
        this.exposureIri = exposureIri;
        this.calculationIri = calculationIri;
        this.value = value;
        this.unit = unit;
    }

    public String getCalculationIri() {
        return calculationIri;
    }

    public ExposureResult(String exposureIri, String calculationIri, double value, String unit, Double percentile) {
        this(exposureIri, calculationIri, value, unit);
        this.percentile = percentile;
    }

    public String getExposureIri() {
        return exposureIri;
    }

    public String getFormattedValue() {
        String formattedValue;
        if (value > 1) {
            formattedValue = String.format("%.0f %s", value, unit);
        } else {
            formattedValue = String.format("%.3g %s", value, unit);
        }
        return percentile == null ? formattedValue : formattedValue + " (percentile: "
                + String.format(Locale.ROOT, "%.2f", percentile) + ")";
    }
}
