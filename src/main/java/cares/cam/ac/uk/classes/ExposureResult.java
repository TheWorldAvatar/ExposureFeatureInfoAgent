package cares.cam.ac.uk.classes;

public class ExposureResult {
    private String exposureIri;
    private String calculationIri;
    private double value;
    private String unit;

    public ExposureResult(String exposureIri, String calculationIri, double value, String unit) {
        this.exposureIri = exposureIri;
        this.calculationIri = calculationIri;
        this.value = value;
        this.unit = unit;
    }

    public String getCalculationIri() {
        return calculationIri;
    }

    public String getExposureIri() {
        return exposureIri;
    }

    public String getFormattedValue() {
        if (value > 1) {
            return String.format("%.0f %s", value, unit);
        } else {
            return String.format("%.3g %s", value, unit);
        }
    }
}
