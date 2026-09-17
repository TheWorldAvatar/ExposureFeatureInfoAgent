package cares.cam.ac.uk;

public class Config {
    public static final String KEYCLOAK_SERVER = System.getenv("KEYCLOAK_SERVER");
    public static final String KEYCLOAK_REALM = System.getenv("KEYCLOAK_REALM");
    public static final String DATABASE = System.getenv().getOrDefault("DATABASE", "postgres");
    public static final String NAMESPACE = System.getenv("NAMESPACE");
}
