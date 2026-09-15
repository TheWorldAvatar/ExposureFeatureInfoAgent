package cares.cam.ac.uk;

import java.net.URI;
import java.security.interfaces.RSAPublicKey;
import java.util.concurrent.TimeUnit;

import com.auth0.jwk.JwkProvider;
import com.auth0.jwk.JwkProviderBuilder;
import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.interfaces.DecodedJWT;

/** Keycloak verification matching trip-agent: RS256, issuer, expiry and subject. */
class TimelineAuthentication {
    private JwkProvider keys;
    private String issuer;

    private synchronized void configure() throws Exception {
        if (keys != null) {
            return;
        }
        if (Config.KEYCLOAK_SERVER == null || Config.KEYCLOAK_SERVER.isBlank()
                || Config.KEYCLOAK_REALM == null || Config.KEYCLOAK_REALM.isBlank()) {
            throw new IllegalStateException("Keycloak authentication is not configured");
        }
        issuer = Config.KEYCLOAK_SERVER.replaceAll("/+$", "") + "/realms/" + Config.KEYCLOAK_REALM;
        keys = new JwkProviderBuilder(URI.create(issuer + "/protocol/openid-connect/certs").toURL())
                .cached(10, 24, TimeUnit.HOURS).rateLimited(10, 1, TimeUnit.MINUTES)
                .timeouts(5000, 5000).build();
    }

    String authenticate(String header) {
        if (header == null || !header.regionMatches(true, 0, "Bearer ", 0, 7)
                || header.substring(7).isBlank()) {
            throw new AuthenticationException("Bearer token is missing");
        }
        try {
            configure();
            String token = header.substring(7).strip();
            DecodedJWT unverified = JWT.decode(token);
            if (!"RS256".equals(unverified.getAlgorithm()) || unverified.getKeyId() == null) {
                throw new IllegalArgumentException("Unsupported token signing header");
            }
            RSAPublicKey key = (RSAPublicKey) keys.get(unverified.getKeyId()).getPublicKey();
            DecodedJWT verified = JWT.require(Algorithm.RSA256(key, null))
                    .withIssuer(issuer).withClaimPresence("exp").withClaimPresence("sub")
                    .build().verify(token);
            String subject = verified.getSubject();
            if (subject == null || subject.isBlank()) {
                throw new IllegalArgumentException("Missing subject");
            }
            return subject;
        } catch (Exception e) {
            throw new AuthenticationException("Invalid bearer token or Keycloak authentication unavailable", e);
        }
    }

    static class AuthenticationException extends RuntimeException {
        AuthenticationException(String message) { super(message); }
        AuthenticationException(String message, Throwable cause) { super(message, cause); }
    }
}
