# syntax=docker/dockerfile:1

# First stage: prepare production dependencies and build plugins before copying source
#==================================================================================================
FROM maven:3.9.6-eclipse-temurin-17 AS retriever

# Copy in Maven settings templates and credentials 
COPY docker/credentials /root/credentials
COPY docker/.m2 /root/.m2

# Populate settings templates with credentials
WORKDIR /root/.m2
# (Note that | rather than / is used as the sed delimiter, since encrypted passwords can contain the latter, but not the former
RUN sed -i "s|MASTER_PASSWORD|$(mvn --encrypt-master-password master_password)|" settings-security.xml
RUN sed -i "s|REPO_USERNAME|$(cat ../credentials/repo_username.txt)|;s|REPO_PASSWORD|$(cat ../credentials/repo_password.txt|xargs mvn --encrypt-password)|" settings.xml

# Source changes do not invalidate this dependency preparation layer.
WORKDIR /root/code

COPY pom.xml ./pom.xml
RUN --mount=type=cache,id=fia-m2-cache,target=/root/.m2/repository,sharing=locked \
    mvn -B dependency:go-offline -P prod-profile -DskipTests

#==================================================================================================

# Second stage: build war file
#==================================================================================================

FROM retriever AS builder

# Copy in Java source and build war
WORKDIR /root/code

COPY src ./src/
COPY WEB-INF ./WEB-INF/

# Reuse the same Maven cache. Allow missing artifacts to be fetched, but do not
# force snapshot updates on every source edit. Cache mounts are builder-local.
RUN --mount=type=cache,id=fia-m2-cache,target=/root/.m2/repository,sharing=locked \
    mvn -B package -DskipTests -P prod-profile

#==================================================================================================

# Third stage: copy the built WAR into the runtime image
#==================================================================================================
FROM tomcat:9 AS agent

WORKDIR /app

# Copy the compiled WAR from the builder
COPY --from=builder /root/code/output/* $CATALINA_HOME/webapps/
COPY ./docker/entrypoint.sh entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]
