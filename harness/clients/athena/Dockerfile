FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml .
RUN mvn -q dependency:go-offline
COPY src src
RUN mvn -q -DskipTests package

FROM public.ecr.aws/lambda/java:21
ENV JAVA_TOOL_OPTIONS="--add-opens=java.base/java.nio=ALL-UNNAMED -Dio.netty.tryReflectionSetAccessible=true"
COPY --from=build /src/target/athena-0.1.0.jar ${LAMBDA_TASK_ROOT}/lib/
CMD ["mink.athena.Handler::handleRequest"]
