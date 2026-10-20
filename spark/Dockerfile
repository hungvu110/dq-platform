# Build context: thư mục gốc repo (docker-compose.yml: build.context = .)

# ---------- Stage 1: build dq-platform.jar (client Deequ + job Scala) bằng Maven, chạy cả test ----------
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /src
# test của dq-client / dq-jobs parse suite thật trong suites/
COPY suites/ suites/
COPY jvm/ jvm/
RUN --mount=type=cache,target=/root/.m2 mvn -B -q -f jvm/pom.xml package

# ---------- Stage 2: image dùng chung cho driver + executor (job Python và job Scala) ----------
FROM apache/spark:3.5.1-scala2.12-java17-python3-ubuntu
USER root
# spark-defaults.conf đặt spark.jars=/opt/dq/jars/dq-platform.jar -> job Python cũng có client JVM.
# Không cần pip install gì thêm: Deequ chạy trong JVM, Python chỉ gọi qua py4j.
COPY --from=build /src/jvm/dq-jobs/target/dq-platform.jar /opt/dq/jars/dq-platform.jar
# Mount point của volume `lake` (docker-compose): tạo sẵn với chủ là spark để volume mới thừa hưởng quyền,
# nếu không Docker tạo nó là root và job (chạy bằng user spark) không ghi được
RUN mkdir -p /data/lake && chown spark:spark /data/lake
USER spark
WORKDIR /opt/dq
