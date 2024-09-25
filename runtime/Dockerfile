ARG VERSION="0.0.0"
ARG PACKAGE_NAME="getml-community-$VERSION-$TARGETARCH-linux"
ARG OUTPUT_DIR="build"
ARG BUILD_OR_COPY_ARTIFACTS="build"

FROM debian:bookworm-slim AS runtime-base
ARG PACKAGE_NAME
ARG OUTPUT_DIR
ARG PORT_MIN=1707
ARG PORT_MAX=1733
ENV PORT_MIN=$PORT_MIN
ENV PORT_MAX=$PORT_MAX

RUN apt-get update && apt-get install -y socat

WORKDIR /usr/local/getML

FROM runtime-base AS runtime-build-artifacts
COPY --from=monorepo-export /$PACKAGE_NAME ./package

FROM runtime-base AS runtime-copy-artifacts
COPY $OUTPUT_DIR/$PACKAGE_NAME ./package

FROM runtime-${BUILD_OR_COPY_ARTIFACTS}-artifacts AS runtime-install
RUN ./package/getML install

COPY runtime/entrypoint.sh .

WORKDIR /home/getml

ENV GETML_ALLOW_PUSH_NOTIFICATIONS=false
ENV GETML_ALLOW_REMOTE_IPS=false
ENV GETML_HOME_DIRECTORY=/home/getml
ENV GETML_HTTP_PORT=1711
ENV GETML_IN_MEMORY=true
ENV GETML_INSTALL=false
ENV GETML_LAUNCH_BROWSER=false
ENV GETML_LOG=false
ENV GETML_PROJECT_DIRECTORY=/home/getml/projects

RUN useradd getml
RUN chown -R getml:getml /home/getml
RUN chown -R getml:getml /usr/local/getML

USER getml

ENTRYPOINT ["/usr/local/getML/entrypoint.sh"]
