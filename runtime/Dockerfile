ARG VERSION
ARG PACKAGE_NAME="getml-$VERSION-$TARGETARCH-linux"
ARG ARTIFACT_DIR="build"

FROM debian:bookworm-slim AS runtime
ARG PACKAGE_NAME
ARG ARTIFACT_DIR="build"
ARG PORT_MIN=1708
ARG PORT_MAX=1733
ENV PORT_MIN=$PORT_MIN
ENV PORT_MAX=$PORT_MAX

RUN apt-get update && apt-get install -y socat

WORKDIR /home/getml
RUN mkdir .getML

COPY $ARTIFACT_DIR/$PACKAGE_NAME .

COPY runtime/entrypoint.sh .

RUN useradd getml
RUN chown -R getml:getml /home/getml
USER getml

WORKDIR /home/getml
RUN ./getML install

ENTRYPOINT ["./entrypoint.sh"]
