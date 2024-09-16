ARG VERSION="0.0.0"
ARG PACKAGE_NAME="getml-community-$VERSION-$TARGETARCH-linux"
ARG OUTPUT_DIR="build"

FROM debian:bookworm-slim AS runtime
ARG PACKAGE_NAME
ARG OUTPUT_DIR
ARG PORT_MIN=1707
ARG PORT_MAX=1733
ENV PORT_MIN=$PORT_MIN
ENV PORT_MAX=$PORT_MAX

RUN apt-get update && apt-get install -y socat

WORKDIR /usr/local/getML

COPY --from=engine-package /$PACKAGE_NAME ./package
RUN ./package/getML install

COPY runtime/entrypoint.sh .

WORKDIR /home/getml

RUN useradd getml
RUN chown -R getml:getml /home/getml
USER getml

ENTRYPOINT ["/usr/local/getML/entrypoint.sh"]
