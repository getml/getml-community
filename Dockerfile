# syntax=docker/dockerfile:1.4

ARG VERSION
ARG PACKAGE_NAME="getml-$VERSION-$TARGETARCH-$TARGETOS"

FROM golang:1.22 AS cli-build
WORKDIR /cli/src
COPY src/getml-app/src .

RUN --mount=type=cache,target=/go/pkg/mod/ \
    go build -o /cli/build/getml-cli . \
    && mkdir /cli/release \
    && cp /cli/build/getml-cli /cli/release/getml-cli

FROM scratch AS cli
ARG PACKAGE_NAME
COPY --from=cli-build /cli/release/getml-cli $PACKAGE_NAME/getML

FROM scratch AS export
ARG PACKAGE_NAME
COPY --from=cli / .
COPY --from=engine-build . .
COPY LICENSE.txt $PACKAGE_NAME
COPY src/package-build-imports $PACKAGE_NAME

FROM python:3.11-slim AS python-base-arm64
ARG WHEEL_PLATFORM="manylinux_2_28_aarch64"

FROM python:3.11-slim AS python-base-amd64
ARG WHEEL_PLATFORM="manylinux_2_28_x86_64"

FROM python-base-$TARGETARCH AS python-build
ARG PACKAGE_NAME
ARG VERSION
WORKDIR /python-api/src
RUN pip install hatch
COPY src/python-api/ .
COPY VERSION ./getml/VERSION
# the binaries are not present yet, so the general `any` wheel will contain no binaries
RUN hatch build
RUN mkdir -p /python-api/src/getml/.getML
COPY --from=export /$PACKAGE_NAME ./getml/.getML
# now the binaries are present, the platform tag is set accordingly by hatch
RUN hatch build -t wheel

FROM scratch AS python
ARG PACKAGE_NAME
COPY --from=python-build python-api/src/dist python-api

FROM alpine AS archive-build
ARG PACKAGE_NAME
COPY --from=export / /$PACKAGE_NAME
RUN mkdir /out
RUN tar czf /out/$PACKAGE_NAME.tar.gz $PACKAGE_NAME

FROM scratch AS archive
COPY --from=archive-build /out/ .

FROM scratch AS all
COPY --from=export / .
COPY --from=python / .
COPY --from=archive / .
