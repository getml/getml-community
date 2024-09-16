# syntax=docker/dockerfile:1.4

ARG OUTPUT_DIR
ARG VERSION
ARG PACKAGE_NAME="getml-community-$VERSION-$TARGETARCH-$TARGETOS"

FROM --platform=$BUILDPLATFORM golang:1.22 AS cli-build
ARG TARGETOS
ARG TARGETARCH
ARG VERSION
WORKDIR /cli/src
COPY src/getml-app/src .

RUN --mount=type=cache,target=/go/pkg/mod/ \
    GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -o /cli/build/getml-cli -ldflags "-X main.version=$VERSION" . \
    && mkdir /cli/release \
    && cp /cli/build/getml-cli /cli/release/getml-cli

FROM scratch AS cli
ARG PACKAGE_NAME
COPY --from=cli-build /cli/release/getml-cli $PACKAGE_NAME/getML

FROM scratch AS export
ARG PACKAGE_NAME
COPY --from=cli / .
COPY --from=engine-package . .
COPY LICENSE.txt INSTALL.md $PACKAGE_NAME
COPY src/package-build-imports $PACKAGE_NAME

FROM --platform=$BUILDPLATFORM python:3.11-slim AS python-base
WORKDIR /python-api/src
RUN pip install hatch
COPY src/python-api/ .
COPY VERSION ./getml/VERSION

FROM python-base AS python-base-arm64
ARG WHEEL_PLATFORM="manylinux_2_28_aarch64"
# HACK: Only build the binaryless "any" wheel on one branch to avoid
# multiplatform artifact collision
RUN hatch build -t wheel

FROM python-base AS python-base-amd64
ARG WHEEL_PLATFORM="manylinux_2_28_x86_64"

FROM python-base-$TARGETARCH AS python-build
ARG OUTPUT_DIR
ARG PACKAGE_NAME
RUN mkdir -p /python-api/src/getml/.getML
COPY --from=export /$PACKAGE_NAME ./getml/.getML
RUN hatch build -t wheel

FROM scratch AS python
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
