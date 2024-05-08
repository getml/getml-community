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

FROM scratch AS engine
ARG PACKAGE_NAME
COPY --from=engine-build /$PACKAGE_NAME/bin $PACKAGE_NAME/bin
COPY --from=engine-build /$PACKAGE_NAME/lib $PACKAGE_NAME/lib

FROM scratch AS export
ARG PACKAGE_NAME
COPY --from=cli / .
COPY --from=engine / .
COPY LICENSE.txt $PACKAGE_NAME
COPY src/package-build-imports $PACKAGE_NAME

FROM python:3.11-slim AS wheel-base-arm64
ARG WHEEL_PLATFORM="manylinux_2_28_aarch64"

FROM python:3.11-slim AS wheel-base-amd64
ARG WHEEL_PLATFORM="manylinux_2_28_x86_64"

FROM wheel-base-$TARGETARCH AS wheel-build
ARG PACKAGE_NAME
ARG VERSION
WORKDIR /wheel/src
COPY src/python-api/ .
COPY VERSION ./getml/VERSION
# the binaries are not present yet, so the general `any` wheel will contain no binaries
RUN pip wheel --no-deps --wheel-dir /wheel/dist .
RUN mkdir -p /wheel/src/getml/.getML
COPY --from=export /$PACKAGE_NAME ./getml/.getML
# now the binaries are present, we set the platform accordingly
RUN pip wheel --no-deps --wheel-dir /wheel/dist \
    --config-settings --build-option=--plat-name \
    --config-settings --build-option=$WHEEL_PLATFORM .

FROM scratch AS wheel
ARG PACKAGE_NAME
COPY --from=wheel-build /wheel/dist $PACKAGE_NAME/wheel

FROM scratch AS package
COPY --from=export / .
COPY --from=wheel / .
