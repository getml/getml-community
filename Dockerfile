ARG OUTPUT_DIR
ARG VERSION
ARG PACKAGE_NAME="getml-community-$VERSION-$TARGETARCH-$TARGETOS"
ARG BUILD_OR_COPY_ARTIFACTS="build"

FROM --platform=$BUILDPLATFORM golang:1.22 AS cli-build
ARG TARGETOS
ARG TARGETARCH
ARG VERSION
WORKDIR /cli/src
COPY src/getml-app/src .

RUN --mount=type=cache,target=/go/pkg/mod/ \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -o /cli/build/getml-cli -ldflags "-X main.version=$VERSION" . \
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
ARG TARGETPLATFORM
ARG BUILDPLATFORM
ARG PACKAGE_NAME
ARG VERSION
WORKDIR /python-api/src
RUN pip install hatch
COPY src/python-api/ .
RUN echo $VERSION > getml/VERSION
RUN hatch build -t wheel
RUN mkdir -p /python-api/src/getml/.getML

FROM python-base AS python-base-arm64
ARG WHEEL_PLATFORM="manylinux_2_28_aarch64"
# HACK: Remove the any platform wheel on one of the branches
# to avoid multi-platform artifact name collision
RUN if [ "$TARGETPLATFORM" != "$BUILDPLATFORM" ]; then rm -rf dist/*.whl; fi

FROM python-base AS python-base-amd64
ARG WHEEL_PLATFORM="manylinux_2_28_x86_64"
RUN if [ "$TARGETPLATFORM" != "$BUILDPLATFORM" ]; then rm -rf dist/*.whl; fi

FROM python-base-$TARGETARCH AS python-build-artifacts
COPY --from=export /$PACKAGE_NAME getml/.getML/$PACKAGE_NAME

FROM python-base-$TARGETARCH AS python-copy-artifacts
ARG OUTPUT_DIR
COPY $OUTPUT_DIR/$PACKAGE_NAME getml/.getML/$PACKAGE_NAME

FROM python-${BUILD_OR_COPY_ARTIFACTS}-artifacts AS python-build
RUN hatch build -t wheel

FROM scratch AS python
COPY --from=python-build python-api/src/dist python-api

FROM alpine AS archive-build
ARG PACKAGE_NAME
COPY --from=export / /
RUN mkdir /out
RUN tar czf /out/$PACKAGE_NAME.tar.gz $PACKAGE_NAME
WORKDIR /out
RUN sha256sum $PACKAGE_NAME.tar.gz > $PACKAGE_NAME.tar.gz.sha256

FROM scratch AS archive
COPY --from=archive-build /out/ .

FROM scratch AS all
COPY --from=export / .
COPY --from=python / .
COPY --from=archive / .
