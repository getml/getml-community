variable "GETML_VERSION" {
  default = "0.0.0"
}

variable "GETML_BUILD_OUTPUT_DIR" {
  default = "build"
}

variable "GETML_CMAKE_FRESH_PRESET" {
  default = "false"
}

target "cli" {
  args = {
    VERSION="${GETML_VERSION}"
  }
  output = ["type=local,dest=${GETML_BUILD_OUTPUT_DIR},platform-split=false"]
  target = "cli"
}

target "engine" {
  args = {
    CMAKE_FRESH_PRESET="${GETML_CMAKE_FRESH_PRESET}"
    ENGINE_REPO_SOURCE="src/engine",
    OUTPUT_DIR="${GETML_BUILD_OUTPUT_DIR}"
    VERSION="${GETML_VERSION}"
  }
  dockerfile = "src/engine/Dockerfile"
  output = ["type=local,dest=${GETML_BUILD_OUTPUT_DIR},platform-split=false"]
  target = "package"
}

target "package" {
  args = {
    CMAKE_FRESH_PRESET="${GETML_CMAKE_FRESH_PRESET}"
    OUTPUT_DIR="${GETML_BUILD_OUTPUT_DIR}"
    VERSION="${GETML_VERSION}"
  }
  # for naming build contexts, we use the following convention:
  # <dockerfile-default-context>-<default-foreign-target-stage>
  # i.e. the context 'engine-package' by default points to the 'package' target
  # inside the engine dockerfile, the context 'monorepo-export' by default
  # points to the 'export' target inside the monorepo (root) dockerfile
  contexts = {
    engine-package = "target:engine"
  }
  output = ["type=local,dest=${GETML_BUILD_OUTPUT_DIR},platform-split=false"]
  target = "export"
}

target "docker" {
  args = {
    BUILD_OR_COPY_ARTIFACTS="build"
    CMAKE_FRESH_PRESET="${GETML_CMAKE_FRESH_PRESET}"
    OUTPUT_DIR="${GETML_BUILD_OUTPUT_DIR}"
    VERSION="${GETML_VERSION}"
  }
  contexts = {
    monorepo-export = "target:package"
  }
  dockerfile = "runtime/Dockerfile"
  output = ["type=docker"]
  tags = ["getml/getml:${GETML_VERSION}", "getml/getml:latest"]
}

target "docker-copy-artifacts" {
  args = {
    BUILD_OR_COPY_ARTIFACTS="copy"
    OUTPUT_DIR="${GETML_BUILD_OUTPUT_DIR}"
    VERSION="${GETML_VERSION}"
  }
  dockerfile = "runtime/Dockerfile"
  output = ["type=docker"]
  tags = ["getml/getml:${GETML_VERSION}", "getml/getml:latest"]
}

target "python" {
  args = {
    BUILD_OR_COPY_ARTIFACTS="build"
    OUTPUT_DIR="${GETML_BUILD_OUTPUT_DIR}"
    VERSION="${GETML_VERSION}"
  }
  contexts = {
    engine-package = "target:engine"
  }
  output = ["type=local,dest=${GETML_BUILD_OUTPUT_DIR},platform-split=false"]
  target = "python"
}

target "python-copy-artifacts" {
  args = {
    BUILD_OR_COPY_ARTIFACTS="copy"
    OUTPUT_DIR="${GETML_BUILD_OUTPUT_DIR}"
    VERSION="${GETML_VERSION}"
  }
  output = ["type=local,dest=${GETML_BUILD_OUTPUT_DIR},platform-split=false"]
  target = "python"
}

target "archive-copy-artifacts" {
  args = {
    BUILD_OR_COPY_ARTIFACTS="copy"
    OUTPUT_DIR="${GETML_BUILD_OUTPUT_DIR}"
    VERSION="${GETML_VERSION}"
  }
  output = ["type=local,dest=${GETML_BUILD_OUTPUT_DIR},platform-split=false"]
  target = "archive"
}

target "archive" {
  args = {
    CMAKE_FRESH_PRESET="${GETML_CMAKE_FRESH_PRESET}"
    OUTPUT_DIR="${GETML_BUILD_OUTPUT_DIR}"
    VERSION="${GETML_VERSION}"
  }
  contexts = {
    engine-package = "target:engine"
  }
  output = ["type=local,dest=${GETML_BUILD_OUTPUT_DIR},platform-split=false"]
  target = "archive"
}

target "all" {
  args = {
    CMAKE_FRESH_PRESET="${GETML_CMAKE_FRESH_PRESET}"
    OUTPUT_DIR="${GETML_BUILD_OUTPUT_DIR}"
    VERSION="${GETML_VERSION}"
  }
  contexts = {
    engine-package = "target:engine"
  }
  output = ["type=local,dest=${GETML_BUILD_OUTPUT_DIR},platform-split=false"]
  target = "all"
}
