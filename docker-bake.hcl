variable "VERSION" {
  default = "unknown"
}

target "cli" {
  target = "cli"
  output = ["build"]
  args = {
    VERSION="${VERSION}"
  }
}

target "engine" {
  dockerfile = "src/engine/Dockerfile"
  target = "package"
  args = {
    ENGINE_REPO_SOURCE="src/engine",
    VERSION="${VERSION}"
  }
}

target "package" {
  contexts = {
    engine-build = "target:engine"
  }
  target = "package"
  output = ["build"]
  args = {
    VERSION="${VERSION}"
  }
}

target "wheel" {
  contexts = {
    engine-build = "target:engine"
  }
  target = "wheel"
  output = ["build"]
  args = {
    VERSION="${VERSION}"
  }
}
