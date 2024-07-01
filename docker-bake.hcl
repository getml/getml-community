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
  output = ["build"]
  args = {
    ENGINE_REPO_SOURCE="src/engine",
    VERSION="${VERSION}"
  }
}

target "package" {
  contexts = {
    engine-build = "target:engine"
  }
  target = "export"
  output = ["build"]
  args = {
    VERSION="${VERSION}"
  }
}

target "python" {
  contexts = {
    engine-build = "target:engine"
  }
  target = "python"
  output = ["build"]
  args = {
    VERSION="${VERSION}"
  }
}

target "archive" {
  contexts = {
    engine-build = "target:engine"
  }
  target = "archive"
  output = ["build"]
  args = {
    VERSION="${VERSION}"
  }
}

target "all" {
  contexts = {
    engine-build = "target:engine"
  }
  target = "all"
  output = ["build"]
  args = {
    VERSION="${VERSION}"
  }
}
