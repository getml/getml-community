from conan import ConanFile
from conan.tools.cmake import CMake, CMakeDeps, CMakeToolchain, cmake_layout
from conan.tools.files import apply_conandata_patches, export_conandata_patches, get
from conan.tools.scm import Git


class xgboostRecipe(ConanFile):
    name = "xgboost"
    package_type = "library"

    # Optional metadata
    license = "Apache-2.0"
    author = "https://xgboost.ai/"
    url = "https://github.com/dmlc/xgboost"
    description = "Scalable, Portable and Distributed Gradient Boosting (GBDT, GBRT or GBM) Library, for Python, R, Java, Scala, C++ and more. Runs on single machine, Hadoop, Spark, Dask, Flink and DataFlow "
    topics = (
        "machine-learning",
        "gradient-boosting",
        "distributed-computing",
        "gbdt",
        "gbm",
        "xgboost",
    )

    # Binary configuration
    settings = "os", "compiler", "build_type", "arch"
    options = {"shared": [True, False], "fPIC": [True, False]}
    default_options = {"shared": False, "fPIC": True}

    def config_options(self):
        if self.settings.os == "Windows":
            self.options.rm_safe("fPIC")

    def configure(self):
        if self.options.shared:
            self.options.rm_safe("fPIC")

    def source(self):
        Git(self).clone(
            url=self.url,
            target=self.source_folder,
            args=[f"--branch=v{self.version}", "--recurse-submodules"],
        )
        apply_conandata_patches(self)

    def layout(self):
        cmake_layout(self, src_folder="src")

    def generate(self):
        deps = CMakeDeps(self)
        deps.generate()
        tc = CMakeToolchain(self)
        tc.generate()

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()

    def package(self):
        cmake = CMake(self)
        cmake.install()

    def package_info(self):
        self.cpp_info.libs = ["xgboost"]
