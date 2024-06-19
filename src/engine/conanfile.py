from conan import ConanFile

from conan.tools.cmake import CMakeToolchain, CMakeDeps

required_conan_version = ">=2.3.0"

class Engine(ConanFile):
    settings = "os", "compiler", "build_type", "arch"
    build_policy = "missing"

    name = "engine"
    version = "1.5.0"
    description = "getml engine"
    license = "Elastic License 2.0 (ELv2)"
    homepage = "https://www.getml.com"
    url = "https://github.com/getml/getml-community/"

    def requirements(self):
        self.requires("boost/1.84.0", override=True)
        self.requires(
            "arrow/16.1.0",
            options={
                "parquet": True,
                "with_boost": True,
                "with_thrift": True,
            })
        self.requires("poco/1.13.3")
        self.requires("mariadb-connector-c/3.3.3", options={"shared": True})
        self.requires("libpq/15.4")
        self.requires("eigen/3.4.0")
        self.requires("sqlite3/3.45.0")
        self.requires("gperftools/2.15")
        self.requires("libunwind/1.8.1")
        self.requires("reflect-cpp/0.11.0", options={"with_json": True})
        self.requires("xgboost/2.0.3")

    def generate(self):
        toolchain = CMakeToolchain(self, generator="Ninja")
        toolchain.user_presets_path = False
        toolchain.generate()
        deps = CMakeDeps(self)
        deps.generate()

    def build_requirements(self):
        self.build_requires("cmake/[>=3.25.0]")
        self.build_requires("ninja/[>=1.11.0]")
