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
        self.requires(
            "boost/1.85.0",
            override=True,
        )
        self.requires(
            "arrow/16.1.0",
            options={
                "parquet": True,
                "with_boost": True,
                "with_thrift": True,
                "boost/*:without_cobalt": True,
                "boost/*:without_locale": True,
                "boost/*:without_atomic": True,
                "boost/*:without_charconv": True,
                "boost/*:without_chrono": True,
                "boost/*:without_cobalt": True,
                "boost/*:without_container": True,
                "boost/*:without_context": True,
                "boost/*:without_contract": True,
                "boost/*:without_coroutine": True,
                "boost/*:without_date_time": True,
                "boost/*:without_exception": True,
                "boost/*:without_fiber": True,
                "boost/*:without_graph": True,
                "boost/*:without_graph_parallel": True,
                "boost/*:without_iostreams": True,
                "boost/*:without_json": True,
                "boost/*:without_locale": True,
                "boost/*:without_log": True,
                "boost/*:without_math": True,
                "boost/*:without_mpi": True,
                "boost/*:without_nowide": True,
                "boost/*:without_program_options": False,
                "boost/*:without_python": True,
                "boost/*:without_random": True,
                "boost/*:without_regex": True,
                "boost/*:without_serialization": True,
                "boost/*:without_stacktrace": True,
                "boost/*:without_test": True,
                "boost/*:without_thread": True,
                "boost/*:without_timer": True,
                "boost/*:without_type_erasure": True,
                "boost/*:without_url": True,
                "boost/*:without_wave": True,
                "boost/*:without_filesystem": True,
                "boost/*:without_system": True,
                "boost/*:header_only": False,
                # "boost/*:asio_no_deprecated": True,
                "boost/*:diagnostic_definitions": False,
                "boost/*:error_code_header_only": True,
                # "boost/*:filesystem_no_deprecated": True,
                # "boost/*:filesystem_use_std_fs": True,
                "boost/*:header_only": False,
                "boost/*:i18n_backend_icu": False,
                "boost/*:magic_autolink": False,
                "boost/*:multithreading": True,
                "boost/*:namespace_alias": False,
                "boost/*:segmented_stacks": False,
                "boost/*:shared": True,
                # "boost/*:system_no_deprecated": True,
                # "boost/*:system_use_utf8": False,
                "boost/*:with_stacktrace_backtrace": False,
            },
        )
        self.requires("poco/1.13.3")
        self.requires("mariadb-connector-c/3.3.3", options={"shared": True})
        self.requires("libpq/15.4")
        self.requires("eigen/3.4.0")
        self.requires("sqlite3/3.45.0")
        if self.settings.os == "Linux":
            self.requires("gperftools/2.15")
            self.requires("libunwind/1.8.1")
        self.requires("reflect-cpp/0.13.0")
        self.requires("xgboost/2.0.3")
        self.requires("range-v3/0.12.0")
        self.requires("gtest/1.15.0")

    def generate(self):
        toolchain = CMakeToolchain(self, generator="Ninja")
        toolchain.user_presets_path = False
        toolchain.generate()
        deps = CMakeDeps(self)
        deps.generate()

    def build_requirements(self):
        self.build_requires("cmake/[>=3.25.0]")
        self.build_requires("ninja/[>=1.11.0]")
