from conan import ConanFile
from conan.tools.cmake import CMakeToolchain, CMake, cmake_layout, CMakeDeps
from conan.tools.files import get


class ReflectCppConan(ConanFile):
    name = "reflect-cpp"
    package_type = "library"
    description = "C++-20 library for fast serialization, deserialization and validation using reflection"
    license = "MIT"
    url = "https://github.com/getml/reflect-cpp"
    topics = (
        "reflection",
        "serialization",
        "memory",
        "json",
        "xml",
        "flatbuffers",
        "yaml",
        "toml",
        "msgpack",
        "header-only",
    )
    settings = "os", "arch", "compiler", "build_type"
    options = {"shared": [True, False], "fPIC": [True, False]}
    default_options = {"shared": False, "fPIC": True}
    implements = ["auto_shared_fpic"]
    generators = "CMakeDeps", "CMakeToolchain"

    def requirements(self):
        self.requires("yyjson/0.10.0", transitive_headers=True, transitive_libs=True)
        self.requires("ctre/3.9.0", transitive_headers=True)

    def layout(self):
        cmake_layout(self)

    def build(self):
        cmake = CMake(self)
        cmake.configure(
            variables={
                "REFLECTCPP_USE_BUNDLED_DEPENDENCIES": "OFF",
                "REFLECTCPP_BUILD_BENCHMARKS": "OFF",
                "REFLECTCPP_BUILD_TESTS": "OFF",
            }
        )
        cmake.build()

    def package(self):
        cmake = CMake(self)
        cmake.install()

    def package_info(self):
        self.cpp_info.libs = ["reflectcpp"]

    def source(self):
        get(self, **self.conan_data["sources"][self.version], strip_root=True)
