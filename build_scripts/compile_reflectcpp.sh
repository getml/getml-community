function compile_reflectcpp() {
    export NUM_THREADS=$(nproc --all)
    cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
    cmake --build build -j$NUM_THREADS  
}
