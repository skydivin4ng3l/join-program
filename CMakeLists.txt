#cmake_minimum_required(VERSION 3.12)
project(join-program)

set(CMAKE_CXX_STANDARD 14)

#add_subdirectory(src)
add_executable(${PROJECT_NAME} src/main.cpp
        src/Relation.h
        src/Relation.cpp
        src/BlockReader.h
        src/BlockReader.cpp
        src/Block.h
        src/Block.cpp
        src/Tuple.h
        src/Tuple.cpp
        src/utils.h
        src/utils.cpp
        src/NestedLoopEquiJoinAlgorithm.h
        src/NestedLoopEquiJoinAlgorithm.cpp
        src/SimpleSortBasedTwoPassEquiJoinAlgorithm.h
        src/SimpleSortBasedTwoPassEquiJoinAlgorithm.cpp
        src/MemoryManager.h
        src/MemoryManager.cpp
        )
