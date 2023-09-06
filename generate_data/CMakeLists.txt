cmake_minimum_required(VERSION 3.10)

# 设置项目名称
project(data_generator)

# 设置C++标准
set(CMAKE_CXX_STANDARD 11)

# 通用源文件
set(COMMON_SOURCE_FILES clock.cpp random.cpp)

# 为data_generator添加源文件
set(DATA_GENERATOR_SOURCE_FILES ${COMMON_SOURCE_FILES} generate_data_file.cpp)

# 为test_generator添加源文件
set(TEST_GENERATOR_SOURCE_FILES ${COMMON_SOURCE_FILES} generate_test_file.cpp tpcc.cpp tpcc.h)

# 指定生成data_generator目标
add_executable(data_generator ${DATA_GENERATOR_SOURCE_FILES})

# 指定生成test_generator目标
add_executable(test_generator ${TEST_GENERATOR_SOURCE_FILES})
