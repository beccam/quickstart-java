cmake_minimum_required(VERSION 2.6.4)

project(quickstart)

include(FindPackageHandleStandardArgs)

# C/C++ Apache Cassandra driver dependency
set(_LIBCASSANDRA_ROOT_PATHS "${PROJECT_SOURCE_DIR}/libs/cassandra/")
set(_LIBCASSANDRA_ROOT_HINTS ${LIBCASSANDRA_ROOT_DIR} $ENV{LIBCASSANDRA_ROOT_DIR})
if(NOT WIN32)
  set(_LIBCASSANDRA_ROOT_PATHS "${_LIBCASSANDRA_ROOT_PATHS}" "/usr/" "/usr/local/")
endif()
set(_LIBCASSANDRA_ROOT_HINTS_AND_PATHS HINTS
  HINTS ${_LIBCASSANDRA_ROOT_HINTS}
  PATHS ${_LIBCASSANDRA_ROOT_PATHS})

find_path(LIBCASSANDRA_INCLUDE_DIR
  NAMES cassandra.h
  HINTS ${_LIBCASSANDRA_INCLUDEDIR} ${_LIBCASSANDRA_ROOT_HINTS_AND_PATHS}
  PATH_SUFFIXES include)
find_library(LIBCASSANDRA_LIBRARY
  NAMES cassandra cassandra_static libcassandra libcassandra_static
  HINTS ${_LIBCASSANDRA_LIBDIR} ${_LIBCASSANDRA_ROOT_HINTS_AND_PATHS}
  PATH_SUFFIXES lib)

# Libuv dependency
set(_LIBUV_ROOT_PATHS "${PROJECT_SOURCE_DIR}/libs/libuv/")
set(_LIBUV_ROOT_HINTS ${LIBUV_ROOT_DIR} $ENV{LIBUV_ROOT_DIR})
if(NOT WIN32)
  set(_LIBUV_ROOT_PATHS "${_LIBUV_ROOT_PATHS}" "/usr/" "/usr/local/")
endif()
set(_LIBUV_ROOT_HINTS_AND_PATHS HINTS
  HINTS ${_LIBUV_ROOT_HINTS}
  PATHS ${_LIBUV_ROOT_PATHS})

find_path(LIBUV_INCLUDE_DIR
  NAMES uv.h
  HINTS ${_LIBUV_INCLUDEDIR} ${_LIBUV_ROOT_HINTS_AND_PATHS}
  PATH_SUFFIXES include)
find_library(LIBUV_LIBRARY
  NAMES uv uv_a libuv libuv_a
  HINTS ${_LIBUV_LIBDIR} ${_LIBUV_ROOT_HINTS_AND_PATHS}
  PATH_SUFFIXES lib)
find_package_handle_standard_args(Libuv "Could NOT find libuv, try to set the path to the libuv root folder in the system variable LIBUV_ROOT_DIR"
  LIBUV_LIBRARY
  LIBUV_INCLUDE_DIR)

find_package_handle_standard_args(CASSANDRA
  "Could NOT find the C/C++ Apache Cassandra driver, try to set the path to the C/C++ Apache Cassandra driver root folder in the system variable LIBCASSANDRA_ROOT_DIR"
  LIBCASSANDRA_LIBRARY
  LIBCASSANDRA_INCLUDE_DIR)

# Determine if CASS_STATIC needs to be defined
if(${LIBCASSANDRA_LIBRARY} MATCHES "cassandra_static(\\.a|\\.lib)$")
  add_definitions(-DCASS_STATIC)
endif()

LIST(APPEND INCLUDES ${LIBCASSANDRA_INCLUDE_DIR})
LIST(APPEND LIBRARIES ${LIBCASSANDRA_LIBRARY}
  ${LIBUV_LIBRARY})
if(WIN32)
  # Additional required libraries for libuv
  LIST(APPEND LIBRARIES iphlpapi psapi wsock32 crypt32 ws2_32 userenv)
endif()

# Create the quickstart source example
include_directories(${INCLUDES})
add_executable(quickstart
  src/quickstart.c
  ${LIBCASSANDRA_INCLUDE_DIR}/cassandra.h)
target_link_libraries(quickstart ${LIBRARIES})
source_group("Header Files\\cassandra" FILES ${LIBCASSANDRA_INCLUDE_DIR}/cassandra.h)
