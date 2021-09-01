cmake_minimum_required (VERSION 2.8)

project (raft)

set (CMAKE_CXX_FLAGS "-std=c++2a -Wall -O2")

set (RAFT "${PROJECT_SOURCE_DIR}/src")

set (SRC
    ${RAFT}/rpc.cc
    ${RAFT}/raft.cc
    ${RAFT}/config.cc
    ${RAFT}/logs.cc
)

set (HEADERS
    ${RAFT}/rpc.h
    ${RAFT}/raft.h
    ${RAFT}/config.h
    ${RAFT}/logs.h
    ${RAFT}/service.h
    ${RAFT}/util.h
)

add_library(angel STATIC IMPORTED)
set_target_properties(angel PROPERTIES IMPORTED_LOCATION "/usr/local/lib/libangel.a")

add_library (raft STATIC ${SRC})

install(TARGETS raft
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib)

install(FILES ${HEADERS} DESTINATION include/raft)

