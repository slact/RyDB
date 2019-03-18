#!/bin/sh
ENABLE_VARS="$ENABLE_VARS rydb-debug|yes|RYDB_DEBUG"
DISABLE_VARS="$DISABLE_VARS rydb-debug|0|RYDB_DEBUG"
ENABLE_RYDB_DEBUG_DOC="enable RYDB_DEBUG code for development & debugging"
DISABLE_RYDB_DEBUG_DOC="disable RYDB_DEBUG code for development & debugging"

WITH_VARS="$WITH_VARS build-type|Debug|CMAKE_BUILD_TYPE"
WITH_CMAKE_BUILD_TYPE_DOC="cmake build type (Debug,Release,RelWithDebInfo)"

WITH_VARS="$WITH_VARS libatomic_ops-path||LIBATOMIC_PATH"
WITH_LIBATOMIC_PATH_DOC="path to libatomic_ops"
