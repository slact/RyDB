# - Try to find libatomic_ops
# Once done this will define
#  LIBATOMIC_OPS_FOUND - System has libatomic_ops
#  LIBATOMIC_OPS_INCLUDE_DIRS - The libatomic_ops include directories
#  LIBATOMIC_OPS_LIBRARIES - The libraries needed to use libatomic_ops
#  LIBATOMIC_OPS_DEFINITIONS - Compiler switches required for using libatomic_ops

find_path(LIBATOMIC_OPS_INCLUDE_DIR atomic_ops
          HINTS ${PC_LIBATOMIC_OPS_INCLUDEDIR} ${PC_LIBATOMIC_OPS_INCLUDE_DIRS}
         )

find_library(LIBATOMIC_OPS_LIBRARY NAMES atomic_ops
             HINTS ${PC_LIBATOMIC_OPS_LIBDIR} ${PC_LIBATOMIC_OPS_LIBRARY_DIRS} )

include(FindPackageHandleStandardArgs)
# handle the QUIETLY and REQUIRED arguments and set LIBATOMI_OPS_FOUND to TRUE
# if all listed variables are TRUE
find_package_handle_standard_args(atomic_ops DEFAULT_MSG
                                  LIBATOMIC_OPS_LIBRARY LIBATOMIC_OPS_INCLUDE_DIR)

mark_as_advanced(LIBATOMIC_OPS_INCLUDE_DIR LIBATOMIC_OPS_LIBRARY)
