#
# This file is open source software, licensed to you under the terms
# of the Apache License, Version 2.0 (the "License").  See the NOTICE file
# distributed with this work for additional information regarding copyright
# ownership.  You may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

#
# Copyright (C) 2020 Scylladb, Ltd.
#

find_package (PkgConfig REQUIRED)

pkg_search_module (uring_PC liburing)

find_library (uring_LIBRARY
  NAMES uring
  HINTS
    ${uring_PC_LIBDIR}
    ${uring_PC_LIBRARY_DIRS})

find_path (uring_INCLUDE_DIR
  NAMES liburing.h
  HINTS
    ${uring_PC_INCLUDEDIR}
    ${uring_PC_INCLUDEDIRS})

mark_as_advanced (
  uring_LIBRARY
  uring_INCLUDE_DIR)

include (FindPackageHandleStandardArgs)

find_package_handle_standard_args (uring
  REQUIRED_VARS
    uring_LIBRARY
    uring_INCLUDE_DIR
  VERSION_VAR uring_PC_VERSION)

set (uring_LIBRARIES ${uring_LIBRARY})
set (uring_INCLUDE_DIRS ${uring_INCLUDE_DIR})

if (uring_FOUND AND NOT (TARGET uring::uring))
  add_library (uring::uring UNKNOWN IMPORTED)

  set_target_properties (uring::uring
    PROPERTIES
      IMPORTED_LOCATION ${uring_LIBRARY}
      INTERFACE_INCLUDE_DIRECTORIES ${uring_INCLUDE_DIRS})
endif ()
