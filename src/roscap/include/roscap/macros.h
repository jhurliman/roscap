#pragma once

#include <ros/macros.h> // for the DECL's

// Import/export for windows dll's and visibility for gcc shared libraries.

#ifdef ROS_BUILD_SHARED_LIBS // ros is being built around shared libraries
  #ifdef roscap_EXPORTS // we are building a shared lib/dll
    #define ROSCAP_DECL ROS_HELPER_EXPORT
  #else // we are using shared lib/dll
    #define ROSCAP_DECL ROS_HELPER_IMPORT
  #endif

#else // ros is being built around static libraries
  #define ROSCAP_DECL
  #define ROSCAP_STORAGE_DECL
#endif
