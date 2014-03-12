
# -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_INIT(ScribeTool, 1.0.0, limingxiu@sogou-inc.com)
AC_CONFIG_AUX_DIR(_aux)
AM_INIT_AUTOMAKE([foreign])
AC_CONFIG_HEADER([package_config.h])

# Checks for system
AC_CANONICAL_HOST

# Checks for programs.
AC_LANG(C++)
AC_PROG_CC
AC_PROG_CXX
PKG_PROG_PKG_CONFIG
AC_DISABLE_STATIC
AC_PROG_LIBTOOL

# Checks for libraries.

# Checks for header files.
AC_HEADER_STDC
AC_CHECK_HEADERS([iostream])

AK_STD_FEATURES

#for large file
CPPFLAGS="$CPPFLAGS -D_FILE_OFFSET_BITS=64"

#Build scribe related package or not
AC_ARG_ENABLE([scribe],
        [  --enable-scribe Building scribe related package],
        [case "$enableval" in
          yes) scribe=true ;;
          no)  scribe=false ;;
          *) AC_MSG_ERROR([bad value $enableval for --enable-scribe]);;
        esac],
        [scribe=true])
AM_CONDITIONAL([SCRIBE], [test x$scribe = xtrue])

#Build xml related package or not
AC_ARG_ENABLE([xml],
        [  --enable-xml Building xml related package],
        [case "$enableval" in
          yes) xml=true ;;
          no)  xml=false ;;
          *) AC_MSG_ERROR([bad value $enableval for --enable-xml]);;
        esac],
        [xml=true])
AM_CONDITIONAL([XML], [test x$xml = xtrue])

AC_CHECK_HEADERS(boost/system/error_code.hpp, SS_BOOST_SYSTEM=true)
AM_CONDITIONAL([SS_BOOST_SYSTEM], [test x$SS_BOOST_SYSTEM = xtrue])

#check ace
AK_DETECT_LIB2(ace, [/usr/local/src/ACE* /usr/local/ACE* /opt/ACE*],
               [ace/ACE.h], [ACE], [ACE::init],
               [optional],
               [], [ace])

AC_CACHE_SAVE

AK_CHECK_BUILD_DEPENDENCIES([yes], [$MODULE_ROOT_VALUES])

# finalize
AC_CONFIG_FILES([
        Makefile
        src/Makefile
])
AC_OUTPUT
AK_PATCH_LIBTOOL
