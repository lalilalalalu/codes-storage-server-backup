AUTOMAKE_OPTIONS = foreign
ACLOCAL_AMFLAGS = -I m4

bin_PROGRAMS =
bin_SCRIPTS =
noinst_LIBRARIES =
noinst_PROGRAMS =
lib_LIBRARIES =
noinst_HEADERS =
TESTS =
XFAIL_TESTS =
check_PROGRAMS =
EXTRA_PROGRAMS =
CLEANFILES =
EXTRA_DIST = prepare.sh
BUILT_SOURCES =

# pkgconfig files
pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA = maint/codes-storage-server.pc
$(pkgconfig_DATA): config.status

AM_CPPFLAGS = ${CODES_NET_CFLAGS}

AM_CFLAGS =

AM_LDFLAGS =

AM_CXXFLAGS = $(AM_CFLAGS)

# all programs/tests use the libraries here
LDADD = $(lib_LIBRARIES) ${CODES_NET_LIBS}

include Make.rules

include $(top_srcdir)/codes/Makefile.subdir
include $(top_srcdir)/src/Makefile.subdir
include $(top_srcdir)/tests/Makefile.subdir
