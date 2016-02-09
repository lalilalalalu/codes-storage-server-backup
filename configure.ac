#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.67])
AC_INIT([codes-storage-server], [0.0.1])

AC_CANONICAL_TARGET
AC_CANONICAL_SYSTEM
AC_CANONICAL_HOST

AM_INIT_AUTOMAKE([foreign subdir-objects -Wall])

m4_ifdef([AM_SILENT_RULES], [AM_SILENT_RULES([yes])])

AC_CONFIG_SRCDIR([README])

# Checks for programs.
AC_PROG_CC
AM_PROG_CC_C_O
AC_PROG_CXX
AC_PROG_CXXCPP
AC_PROG_RANLIB

# Check for C99
AC_PROG_CC_C99

PKG_PROG_PKG_CONFIG

dnl Check to see if CC is an MPI compiler
AC_MSG_CHECKING(whether the mpicc compiler works)
AC_TRY_COMPILE([#include <mpi.h>], [int ret = MPI_Init(0, (void*)0)],
    AC_MSG_RESULT(yes),
    AC_MSG_RESULT(no)
    AC_MSG_ERROR([CC doesnt appear to be a valid MPI compiler.  See INSTALL
                  document or try adding CC=mpicc to your configure command
                  line.])
)

# check for codes-net (codes-base is implicit)
PKG_CHECK_MODULES_STATIC([CODES], [codes], [],
                  [AC_MSG_ERROR([Could not find working codes installation via pkg-config])])

AC_CONFIG_FILES([Makefile])

AC_OUTPUT([maint/codes-storage-server.pc])
