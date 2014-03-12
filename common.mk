MOSTLYCLEANFILES=
BUILT_SOURCES=

CFLAGS+=$(if $(DEBUG), -g -O0)
CXXFLAGS+=$(if $(DEBUG), -g -O0)

CPPFLAGS+=$(if $(OPTIMIZE), -DNDEBUG)
CFLAGS+=$(if $(OPTIMIZE), -O3)
CXXFLAGS+=$(if $(OPTIMIZE), -O3)

CFLAGS+=$(if $(PROFILE), -pg)
CXXFLAGS+=$(if $(PROFILE), -pg)

AM_CPPFLAGS=-D'SVNVERSION="$(SVNVERSION)"' -fPIC -I$(top_srcdir)

AM_LDFLAGS=-Wl,-no-undefined
AM_CFLAGS=-Wall -Werror
AM_CXXFLAGS=-Wall -Werror

SS_THRIFT_VERSION=$(strip $(shell thrift -version | awk '{print $$3}' | awk -F'.' '{print $$1*1000+$$2*100+$$3}' | tr -cd [0-9] ))
AM_CXXFLAGS+=-D SS_THRIFT_VERSION=$(SS_THRIFT_VERSION)

_SVNVERSION=$(strip $(subst exported, 0, $(shell svnversion -nc $(srcdir) | sed 's/^.*://' | tr -cd [0-9] )))
SVNVERSION=$(if $(_SVNVERSION),$(_SVNVERSION),0)
CONFIGURE_DEPENDENCIES=$(subst @@@,,$(subst CONFIGURE_DEPENDENCIES,@,@CONFIGURE_DEPENDENCIES@))
CONFIGURE_DEPENDENCIES+=$(top_srcdir)/acsite.m4

collectlib_DIR=$(top_builddir)/_lib
collectbin_DIR=$(top_builddir)/_bin
collectinclude_DIR=$(top_builddir)/_include
include $(top_srcdir)/collect.mk
