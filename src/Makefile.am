include $(top_srcdir)/common.mk

$(shell thrift -I /usr/share/ --gen cpp ./scribe.thrift)
$(shell mv ./gen-cpp/* ./scribe/gen-cpp; rmdir gen-cpp)
SCRIBE_SRC=scribe/ScribeHandler.cpp scribe/gen-cpp/scribe.cpp scribe/gen-cpp/scribe_types.cpp
SCRIBE_INCLUDES= -I/usr/include/thrift -I/usr/include/thrift/fb303

if SS_BOOST_SYSTEM
SCRIBE_LIBS= -lthrift -lthriftnb -lfb303 -levent -lboost_system
else
SCRIBE_LIBS= -lthrift -lthriftnb -lfb303 -levent 
endif

XML_SRC=xml/SXml.cpp
XML_INCLDUES= -Ixml -I/usr/include/libxml2
XML_LIBS= -lxml2

AM_CPPFLAGS+=-Ibasetool -ggdb -Wno-deprecated

lib_LTLIBRARIES=libspiderutils.la
libspiderutils_la_LIBADD=-lACE -lpthread -lcrypto -lz
libspiderutils_la_SOURCES= \
	    basetool/Configuration.cpp basetool/debug.cpp

if SCRIBE
libspiderutils_la_SOURCES+= $(SCRIBE_SRC)
libspiderutils_la_LIBADD += $(SCRIBE_LIBS)
AM_CPPFLAGS+= $(SCRIBE_INCLUDES)
endif

if XML
libspiderutils_la_SOURCES+=$(XML_SRC)
libspiderutils_la_LIBADD += $(XML_LIBS)
AM_CPPFLAGS+=$(XML_INCLDUES)
endif

libspiderutils_la_SOURCES+= ${QDB_SRC}
libspiderutils_la_LIBADD += $(QDB_LIBS)
AM_CPPFLAGS+= $(QDB_INCLUDES)

AM_CFLAGS+=-Werror -Wall
AM_CXXFLAGS+=-Werror -Wall
AM_LDFLAGS+=-version-number=4:4:$(SVNVERSION)

AUTOMAKE_OPTIONS=subdir-objects
