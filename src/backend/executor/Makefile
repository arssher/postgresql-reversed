#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for executor
#
# IDENTIFICATION
#    src/backend/executor/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/executor
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

OBJS = execAmi.o execGrouping.o execIndexing.o execJunk.o \
       execMain.o execProcnode.o execQual.o \
       execScan.o execTuples.o \
       execUtils.o functions.o instrument.o nodeAgg.o nodeCustom.o \
       nodeHash.o nodeHashjoin.o \
       nodeLimit.o \
       nodeSeqscan.o nodeSubplan.o \
       nodeWindowAgg.o tstoreReceiver.o tqueue.o spi.o

include $(top_srcdir)/src/backend/common.mk
