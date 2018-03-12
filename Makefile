include local.mk
CC=chpl
INCLUDES=-I$(BLAS_HOME)/include
LIBS=-L${BLAS_HOME}/lib -lblas
SRCDIR=src
BINDIR=target
MODULES=-M$(CDO_HOME)/src -M$(NUMSUCH_HOME)/src
EXEC=cdoExtras

default: all

all: $(SRCDIR)/CdoExtras.chpl
	$(CC) $(INCLUDES) $(LIBS) $(MODULES) -o $(BINDIR)/$(EXEC) $<

run:
	./$(BINDIR)/$(EXEC)
