include local.mk
CC=chpl
INCLUDES=-I$(BLAS_HOME)/include -I$(POSTGRES_HOME)
LIBS=-L${BLAS_HOME}/lib -lblas
SRCDIR=src
BINDIR=target
MODULES=-M$(CDO_HOME)/src -M$(NUMSUCH_HOME)/src -M$(CHARCOAL_HOME)/src
EXEC=cdoExtras

default: all

all: $(SRCDIR)/CdoExtras.chpl
	$(CC) $(INCLUDES) $(LIBS) $(MODULES) -o $(BINDIR)/$(EXEC) $<

run:
	./$(BINDIR)/$(EXEC)

run-test: test/CdoExtrasTest.chpl
	$(CC) $(INCLUDES) $(LIBS) $(MODULES) -M$(SRCDIR) -o test/test $< ; \
	./test/test -f test/db_creds.txt ; \
	rm test/test
