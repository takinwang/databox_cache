
PYTHON = /usr/bin/python3
CAT    = /usr/bin/cat
ECHO   = /usr/bin/echo
SWIG   = /usr/bin/swig
MKDIR  = /usr/bin/mkdir

all: clean build install
     
clean: 
	@echo
	@echo "Cleaning Make files"
	@echo "=================="
	@echo
	
	$(RM) build -r
	$(RM) dboxslab.egg-info -r
	$(RM) dist -r
	$(RM) dboxslab_wrap.cxx -r
	
	@echo

build: 
	@echo 
	@echo "Building Make files"
	@echo "=================="
	@echo
		
	$(RM) dboxslab_wrap.cxx -r
	
	$(CAT) ./i/tmpl.i                              >  dboxslab.i 	
	$(ECHO) "%pythoncode %{"                       >> dboxslab.i 	
	$(CAT) ./i/tmpl.py                             >> dboxslab.i	
	$(ECHO) "%}"                                   >> dboxslab.i
	
	$(SWIG) -c++ -python -O -py3 dboxslab.i

	@echo
		
rebuild: 
	@echo 
	@echo "Rebuilding Make files"
	@echo "=================="
	@echo
	
	$(RM) build -r
	$(RM) dboxslab.egg-info -r	
	$(RM) dist -r
	
	$(RM) dboxslab_wrap.cxx -r
	
	$(CAT) ./i/tmpl.i                              >  dboxslab.i 	
	$(ECHO) "%pythoncode %{"                       >> dboxslab.i 	
	$(CAT) ./i/tmpl.py                             >> dboxslab.i	
	$(ECHO) "%}"                                   >> dboxslab.i
	
	$(SWIG) -c++ -python -O -py3 dboxslab.i
	
	@echo
	
install: 
	@echo 
	@echo "Install Make files"
	@echo "=================="
	@echo  
	
	$(PYTHON) setup.py install --prefix=/usr
	
	@echo	 
	
uninstall: 	
	@echo
	@echo "Cleaning installed files"
	@echo "=================="
	@echo
	
	$(MKDIR) -p    /usr/local/lib64/python3.6/site-packages/
	$(RM) -r       /usr/local/lib64/python3.6/site-packages/dboxslab*
	
	$(MKDIR) -p    /usr/lib64/python3.6/site-packages/
	$(RM) -r       /usr/lib64/python3.6/site-packages/dboxslab*
	
	@echo	 
    