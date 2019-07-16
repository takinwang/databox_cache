
PYTHON = /usr/bin/python

all: build install
     
clean: 
	@echo
	@echo "Cleaning CMake files"
	@echo "=================="
	@echo
	$(PYTHON) make.py clean 
	@echo

build: 
	@echo 
	@echo "Building CMake files"
	@echo "=================="
	@echo
	$(PYTHON) make.py build 
	@echo
		
rebuild: 
	@echo 
	@echo "Building CMake files"
	@echo "=================="
	@echo
	$(PYTHON) make.py rebuild 
	@echo
	
install: 
	@echo 
	@echo "Install CMake files"
	@echo "=================="
	@echo 
	$(PYTHON) make.py install 
	@echo	 
	
uninstall: 	
	@echo
	@echo "Cleaning installed files"
	@echo "=================="
	@echo
	$(PYTHON) make.py uninstall 
	@echo	 
    