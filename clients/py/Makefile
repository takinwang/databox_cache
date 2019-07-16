
all: install clean
	
install: 
	@echo 
	@echo "Install dboxmr extension"
	@echo "=================="
	@echo
	
	$(RM) -rf  /usr/lib/python3.6/site-packages/dboxmr* 
	$(RM) -rf  /usr/local/lib/python3.6/site-packages/dboxmr*
	
	python3 setup.py install --prefix=/usr
	
uninstall: 	
	@echo
	@echo "Cleaning installed files"
	@echo "=================="
	@echo
	$(RM) -rf  /usr/lib/python3.6/site-packages/dboxmr* 
	$(RM) -rf  /usr/local/lib/python3.6/site-packages/dboxmr*
    
    
clean: 
	@echo
	@echo "Cleaning cached files"
	@echo "=================="
	@echo
	$(RM) -rf build dist dboxmr.egg-info __pycache__