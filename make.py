import os, sys 


def error():
    sys.stderr.write(sys.argv[0] + " install|build|clean\n")


def debug(line):
    sys.stdout.write(line + "\n")


def run(cmd):
    debug(cmd)
    return os.system(cmd)

    
def main() :
    if len(sys.argv) < 2:
        error()
        return 
    
    current_path = os.path.realpath(os.curdir)
    build_path = os.path.join(current_path, "build")
    action = sys.argv[1]
    
    debug("Current path: " + current_path)
    
    cmd = []
    if action == "build":
        cmd.append('mkdir -p %s' % build_path)  
        cmd.append('cd %s' % build_path)
        cmd.append('cmake -DCMAKE_EXPORT_COMPILE_COMMANDS:BOOL=ON --warn-unitialized --warn-unused -G "Unix Makefiles" %s' % current_path)
        cmd.append('make')
        cmd.append('')
        return run("\n".join(cmd))
    
    if action == "rebuild":
        if not os.path.exists(build_path) :
            debug("Path not exist: " + build_path)
            return 
        cmd.append('cd %s' % build_path)
        cmd.append('make rebuild_cache')
        cmd.append('make')        
        cmd.append('')
        return run("\n".join(cmd)) 
        
    if action == "clean" :
        if not os.path.exists(build_path) :
            debug("Path not exist: " + build_path)
            return 
        cmd.append('rm -rf %s' % build_path) 
        cmd.append('')
        return run("\n".join(cmd)) 

    if action == "install" :
        if not os.path.exists(build_path) :
            debug("Path not exist: " + build_path)
            return 
        cmd.append('cd %s' % build_path)
        cmd.append('cmake -DCMAKE_EXPORT_COMPILE_COMMANDS:BOOL=ON --warn-unitialized --warn-unused -G "Unix Makefiles" %s' % current_path)        
        cmd.append('make')        
        cmd.append('make install')
        cmd.append('')            
        return run("\n".join(cmd)) 
    
    if action == "uninstall" :
        manifest = os.path.join(build_path, "install_manifest.txt")
        if not os.path.exists(manifest) :
            debug("File not exist: " + manifest)
            return 
        
        with open(manifest) as f:
            lines = filter(lambda a: a.strip(), f.readlines())
            for aline in lines : 
                run("rm -rf %s" % aline) 
        return 
    
    error()

        
if __name__ == "__main__":
    main()
