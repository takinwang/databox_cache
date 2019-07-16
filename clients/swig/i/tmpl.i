/* dboxslab.i */
 
%module dboxslab
 
%include "typemaps.i"

%include "std_string.i"

%include "exception.i" 
 
%{ 
 
#define SWIG_FILE_WITH_INIT

#include "dboxslab_swig.hpp"
 
%} 

%exception {
	try {
	$function
	} 
	catch (dbox_error & e) {
	 	PyErr_SetString(PyExc_RuntimeError, e.what());
		SWIG_fail;
	} catch(...) {
		PyErr_SetString(PyExc_RuntimeError, "Unknown exception");
		SWIG_fail;
	} 	
}; 

%feature("kwargs");
  
%feature("docstring", "初始化内部工作线程，不调用将不可用\n@param qsize: 内部工作线程数") initialize;
void initialize(int qsize) ;

%feature("docstring", "退出内部工作线程，调用后将不可用") finalize;
void finalize();

struct VFileStat {
	unsigned int size  ;
	unsigned int mtime  ;
};

%typemap(in) (const char *buffer, unsigned int size) {
   if ( PyString_Check($input) ){
   	  SWIG_exception_fail(SWIG_ArgError(-1), "in method '" "VFile_Write" "', argument " "2"" of type '" "unsigned int""'");
   } 
   $1 = PyString_AsString($input);
   $2 = PyString_Size($input);
}

#%typemap(out) VFileHelper * {  
#	$result = SWIG_NewPointerObj(SWIG_as_voidptr( $1 ), SWIGTYPE_p_VFileHelper, SWIG_POINTER_NEW |  0 ); 
#}

%rename ( VFile ) VFileHelper;
%feature("docstring", "虚拟文件对象类") VFileHelper;
class VFileHelper {
public:
	
	%feature("docstring", "初始化空白虚拟文件对象类，需要使用 Client.Open 创建。") VFileHelper;
	VFileHelper();

	%feature("docstring", "当前文件对象是否有效果") isValid;
	bool isValid();
	
	%feature("docstring", "当前文件对象的名称") name;
	const std::string name() ;

	%feature("docstring", "当前文件对象的最后错误信息") message;
	const std::string message();
						
%extend { 
	
	%feature("docstring", "客户端串行读取数据，线程内不安全\nRead(size: 'unsigned int', offset: 'unsigned int') -> 'bytes'") Read;	
	PyObject * Read(unsigned int size, unsigned int offset){
		char * ptr = NULL;
		ssize_t bytes = self->Read(ptr, size, offset);
		return SWIG_FromCharPtrAndSize( ptr, bytes );		
	} 
		
	%feature("docstring", "客户端分块并行读取数据，线程内不安全\nRead2(size: 'unsigned int', offset: 'unsigned int') -> 'bytes'") Read2; 
	PyObject * Read2(unsigned int size, unsigned int offset){
		char * ptr = NULL;
		ssize_t bytes = self->Read2(ptr, size, offset); 
		return SWIG_FromCharPtrAndSize( ptr, bytes ); 
	} 
	
	%feature("docstring", "客户端串行写入数据，线程内安全\nWrite(buffer: 'bytes', offset: 'unsigned int', async_write: 'bool'=False) -> 'ssize_t'") Write;	
	ssize_t Write(const char * buffer, unsigned int size, unsigned int offset, bool async_write = false){
		ssize_t bytes = self->Write( buffer, size, offset, async_write);
		return bytes;
	}
	
}  

	%feature("docstring", "截取文件\nTruncate(newsize: 'unsigned int') -> 'int'\n@param newsize: 新文件长度") Truncate;
	int Truncate(unsigned int newsize);
	
	%newobject GetAttr; 
	%feature("docstring", "获取文件长度和修改时间\nGetAttr() -> 'VFileStat'") GetAttr;
	VFileStat * GetAttr() ;
	
	%feature("docstring", "刷新远程块缓存服务器的修改块 和 后端存储\nFlush() -> 'int'") Flush;
	int Flush();
	
};

%rename ( Client ) ClientHelper;
%feature("docstring", "分布式虚拟缓存客户端类") ClientHelper;
class ClientHelper {
public: 

	%feature("docstring", "Client(hostinfo: 'string', timeout: 'int'=30)\n初始化 Client 对象\n@param hostinfo: 服务地址，可以为 127.0.0.1:8000 IP地址或 /tmp/dboxslab.sock 本地 socket 文件\n@param timeout: 服务调用超时时间") ClientHelper;
	ClientHelper( const char * hostinfo, int timeout = 30 );

	%newobject Open; 
	%feature("docstring", "Open(filename: 'string', mode: 'string') -> 'VFile'\n新建或打开一个文件对象进行读写\n@param filename: 文件名称\n@param mode: r 只读模式，w 修改模式") Open;
	VFileHelper * Open(const char * filename, const char * mode = NULL);
	
	%feature("docstring", "Unlink(filename: 'string') -> 'int'\n删除文件\n@param filename: 文件名称") Unlink;
	int Unlink(const char * filename);

	%feature("docstring", "MkDir(filename: 'string') -> 'int'\n建立目录\n@param filename: 文件名称") MkDir;
	int MkDir(const char * filename);

	%feature("docstring", "RmDir(filename: 'string') -> 'int'\n删除目录\n@param filename: 文件名称") RmDir;
	int RmDir(const char * filename);
	
	%newobject GetAttr; 
	%feature("docstring", "GetAttr(filename: 'string') -> 'VFileStat'\n文件属性（ mtime 和 size ）\n@param filename: 文件名称") GetAttr;
	VFileStat * GetAttr(const char * filename);

	%feature("docstring", "Truncate(filename: 'string', newsize: 'unsigned int') -> 'int'\n截取文件\n@param filename: 文件名称\n@param newsize: 新文件长度") Truncate;
	int Truncate(const char * filename, unsigned int newsize);

	%feature("docstring", "Close(filename: 'string') -> 'int'\n关闭缓存信息\n@param filename: 文件名称") Close;
	int Close(const char * filename);
};

