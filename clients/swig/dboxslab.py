# This file was automatically generated by SWIG (http://www.swig.org).
# Version 3.0.12
#
# Do not make changes to this file unless you know what you are doing--modify
# the SWIG interface file instead.

from sys import version_info as _swig_python_version_info
if _swig_python_version_info >= (3, 0, 0):
    new_instancemethod = lambda func, inst, cls: _dboxslab.SWIG_PyInstanceMethod_New(func)
else:
    from new import instancemethod as new_instancemethod
if _swig_python_version_info >= (2, 7, 0):
    def swig_import_helper():
        import importlib
        pkg = __name__.rpartition('.')[0]
        mname = '.'.join((pkg, '_dboxslab')).lstrip('.')
        try:
            return importlib.import_module(mname)
        except ImportError:
            return importlib.import_module('_dboxslab')
    _dboxslab = swig_import_helper()
    del swig_import_helper
elif _swig_python_version_info >= (2, 6, 0):
    def swig_import_helper():
        from os.path import dirname
        import imp
        fp = None
        try:
            fp, pathname, description = imp.find_module('_dboxslab', [dirname(__file__)])
        except ImportError:
            import _dboxslab
            return _dboxslab
        try:
            _mod = imp.load_module('_dboxslab', fp, pathname, description)
        finally:
            if fp is not None:
                fp.close()
        return _mod
    _dboxslab = swig_import_helper()
    del swig_import_helper
else:
    import _dboxslab
del _swig_python_version_info

try:
    _swig_property = property
except NameError:
    pass  # Python < 2.2 doesn't have 'property'.

try:
    import builtins as __builtin__
except ImportError:
    import __builtin__

def _swig_setattr_nondynamic(self, class_type, name, value, static=1):
    if (name == "thisown"):
        return self.this.own(value)
    if (name == "this"):
        if type(value).__name__ == 'SwigPyObject':
            self.__dict__[name] = value
            return
    method = class_type.__swig_setmethods__.get(name, None)
    if method:
        return method(self, value)
    if (not static):
        object.__setattr__(self, name, value)
    else:
        raise AttributeError("You cannot add attributes to %s" % self)


def _swig_setattr(self, class_type, name, value):
    return _swig_setattr_nondynamic(self, class_type, name, value, 0)


def _swig_getattr(self, class_type, name):
    if (name == "thisown"):
        return self.this.own()
    method = class_type.__swig_getmethods__.get(name, None)
    if method:
        return method(self)
    raise AttributeError("'%s' object has no attribute '%s'" % (class_type.__name__, name))


def _swig_repr(self):
    try:
        strthis = "proxy of " + self.this.__repr__()
    except __builtin__.Exception:
        strthis = ""
    return "<%s.%s; %s >" % (self.__class__.__module__, self.__class__.__name__, strthis,)


def _swig_setattr_nondynamic_method(set):
    def set_attr(self, name, value):
        if (name == "thisown"):
            return self.this.own(value)
        if hasattr(self, name) or (name == "this"):
            set(self, name, value)
        else:
            raise AttributeError("You cannot add attributes to %s" % self)
    return set_attr



def initialize(qsize: 'int') -> "void":
    """
    初始化内部工作线程，不调用将不可用
    @param qsize: 内部工作线程数
    """
    return _dboxslab.initialize(qsize)

def finalize() -> "void":
    """退出内部工作线程，调用后将不可用"""
    return _dboxslab.finalize()
class VFileStat(object):
    thisown = _swig_property(lambda x: x.this.own(), lambda x, v: x.this.own(v), doc='The membership flag')
    __repr__ = _swig_repr
    size = _swig_property(_dboxslab.VFileStat_size_get, _dboxslab.VFileStat_size_set)
    mtime = _swig_property(_dboxslab.VFileStat_mtime_get, _dboxslab.VFileStat_mtime_set)

    def __init__(self):
        _dboxslab.VFileStat_swiginit(self, _dboxslab.new_VFileStat())
    __swig_destroy__ = _dboxslab.delete_VFileStat
VFileStat_swigregister = _dboxslab.VFileStat_swigregister
VFileStat_swigregister(VFileStat)

class VFile(object):
    """虚拟文件对象类"""

    thisown = _swig_property(lambda x: x.this.own(), lambda x, v: x.this.own(v), doc='The membership flag')
    __repr__ = _swig_repr

    def __init__(self):
        """初始化空白虚拟文件对象类，需要使用 Client.Open 创建。"""
        _dboxslab.VFile_swiginit(self, _dboxslab.new_VFile())

    def isValid(self) -> "bool":
        """当前文件对象是否有效果"""
        return _dboxslab.VFile_isValid(self)


    def name(self) -> "std::string const":
        """当前文件对象的名称"""
        return _dboxslab.VFile_name(self)


    def message(self) -> "std::string const":
        """当前文件对象的最后错误信息"""
        return _dboxslab.VFile_message(self)


    def Read(self, size: 'unsigned int', offset: 'unsigned int') -> "PyObject *":
        """
        客户端串行读取数据，线程内不安全
        Read(size: 'unsigned int', offset: 'unsigned int') -> 'bytes'
        """
        return _dboxslab.VFile_Read(self, size, offset)


    def Read2(self, size: 'unsigned int', offset: 'unsigned int') -> "PyObject *":
        """
        客户端分块并行读取数据，线程内不安全
        Read2(size: 'unsigned int', offset: 'unsigned int') -> 'bytes'
        """
        return _dboxslab.VFile_Read2(self, size, offset)


    def Write(self, buffer: 'char const *', offset: 'unsigned int', async_write: 'bool'=False) -> "ssize_t":
        """
        客户端串行写入数据，线程内安全
        Write(buffer: 'bytes', offset: 'unsigned int', async_write: 'bool'=False) -> 'ssize_t'
        """
        return _dboxslab.VFile_Write(self, buffer, offset, async_write)


    def Truncate(self, newsize: 'unsigned int') -> "int":
        """
        截取文件
        Truncate(newsize: 'unsigned int') -> 'int'
        @param newsize: 新文件长度
        """
        return _dboxslab.VFile_Truncate(self, newsize)


    def GetAttr(self) -> "VFileStat *":
        """
        获取文件长度和修改时间
        GetAttr() -> 'VFileStat'
        """
        return _dboxslab.VFile_GetAttr(self)


    def Flush(self) -> "int":
        """
        刷新远程块缓存服务器的修改块 和 后端存储
        Flush() -> 'int'
        """
        return _dboxslab.VFile_Flush(self)

    __swig_destroy__ = _dboxslab.delete_VFile
VFile.isValid = new_instancemethod(_dboxslab.VFile_isValid, None, VFile)
VFile.name = new_instancemethod(_dboxslab.VFile_name, None, VFile)
VFile.message = new_instancemethod(_dboxslab.VFile_message, None, VFile)
VFile.Read = new_instancemethod(_dboxslab.VFile_Read, None, VFile)
VFile.Read2 = new_instancemethod(_dboxslab.VFile_Read2, None, VFile)
VFile.Write = new_instancemethod(_dboxslab.VFile_Write, None, VFile)
VFile.Truncate = new_instancemethod(_dboxslab.VFile_Truncate, None, VFile)
VFile.GetAttr = new_instancemethod(_dboxslab.VFile_GetAttr, None, VFile)
VFile.Flush = new_instancemethod(_dboxslab.VFile_Flush, None, VFile)
VFile_swigregister = _dboxslab.VFile_swigregister
VFile_swigregister(VFile)

class Client(object):
    """分布式虚拟缓存客户端类"""

    thisown = _swig_property(lambda x: x.this.own(), lambda x, v: x.this.own(v), doc='The membership flag')
    __repr__ = _swig_repr

    def __init__(self, hostinfo: 'char const *', timeout: 'int'=30):
        """
        Client(hostinfo: 'string', timeout: 'int'=30)
        初始化 Client 对象
        @param hostinfo: 服务地址，可以为 127.0.0.1:8000 IP地址或 /tmp/dboxslab.sock 本地 socket 文件
        @param timeout: 服务调用超时时间
        """
        _dboxslab.Client_swiginit(self, _dboxslab.new_Client(hostinfo, timeout))

    def Open(self, filename: 'char const *', mode: 'char const *'=None) -> "VFileHelper *":
        """
        Open(filename: 'string', mode: 'string') -> 'VFile'
        新建或打开一个文件对象进行读写
        @param filename: 文件名称
        @param mode: r 只读模式，w 修改模式
        """
        return _dboxslab.Client_Open(self, filename, mode)


    def Unlink(self, filename: 'char const *') -> "int":
        """
        Unlink(filename: 'string') -> 'int'
        删除文件
        @param filename: 文件名称
        """
        return _dboxslab.Client_Unlink(self, filename)


    def MkDir(self, filename: 'char const *') -> "int":
        """
        MkDir(filename: 'string') -> 'int'
        建立目录
        @param filename: 文件名称
        """
        return _dboxslab.Client_MkDir(self, filename)


    def RmDir(self, filename: 'char const *') -> "int":
        """
        RmDir(filename: 'string') -> 'int'
        删除目录
        @param filename: 文件名称
        """
        return _dboxslab.Client_RmDir(self, filename)


    def GetAttr(self, filename: 'char const *') -> "VFileStat *":
        """
        GetAttr(filename: 'string') -> 'VFileStat'
        文件属性（ mtime 和 size ）
        @param filename: 文件名称
        """
        return _dboxslab.Client_GetAttr(self, filename)


    def Truncate(self, filename: 'char const *', newsize: 'unsigned int') -> "int":
        """
        Truncate(filename: 'string', newsize: 'unsigned int') -> 'int'
        截取文件
        @param filename: 文件名称
        @param newsize: 新文件长度
        """
        return _dboxslab.Client_Truncate(self, filename, newsize)


    def Close(self, filename: 'char const *') -> "int":
        """
        Close(filename: 'string') -> 'int'
        关闭缓存信息
        @param filename: 文件名称
        """
        return _dboxslab.Client_Close(self, filename)

    __swig_destroy__ = _dboxslab.delete_Client
Client.Open = new_instancemethod(_dboxslab.Client_Open, None, Client)
Client.Unlink = new_instancemethod(_dboxslab.Client_Unlink, None, Client)
Client.MkDir = new_instancemethod(_dboxslab.Client_MkDir, None, Client)
Client.RmDir = new_instancemethod(_dboxslab.Client_RmDir, None, Client)
Client.GetAttr = new_instancemethod(_dboxslab.Client_GetAttr, None, Client)
Client.Truncate = new_instancemethod(_dboxslab.Client_Truncate, None, Client)
Client.Close = new_instancemethod(_dboxslab.Client_Close, None, Client)
Client_swigregister = _dboxslab.Client_swigregister
Client_swigregister(Client)





