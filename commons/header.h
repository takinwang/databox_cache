/*
 * header.h
 *
 *  Created on: Oct 15, 2018
 *      Author: root
 */

#ifndef HEADER_H_
#define HEADER_H_

#include <string>
#include <stdlib.h>
#include <stdexcept>

#define	tsFailed   -1000 //失败
#define	tsSuccess   0 //成功

#define MAX_REQUEST_SIZE ( 8 * 1024 * 1024 ) //单次请求的最大数据长度

#define FAILED_INVALID_MEMORY      "Invalid memory"
#define FAILED_INVALID_ARGUMENT    "Invalid argument"
#define FAILED_INVALID_RESPONSE    "Invalid response"
#define FAILED_INTERNAL_ERROR      "Internal Error"
#define FAILED_CONNECTION_TIMEOUT  "Connection timeout"
#define FAILED_CONNECTION_FAILED   "Connection failed"

#define MEM_PREFIX   "mem://" //内存文件前缀

enum CacheAdminAction {

	caaClearFiles = 10 //清除缓存文件

};

enum CacheAction {
	caSuccess = 0,             //通用返回
	caOpen = 1,                //打开缓存文件
	caClose = 2,               //关闭缓存文件

	caClientRead2 = 9,         //并行读取缓存数据，支持 c++ 库
	caClientRead = 10,         //读取缓存数据
	caClientReadResp = 11,     //读取数据返回

	caClientWrite = 12,        //写入数据到缓存
	caClientWriteResp = 13,    //写入数据返回

	caClientUnlink = 14,       //客户端删除文件
	caClientUnlinkResp = 15,   //客户端删除文件返回

	caClientMkDir = 16,        //客户端建立目录
	caClientMkDirResp = 17,    //客户端建立目录返回

	caClientRmDir = 18,        //客户端删除目录
	caClientRmDirResp = 19,    //客户端删除目录返回

	caClientTruncate = 20,     //客户端截取文件
	caClientTruncateResp = 21, //客户端截取文件返回

	caClientGetAttr = 22,      //客户端读取文件属性
	caClientGetAttrResp = 23,  //客户端读取文件属性返回

	caClientFlush = 24,        //客户端刷新写入数据
	caClientFlushResp = 25,    //客户端刷新写入数据返回

	caSlabStatus = 30,         //获取节点信息，例如块数量、利用数等，主节点定时主动块节点获取统计信息，块节点反馈信息，彼此通讯确认块节点是否与主节点连通
	caSlabStatusResp = 31,     //返回节点信息，主节点定时主动块节点获取统计信息，块节点反馈信息，彼此通讯确认块节点是否与主节点连通

	caSlabRemovePeer = 32,     //删除一个数据块对象所在节点信息
	caSlabRemovePeerResp = 33, //删除一个数据块对象所在节点信息返回

	caSlabGetMeta = 34,        //获取数据信息
	caSlabGetMetaResp = 35,    //获取数据信息返回

	caSlabPutMeta = 36,        //修改数据信息
	caSlabPutMetaResp = 37,    //修改数据信息返回

	caSlabPeerRead = 38,       //邻居读取缓存
	caSlabPeerReadResp = 39,   //邻居读取缓存返回

	caSlabPutAttr = 40,        //修改文件属性
	caSlabPutAttrResp = 41,    //修改文件属性返回

	caSlabGetAttr = 42,        //获取文件属性
	caSlabGetAttrResp = 43,    //获取文件属性返回

	caMasterCheckIt = 44,      //主节点主动检查数据
	caMasterCheckItResp = 45   //主节点主动检查数据返回

};

struct FileStat {
	time_t mtime { 0 };
	off_t size { 0 };
};

#endif /* HEADER_H_ */
