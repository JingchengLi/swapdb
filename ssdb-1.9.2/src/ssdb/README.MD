#ssdb 存储部分内部接口

#约定

###返回值  
ret < 0 错误, 错误信息从GetErrorInfo中获得  
ret == 0 item不存在, 或者操作数量为0  
ret == 1 item存在, 或者操作成功  
ret > 0 变动总数, 原则上不建议使用返回值表示  


###函数命名  
函数名称 redis命令保持一致, 其余内部接口采用驼峰式
函数参数 

###类名
强制驼峰命名 BackendDump SortedSet



#支持情况
dbsize 不准确  
msetnx 不支持,暂时没有需求  
