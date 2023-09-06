# 如何使用tpcc-generator
## STEP 1
在当前目录下make, 获得tpcc-generator二进制程序
## STEP 2
**./tpcc-generator warehouse_count**

比如./tpcc-generator 5就是生成仓库数量为5的tpcc测试数据

```zsh
make
./tpcc-generator 1
```

## 注意
因为sxy老师给的csv文件第一行是属性名，所以必须把csv文件的第一行去掉，因此tpcc-generator生成的所有csv文件的第一行都会是空行