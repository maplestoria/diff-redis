# diff-redis

找出两个Redis间缺失的Key

在异常情况导致Redis同步中断的情况下, 经常需要找出缺失的那些Key. 通过这个小程序再配合redis-shake的rump模式, 可以直接将指定的Key同步过去.

## note

- 不支持Cluster, Sentinel
- 不支持比较Value的差异