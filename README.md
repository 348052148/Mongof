# Mongof
一个php新版mongodb驱动过渡工具，支持php老版mongodb常用操作。用新版mongodb驱动实现老版mongo操作

## 使用方式
MongoDB 类中的构造方法里。配置项内容如下
```javascript
array(
            'dsn' => 'mongodb://host1:3717,host2:3717,host3:3717' ,//连接
            'option'=> array(
                'connect' => true,//是否使用同一个连接，默认是true
                //'connectTimeoutMS'=>8000,//连接超时时间（以毫秒为单位）
                'replicaSet'=>'replicaSet', //复制集名字
                //'readPreference' => \MongoClient::RP_NEAREST,//采用这个模式的话，读操作会发生在网络延时小的节点，不考虑读取的数据是过时的还是最新的。
                'username'=>'root',//用户名
                'password'=>'123456',//用户密码
                'db'=>'admin',//用户库
            ),
            'use_db'=>'selfDb',//选择需要操作的库
        )
```
