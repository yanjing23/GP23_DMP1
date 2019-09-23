package com.tags
//标签实现接口.定义方法进行实现
trait tag {
 def makeTags(args:Any*):List[(String,Int)]
}
