package com.util

object rpt {

  def caculateReq(reqMode:Int,prcNode:Int)={

    if(reqMode == 1 && prcNode == 1){
      List[Double](1,0,0)
    }else if(reqMode == 1 && prcNode == 2) {
      List[Double](1,1,0)
    }else if(reqMode == 1 && prcNode == 3) {
      List[Double](1,1,1)
    } else {
      List[Double](0,0,0)
    }
  }
  def caculaeRtb(effive:Int,bill: Int,bid: Int,orderId: Int,win: Int,winPrice:Double,adPayMent:Double):List[Double]= {


    if (effive == 1 && bill == 1 && bid == 1) {
      if (effive == 1 && bill == 1 && win == 1 && orderId != 0) {
        List[Double](1, 1, winPrice / 1000.0, adPayMent / 1000.0)
      } else {
        List[Double](1, 0, 0, 0)
      }
    }else
      {
        List[Double](0, 0, 0, 0)
      }
    }


  def caculateShowClick(reqMode:Int,effive:Int):List[Double]={

   //广告展示,点击
    if(reqMode == 2 && effive == 1) {
      List[Double](1,0)
    }else if(reqMode == 3 && effive == 1){
      List[Double](0,1)
    } else  List[Double](0,0)

  }


}
