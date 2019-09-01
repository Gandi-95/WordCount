package com.imooc.spark

import java.io.RandomAccessFile

import com.ggstar.util.ip.IpHelper
import org.lionsoul.ip2region.{DataBlock, DbConfig, DbSearcher}

object IPUtiles {
  def getCity(ip:String) = {
    IpHelper.findRegionByIp(ip)
  }

  def getIPData(ip:String) = {
    //根据ip进行位置信息搜索
    val config = new DbConfig();

    //获取ip库的位置（放在src下）（直接通过测试类获取文件Ip2RegionTest为测试类）
    val dbfile = getClass.getClassLoader.getResource("ip2region.db").getPath
    val searcher = new DbSearcher(config, dbfile);
    //采用Btree搜索
    val block = searcher.btreeSearch(ip);
    block.getRegion()
  }

  def main(args: Array[String]): Unit = {
    println(getCity("218.22.9.56"))
    println(getIPData("218.22.9.56"))
  }

}
