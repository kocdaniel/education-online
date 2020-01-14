package com.atguigu.member.service

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.member.bean._
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SaveMode, SparkSession}

object EtlDataService {

  /**
    * 导入网站表基础数据
    *
    * @param sc
    * @param sparkSession
    * @return
    */
  def etlBaseWebSiteLog(sc: SparkContext, sparkSession: SparkSession) = {
    import sparkSession.implicits._ //隐式转换
    sc
      .textFile("/user/atguigu/ods/baswewebsite.log")
      .filter(item => {
        val obj: JSONObject = JSON.parseObject(item)
        obj.isInstanceOf[JSONObject]
      })
      .mapPartitions(partition => {
        partition.map(item => {
          val baseWebSiteLog: BaseWebSiteLog = JSON.parseObject(item, classOf[BaseWebSiteLog])
          baseWebSiteLog
        })
      }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_base_website")
  }

  /**
    * etl用户表数据
    * @param sc
    * @param sparkSession
    * @return
    */
  def etlDwdMember(sc: SparkContext, sparkSession: SparkSession) = {
    import sparkSession.implicits._ //隐式转换
    sc.textFile("/user/atguigu/ods/member.log")
      .filter(item => {
        val obj: JSONObject = JSON.parseObject(item)
        obj.isInstanceOf[JSONObject]
      })
      .mapPartitions(partition => {
      partition.map(item => {
        val dwdMember: DwdMember = JSON.parseObject(item, classOf[DwdMember])
        dwdMember.fullname = dwdMember.fullname.head + "XX"
        dwdMember.phone = dwdMember.phone.substring(0,3) + "****" + dwdMember.phone.substring(8,11)
        dwdMember.password = "******"
        dwdMember
      })
    }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_member")
  }

  /**
    * 导入广告表基础数据
    * @param sc
    * @param sparkSession
    */
  def etlBaseAdLog(sc: SparkContext, sparkSession: SparkSession): Unit ={
    import sparkSession.implicits._ //隐式转换
    sc.textFile("/user/atguigu/ods/baseadlog.log")
      .filter(item => {
        val obj: JSONObject = JSON.parseObject(item)
        obj.isInstanceOf[JSONObject]
      })
      .mapPartitions(partition => {
      partition.map(item => {
        val baseAdLog: BaseAdLog = JSON.parseObject(item, classOf[BaseAdLog])
        baseAdLog
      })
    }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_base_ad")
  }

  /**
    * etl用户注册信息
    * @param sc
    * @param sparkSession
    */
  def etlMemberRegtype(sc: SparkContext, sparkSession: SparkSession): Unit ={
    import sparkSession.implicits._ //隐式转换
    sc.textFile("/user/atguigu/ods/memberRegtype.log")
      .filter(item => {
        val obj: JSONObject = JSON.parseObject(item)
        obj.isInstanceOf[JSONObject]
      })
      .mapPartitions(partition => {
      partition.map(item => {
        val memberRegType: MemberRegType = JSON.parseObject(item, classOf[MemberRegType])
        memberRegType.regsourcename = memberRegType.regsourcename match {
          case "1" => "PC"
          case "2" => "Mobile"
          case "3" => "APP"
          case "4" => "WeChat"
          case _ => "other"
        }
        memberRegType
      })
    }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_member_regtype")
  }

  /**
    * 导入用户付款信息
    * @param sc
    * @param sparkSession
    */
  def etlPcenterMemPayMoney(sc: SparkContext, sparkSession: SparkSession): Unit ={
    import sparkSession.implicits._ //隐式转换
    sc.textFile("/user/atguigu/ods/pcentermempaymoney.log")
      .filter(item => {
        val obj: JSONObject = JSON.parseObject(item)
        obj.isInstanceOf[JSONObject]
      })
      .mapPartitions(partition => {
      partition.map(item => {
        val pcenterMemPayMoney: PcenterMemPayMoney = JSON.parseObject(item, classOf[PcenterMemPayMoney])
        pcenterMemPayMoney
      })
    }).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_pcentermempaymoney")
  }


  /**
    * 导入用户vip基础数据
    * @param sc
    * @param sparkSession
    */
  def etlPcenterMemViplevel(sc: SparkContext, sparkSession: SparkSession): Unit ={
    import sparkSession.implicits._ //隐式转换
    sc.textFile("/user/atguigu/ods/pcenterMemViplevel.log")
      .filter(item => {
        val obj: JSONObject = JSON.parseObject(item)
        obj.isInstanceOf[JSONObject]
      })
      .mapPartitions(partition => {
      partition.map(item => {
        val pcenterMemVipLevel: PcenterMemVipLevel = JSON.parseObject(item, classOf[PcenterMemVipLevel])
        pcenterMemVipLevel
      })
    }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_vip_level")
  }


}
