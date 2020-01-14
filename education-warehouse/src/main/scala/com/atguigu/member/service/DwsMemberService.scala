package com.atguigu.member.service

import com.atguigu.member.bean.{DwsMember, DwsMember_Result, MemberZipper, MemberZipperResult}
import com.atguigu.member.dao.DwdMemberDao
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel

object DwsMemberService {

  /**
    * useApi
    *
    * @param sparkSession
    * @param dt
    */
  def importMemberUseApi(sparkSession: SparkSession, dt: String) = {
    import sparkSession.implicits._ // 隐式转换
    val dwdMember: Dataset[Row] = DwdMemberDao.getDwdMember(sparkSession).where(s"'${dt}'")
    // 主用户表
    val dwdBaseAd: DataFrame = DwdMemberDao.getDwdBaseAd(sparkSession)
    val dwdBaseWebSite: DataFrame = DwdMemberDao.getDwdBaseWebSite(sparkSession)
    val dwdMemberRegType: DataFrame = DwdMemberDao.getDwdMemberRegType(sparkSession)
    val dwdPcentermemPayMoney: DataFrame = DwdMemberDao.getDwdPcentermemPayMoney(sparkSession)
    val dwdVipLevel: DataFrame = DwdMemberDao.getDwdVipLevel(sparkSession)

    //dwdMember.join(dwdMemberRegType, dwdMember("uid") === dwdMemberRegType("uid") && dwdMember("dn") === dwdMemberRegType("dn"))
    // 用dwdMember join 其它6张表， dwdMember为主表：当天的注册用户
    // dwdMember中可能每个用户点击多次，有多条数据，需要按照uid和dn对用户进行聚合和清洗
    // 需要累加的数据如paymoney则累加，重复的用户信息取第一条
    val dwsMember: Dataset[DwsMember] = dwdMember
      .join(dwdMemberRegType, Seq("uid", "dn"), "left")
      .join(dwdBaseAd, Seq("ad_id", "dn"), "left")
      .join(dwdBaseWebSite, Seq("siteid", "dn"), "left")
      .join(dwdPcentermemPayMoney, Seq("uid", "dn"), "left")
      .join(dwdVipLevel, Seq("vip_id", "dn"), "left")
      .select("uid", "ad_id", "fullname", "iconurl", "lastlogin", "mailaddr", "memberlevel", "password"
        , "paymoney", "phone", "qq", "register", "regupdatetime", "unitname", "userip", "zipcode", "appkey"
        , "appregurl", "bdp_uuid", "reg_createtime", "isranreg", "regsource", "regsourcename", "adname"
        , "siteid", "sitename", "siteurl", "site_delete", "site_createtime", "site_creator", "vip_id", "vip_level",
        "vip_start_time", "vip_end_time", "vip_last_modify_time", "vip_max_free", "vip_min_free", "vip_next_level"
        , "vip_operator", "dt", "dn")
      .as[DwsMember]
    dwsMember.persist(StorageLevel.MEMORY_AND_DISK_SER)

    // 相同用户进行聚合清洗
    val resultData = dwsMember.groupByKey(item => item.uid + "_" + item.dn)
      // mapGroups 会将相同key对应的value组成一个迭代器
      // iters[DwsMember]
      .mapGroups { case (key, iters) =>
        val keys: Array[String] = key.split("_")
        val uid: Int = Integer.parseInt(keys(0))
        val dn = keys(1)
        // 迭代器特性：单向，只能遍历一次，不能重复使用；若需要重复使用需要转化为list集合
        val dwsMemberList = iters.toList
        val paymoney: String = dwsMemberList.filter(_.paymoney != null).map(_.paymoney).reduceLeftOption(_ + _).getOrElse(BigDecimal.apply(0.00)).toString()
        val ad_id: Int = dwsMemberList.map(_.ad_id).head
        val fullname = dwsMemberList.map(_.fullname).head
        val icounurl = dwsMemberList.map(_.iconurl).head
        val lastlogin = dwsMemberList.map(_.lastlogin).head
        val mailaddr = dwsMemberList.map(_.mailaddr).head
        val memberlevel = dwsMemberList.map(_.memberlevel).head
        val password = dwsMemberList.map(_.password).head
        val phone = dwsMemberList.map(_.phone).head
        val qq = dwsMemberList.map(_.qq).head
        val register = dwsMemberList.map(_.register).head
        val regupdatetime = dwsMemberList.map(_.regupdatetime).head
        val unitname = dwsMemberList.map(_.unitname).head
        val userip = dwsMemberList.map(_.userip).head
        val zipcode = dwsMemberList.map(_.zipcode).head
        val appkey = dwsMemberList.map(_.appkey).head
        val appregurl = dwsMemberList.map(_.appregurl).head
        val bdp_uuid = dwsMemberList.map(_.bdp_uuid).head
        val reg_createtime = dwsMemberList.map(_.reg_createtime).head
        val isranreg = dwsMemberList.map(_.isranreg).head
        val regsource = dwsMemberList.map(_.regsource).head
        val regsourcename = dwsMemberList.map(_.regsourcename).head
        val adname = dwsMemberList.map(_.adname).head
        val siteid = dwsMemberList.map(_.siteid).head
        val sitename = dwsMemberList.map(_.sitename).head
        val siteurl = dwsMemberList.map(_.siteurl).head
        val site_delete = dwsMemberList.map(_.site_delete).head
        val site_createtime = dwsMemberList.map(_.site_createtime).head
        val site_creator = dwsMemberList.map(_.site_creator).head
        val vip_id = dwsMemberList.map(_.vip_id).head
        val vip_level = dwsMemberList.map(_.vip_level).max
        val vip_start_time = dwsMemberList.map(_.vip_start_time).min
        val vip_end_time = dwsMemberList.map(_.vip_end_time).max
        val vip_last_modify_time = dwsMemberList.map(_.vip_last_modify_time).max
        val vip_max_free = dwsMemberList.map(_.vip_max_free).head
        val vip_min_free = dwsMemberList.map(_.vip_min_free).head
        val vip_next_level = dwsMemberList.map(_.vip_next_level).head
        val vip_operator = dwsMemberList.map(_.vip_operator).head

      DwsMember_Result(uid, ad_id, fullname, icounurl, lastlogin, mailaddr, memberlevel, password, paymoney,
        phone, qq, register, regupdatetime, unitname, userip, zipcode, appkey, appregurl,
        bdp_uuid, reg_createtime, isranreg, regsource, regsourcename, adname, siteid,
        sitename, siteurl, site_delete, site_createtime, site_creator, vip_id, vip_level,
        vip_start_time, vip_end_time, vip_last_modify_time, vip_max_free, vip_min_free,
        vip_next_level, vip_operator, dt, dn)
    }

    resultData.show()
    //    result.foreach(println)
    while (true) {
      println("1")
    }

  }


  /**
    * 使用sql插入
    *
    * @param sparkSession
    * @param time
    */
  def importMember(sparkSession: SparkSession, time: String): Unit = {

    import sparkSession.implicits._

    // 查询全量数据 刷新到宽表
    sparkSession.sql(
      s"""
         |select
         |      uid,
         |      first(ad_id),
         |      first(fullname),
         |      first(iconurl),
         |      first(lastlogin),
         |      first(mailaddr),
         |      first(memberlevel),
         |      first(password),
         |      sum(cast(paymoney as decimal(10,4))),
         |      first(phone),
         |      first(qq),
         |      first(register),
         |      first(regupdatetime),
         |      first(unitname),
         |      first(userip),
         |      first(zipcode),
         |      first(appkey),
         |      first(appregurl),
         |      first(bdp_uuid),
         |      first(reg_createtime),
         |      first(isranreg),
         |      first(regsource),
         |      first(regsourcename),
         |      first(adname),
         |      first(siteid),
         |      first(sitename),
         |      first(siteurl),
         |      first(site_delete),
         |      first(site_createtime),
         |      first(site_creator),
         |      first(vip_id),
         |      max(vip_level),
         |      min(vip_start_time),
         |      max(vip_end_time),
         |      max(vip_last_modify_time),
         |      first(vip_max_free),
         |      first(vip_min_free),
         |      max(vip_next_level),
         |      first(vip_operator),
         |      dt,
         |      dn
         |from
         |(
         |      select
         |            a.uid,
         |            a.ad_id,
         |            a.fullname,
         |            a.iconurl,
         |            a.lastlogin,
         |            a.mailaddr,
         |            a.memberlevel,
         |            a.password,
         |            e.paymoney,
         |            a.phone,
         |            a.qq,
         |            a.register,
         |            a.regupdatetime,
         |            a.unitname,
         |            a.userip,
         |            a.zipcode,
         |            a.dt,
         |            b.appkey,
         |            b.appregurl,
         |            b.bdp_uuid,
         |            b.createtime as reg_createtime,
         |            b.isranreg,
         |            b.regsource,
         |            b.regsourcename,
         |            c.adname,
         |            d.siteid,
         |            d.sitename,
         |            d.siteurl,
         |            d.delete as site_delete,
         |            d.createtime as site_createtime,
         |            d.creator as site_creator,
         |            f.vip_id,
         |            f.vip_level,
         |            f.start_time as vip_start_time,
         |            f.end_time as vip_end_time,
         |            f.last_modify_time as vip_last_modify_time,
         |            f.max_free as vip_max_free,
         |            f.min_free as vip_min_free,
         |            f.next_level as vip_next_level,
         |            f.operator as vip_operator,
         |            a.dn
         |      from dwd.dwd_member a
         |      left join dwd.dwd_member_regtype b
         |      on a.uid=b.uid and a.dn=b.dn
         |      left join dwd.dwd_base_ad c
         |      on a.ad_id=c.adid and a.dn=c.dn
         |      left join dwd.dwd_base_website d
         |      on b.websiteid=d.siteid and b.dn=d.dn
         |      left join dwd.dwd_pcentermempaymoney e
         |      on a.uid=e.uid and a.dn=e.dn
         |      left join dwd.dwd_vip_level f
         |      on e.vip_id=f.vip_id and e.dn=f.dn
         |      where a.dt='${time}'
         |)
         |group by uid,dn,dt
      """.stripMargin).coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dws.dws_member")


    // 查询当天增量数据
    val dayResult = sparkSession.sql(
      s"""
         |select
         |      a.uid,
         |      sum(cast(a.paymoney as decimal(10,4))) as paymoney,
         |      max(b.vip_level) as vip_level,
         |      from_unixtime(unix_timestamp('$time','yyyyMMdd'),'yyyy-MM-dd') as start_time,
         |      '9999-12-31' as end_time,
         |      first(a.dn) as dn
         |from dwd.dwd_pcentermempaymoney a
         |join dwd.dwd_vip_level b
         |on a.vip_id=b.vip_id and a.dn=b.dn
         |where a.dt='$time'
         |group by uid
      """.stripMargin).as[MemberZipper]

    //查询历史拉链表数据
    val historyResult = sparkSession.sql(
      """
        |select * from dws.dws_member_zipper
      """.stripMargin).as[MemberZipper]

    //两份数据根据用户id进行聚合 对end_time进行重新修改
    dayResult.union(historyResult)
      // 自定义分组的key
      .groupByKey(item => item.uid + "_" + item.dn)
      // mapGroups()将相同key的数据聚合到一起，形成一个迭代器
      // 迭代器中是相同key的MemberZipper对象
      .mapGroups { case (key, iters) =>
      val keys: Array[String] = key.split("_")
      val uid = keys(0)
      val dn = keys(1)
      // 按照开始时间进行排序
      val list: List[MemberZipper] = iters.toList.sortBy(item => item.start_time)

      // 如果排序之后的倒数第二条数据的end_time="9999-12-31"，说明此用户今天有信息更改
      if (list.size > 1 && "9999-12-31".equals(list(list.size - 2).end_time)) {
        // 获取历史数据的最后一条数据
        val oldLastModel: MemberZipper = list(list.size - 2)

        // 获取当前最新表的最后一条数据
        val lastModel: MemberZipper = list(list.size - 1)
        oldLastModel.end_time = lastModel.start_time
        lastModel.paymoney = (BigDecimal.apply(lastModel.paymoney) + BigDecimal.apply(oldLastModel.paymoney)).toString()
      }
      MemberZipperResult(list)
    }.flatMap(_.list).coalesce(3).write.mode(SaveMode.Overwrite).insertInto("dws.dws_member_zipper") //重组对象打散 刷新拉链表

  }
}
