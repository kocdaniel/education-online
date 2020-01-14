package com.atguigu.member.bean

// 网站表基础数据
case class BaseWebSiteLog(
                           siteid: String,
                           sitename: String,
                           siteurl: String,
                           delete: String,
                           createtime: String,
                           creator: String,
                           dn: String
                         )

// 用户表数据
case class DwdMember(
                      uid: Int,
                      ad_id: Int,
                      birthday: String,
                      email: String,
                      var fullname: String,
                      iconurl: String,
                      lastlogin: String,
                      mailaddr: String,
                      memberlevel: String,
                      var password: String,
                      paymoney: String,
                      var phone: String,
                      qq: String,
                      register: String,
                      regupdatetime: String,
                      unitname: String,
                      userip: String,
                      zipcode: String,
                      dt: String,
                      dn: String
                    )

// 广告表基础数据
case class BaseAdLog(
                      adid: String,
                      adname: String,
                      dn: String
                    )

// 用户注册信息
case class MemberRegType(
                          uid: Int,
                          appkey: String,
                          appregurl: String,
                          bdp_uuid: String,
                          createtime: String,
                          isranreg: String,
                          regsource: String,
                          var regsourcename: String,
                          websiteid: Int,
                          dt: String,
                          dn: String
                        )

// 用户付款信息
case class PcenterMemPayMoney(
                               uid: Int,
                               paymoney: String,
                               siteid: Int,
                               vip_id: Int,
                               dt: String,
                               dn: String
                             )

// 用户vip基础数据
case class PcenterMemVipLevel(
                               vip_id: Int,
                               vip_level: String,
                               start_time: String,
                               end_time: String,
                               last_modify_time: String,
                               max_free: String,
                               min_free: String,
                               next_level: String,
                               operator: String,
                               dn: String
                             )

// dws 用户宽表
case class DwsMember(
                      uid: Int,
                      ad_id: Int,
                      fullname: String,
                      iconurl: String,
                      lastlogin: String,
                      mailaddr: String,
                      memberlevel: String,
                      password: String,
                      paymoney: BigDecimal,
                      phone: String,
                      qq: String,
                      register: String,
                      regupdatetime: String,
                      unitname: String,
                      userip: String,
                      zipcode: String,
                      appkey: String,
                      appregurl: String,
                      bdp_uuid: String,
                      reg_createtime: String,
                      isranreg: String,
                      regsource: String,
                      regsourcename: String,
                      adname: String,
                      siteid: String,
                      sitename: String,
                      siteurl: String,
                      site_delete: String,
                      site_createtime: String,
                      site_creator: String,
                      vip_id: String,
                      vip_level: String,
                      vip_start_time: String,
                      vip_end_time: String,
                      vip_last_modify_time: String,
                      vip_max_free: String,
                      vip_min_free: String,
                      vip_next_level: String,
                      vip_operator: String,
                      dt: String,
                      dn: String
                    )

// dws 经过聚合清洗之后的用户宽表
case class DwsMember_Result(
                             uid: Int,
                             ad_id: Int,
                             fullname: String,
                             icounurl: String,
                             lastlogin: String,
                             mailaddr: String,
                             memberlevel: String,
                             password: String,
                             paymoney: String,
                             phone: String,
                             qq: String,
                             register: String,
                             regupdatetime: String,
                             unitname: String,
                             userip: String,
                             zipcode: String,
                             appkey: String,
                             appregurl: String,
                             bdp_uuid: String,
                             reg_createtime: String,
                             isranreg: String,
                             regsource: String,
                             regsourcename: String,
                             adname: String,
                             siteid: String,
                             sitename: String,
                             siteurl: String,
                             site_delete: String,
                             site_createtime: String,
                             site_creator: String,
                             vip_id: String,
                             vip_level: String,
                             vip_start_time: String,
                             vip_end_time: String,
                             vip_last_modify_time: String,
                             vip_max_free: String,
                             vip_min_free: String,
                             vip_next_level: String,
                             vip_operator: String,
                             dt: String,
                             dn: String
                           )

// 用户拉链表
case class MemberZipper(
                         uid: Int,
                         var paymoney: String,
                         vip_level: String,
                         start_time: String,
                         var end_time: String,
                         dn: String
                       )


case class MemberZipperResult(list: List[MemberZipper])


case class QueryResult(
                        uid: Int,
                        ad_id: Int,
                        memberlevel: String,
                        register: String,
                        appregurl: String, //注册来源url
                        regsource: String,
                        regsourcename: String,
                        adname: String,
                        siteid: String,
                        sitename: String,
                        vip_level: String,
                        paymoney: BigDecimal,
                        dt: String,
                        dn: String
                      )