package com.atguigu.qz.bean

case class DwdQzPoint(pointid: Int, courseid: Int, pointname: String, pointyear: String, chapter: String,
                      creator: String, createtime: String, status: String, modifystatus: String, excisenum: Int,
                      pointlistid: Int, chapterid: Int, sequence: String, pointdescribe: String, pointlevel: String,
                      typelist: String, var score: BigDecimal, thought: String, remid: String, pointnamelist: String,
                      typelistids: String, pointlist: String, dt: String, dn: String)

case class DwdQzPaperView(paperviewid: Int, paperid: Int, paperviewname: String, paperparam: String, openstatus: String,
                          explainurl: String, iscontest: String, contesttime: String, conteststarttime: String, contestendtime: String,
                          contesttimelimit: String, dayiid: Int, status: String, creator: String, createtime: String,
                          paperviewcatid: Int, modifystatus: String, description: String, papertype: String, downurl: String,
                          paperuse: String, paperdifficult: String, testreport: String, paperuseshow: String, dt: String, dn: String)

case class DwdQzQuestion(questionid: Int, parentid: Int, questypeid: Int, quesviewtype: Int, content: String, answer: String,
                         analysis: String, limitminute: String, scoe: BigDecimal, splitcore: BigDecimal, status: String,
                         optnum: Int, lecture: String, creator: String, createtime: String, modifystatus: String,
                         attanswer: String, questag: String, vanalysisaddr: String, difficulty: String, quesskill: String,
                         vdeoaddr: String, dt: String, dn: String)


case class DwdQzChapter(
                      chapterid: Int,
                      chapterlistid: Int,
                      chaptername: String,
                      sequence: String,
                      showstatus: String,
                      creator: String,
                      createtime: String,
                      courseid: Int,
                      chapternum: Int,
                      outchapterid: Int,
                      dt: String,
                      dn: String
                    )


case class DwdQzChapterList(
                          chapterlistid: Int,
                          chapterlistname: String,
                          courseid: Int,
                          sequence: String,
                          chapterallnum: Int,
                          status: String,
                          creator: String,
                          createtime: String,
                          dt: String,
                          dn: String
                        )

