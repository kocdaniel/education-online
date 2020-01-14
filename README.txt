需要注意的细节：
1. caselass.toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto/saveAsTable("表名")
    - 使用insertInto DF的字段顺序需要和表的字段顺序一致，否则会错位
    - 使用saveAsTable 是按照字段名称插入的

2. val baseWebSiteLog: BaseWebSiteLog = JSON.parseObject(item, classOf[BaseWebSiteLog])
    - JSON解析json字符串是按照key解析的，key对应样例类的字段名称

3. 迭代器特性：单向，只能遍历一次，不能重复使用；若需要重复使用需要转化为list集合

4. join: 两种方式的区别
    - dwdQzChapter.join(dwdQzChapterlist, dwdQzChapter("chapterlistid") === dwdQzChapterlist("chapterlistid") && dwdQzChapter("dn") === dwdQzChapterlist("dn"))
        - 相同字段不会去重
    - dwdQzChapter.join(dwdQzChapterlist, Seq("chapterlistid", "dn"))
        - 相同字段会去重
        - 两个表join的字段名必须相同