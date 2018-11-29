package com.spark

import java.util.Properties

import com.common.Constants
import org.apache.spark.sql.SparkSession

object MergeRecordAndReimburseByHospital {

  //recordid hospitalid
  //  // diseaseid departmentid doctorid
  //  // flag starttime endtime allcost isrecovery
  case class Record(recordid: String, hospitalid: String, diseaseid: String,
                    departmentid: String, doctorid: String, flag: Int,
                    starttime: String, endtime: String, allcost: BigDecimal,
                    isrecovery: Int)

  //recordid reimbursetime recost
  case class Reimburse(recordid: String, reimbursetime: String, recost: BigDecimal)



  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      throw new IllegalArgumentException("请传入路径")
    }
    val tmpDir = args(0);

    val spark = SparkSession.builder().master(Constants.MASTER)
      .appName(Constants.APP_NAME).getOrCreate()
    //读取就诊信息
    import spark.implicits._
    val recordDS = spark.sparkContext.textFile(tmpDir + "\\record\\")
      .map(line => {
        val fields = line.split("\t")
        Record(fields(0), fields(1), fields(2), fields(3), fields(4), fields(5).toInt, fields(6), fields(7), BigDecimal(fields(8)), fields(9).toInt)
      }).toDS()

    recordDS.show()


    val reimburseDS = spark.sparkContext.textFile(tmpDir + "\\reimburse\\")
      .map(line => {
        val fields = line.split("\t")
        Reimburse(fields(0), fields(1), BigDecimal(fields(2)))
      }).toDS()


    reimburseDS.show()


    recordDS.createOrReplaceTempView("record")
    reimburseDS.createOrReplaceTempView("reimburse")


    val tmp = spark.sql("select\nl.recordid,l.hospitalid,\nl.diseaseid,l.departmentid,l.doctorid\n,l.flag,l.starttime,l.endtime,l.allcost,l.isrecovery,r.allrecost\nfrom\nrecord l,(select recordid,sum(recost) allrecost from reimburse group by recordid) r\nwhere \nl.recordid=r.recordid")
    //.show()
    tmp.createOrReplaceTempView("tmp")

    tmp.show()

    spark.sql("select \nhospitalid,\ncount(flag=1 or null) hcount,\nsum(case flag when 1 then allcost else 0 end) hcost,\nsum(case flag when 1 then allrecost else 0 end) hreimburse ,\nsum(case flag when 1 then isrecovery else 0 end) hrecovery, \nsum(case flag when 1 then datediff(endtime,starttime)+1 else 0 end) hday,\ncount(flag=2 or null) ocount, \nsum(case flag when 2 then allcost else 0 end) ocost ,\nsum(case flag when 2 then allrecost else 0 end) oreimburse ,\nsum(case flag when 2 then isrecovery else 0 end) orecovery, \ncount(flag=2 or null) oday \nfrom\ntmp\ngroup by hospitalid")
      .show()
    val today=spark.sql("select \nl.hospitalid, l.hcount, l.hcost, l.hreimburse, l.hrecovery,l.hday,\nr.ocount,r.ocost,r.oreimburse,r.orecovery,r.oday\nfrom\n(select \nhospitalid,\ncount(*) hcount,\nsum(allcost) hcost,\nsum(allrecost) hreimburse ,\nsum(isrecovery) hrecovery, \nsum(datediff(endtime,starttime)+1) hday\nfrom\ntmp\nwhere flag=1\ngroup by hospitalid) l,\n(select \nhospitalid,\ncount(*) ocount,\nsum(allcost) ocost,\nsum(allrecost) oreimburse ,\nsum(isrecovery) orecovery, \ncount(*) oday\nfrom\ntmp\nwhere flag=2\ngroup by hospitalid) r\nwhere\nl.hospitalid=r.hospitalid")
     //.show()
    //获取历史数据//spark sparksql
    // 从hbase读取数据
    val prop: Properties = new Properties()
    prop.setProperty("user","root")
    prop.setProperty("password","lr2417")
    //prop.setProperty("driver","org.apache.phoenix.jdbc.PhoenixDriver")

    val history=spark.sqlContext.read.jdbc(Constants.DB_PHOENIX_URL,"GUIDE_HOSPITAL_HISTORY1",prop)
    //开始计算
    //总和数据
//    today.createOrReplaceTempView("today")
    history.createOrReplaceTempView("history")
    spark.sql("select * from history").show()
   // history.show()

    val accData=spark.sql("select \nl.hospitalid,\nl.hcount+r.hcount hcount,\nl.hcost+r.hcost hcost,\nl.hreimburse+r.hreimburse hreimburse,\nl.hrecovery+r.hrecovery hrecovery,\nl.hday+r.hday hday,\nl.ocount+r.ocount ocount,\nl.ocost+r.ocost ocost,\nl.oreimburse+r.oreimburse oreimburse,\nl.orecovery+r.orecovery orecovery,\nl.oday+r.oday oday\nfrom \ntoday l , history r\nwhere \nl.hospitalid=r.hospitalid\nunion\nselect \nl.hospitalid, l.hcount, l.hcost, l.hreimburse, l.hrecovery,l.hday,\nl.ocount,l.ocost,l.oreimburse,l.orecovery,l.oday\nfrom\ntoday l where hospitalid not in (select hospitalid from history)")

    accData.show()

    //求指标数据
    accData.createOrReplaceTempView("acc")

    val percentData=spark.sql("select hospitalid,\nhcount ,hcost/hcount havgcost,hreimburse/hcount havgreimburse ,hreimburse/hcost havgreproportion, hday/hcount havgday ,hrecovery/hcount havgfinproportion ,ocount , ocost/ocount oavgcost ,oreimburse/ocount oavgreimburse   ,oreimburse/ocost oavgreproportion ,orecovery/ocount oavgfinproportion\nfrom acc")

    percentData.show()
    //指标数据保存mysql
    percentData.write
      .mode("overwrite")
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/test")
      .option("dbtable","guide_hospital")
      .option("user","root")
      .option("password","123456")
      .save()


    //总量数据保存hbase
    accData.write
      .format("org.apache.phoenix.spark")
      .mode("overwrite")
      .option("table", "guide_hospital_history")
      .option("zkUrl", "jdbc:phoenix:192.168.203.217:2181")
      .save()
  }


}
